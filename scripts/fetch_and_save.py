# -*- coding: utf-8 -*-
"""
新北市鶯歌區 YouBike2.0 定時抓取腳本
設計給 GitHub Actions 排程使用，每次執行:
  1. 抓取新北市全市即時資料 (自動翻頁)
  2. 篩選鶯歌區
  3. 安全 append 到「當天」的 CSV (data/raw/YYYY-MM-DD.csv)

安全性設計:
  - 用 CSV 逐行 append，不用單一 parquet 累加寫入(避免中斷寫壞檔案)
  - 網路失敗會重試 3 次(指數退避)，仍失敗則印出錯誤但不讓整個 process crash 掉，
    讓 GitHub Actions 這次執行標記失敗即可，不影響下一次排程正常執行
  - 用台北時區(UTC+8)決定檔名日期，避免 GitHub Actions 預設 UTC 造成日期跳號
"""

import sys
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path

import pandas as pd
import requests

API_BASE = "https://data.ntpc.gov.tw/api/datasets/010e5b15-3823-4b20-b401-b1cf000550c5/csv"
TARGET_AREA = "鶯歌區"
DATA_DIR = Path(__file__).resolve().parent.parent / "data" / "raw"
TAIPEI_TZ = timezone(timedelta(hours=8))

MAX_RETRIES = 3
RETRY_BACKOFF_SEC = 5  # 第一次失敗等5秒重試，第二次等10秒，第三次等20秒


def fetch_all_stations(page_size: int = 1000) -> pd.DataFrame:
    """翻頁抓取新北市全市YouBike即時資料，內建重試機制。"""
    all_frames = []
    page = 0
    while True:
        url = f"{API_BASE}?page={page}&size={page_size}"

        df_page = None
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                resp = requests.get(url, timeout=30)
                resp.raise_for_status()
                df_page = pd.read_csv(pd.io.common.StringIO(resp.text))
                break
            except Exception as e:
                wait = RETRY_BACKOFF_SEC * (2 ** (attempt - 1))
                print(f"[warn] page={page} 第{attempt}次嘗試失敗: {e}，{wait}秒後重試")
                if attempt == MAX_RETRIES:
                    raise
                time.sleep(wait)

        if df_page is None or df_page.empty:
            break

        all_frames.append(df_page)
        if len(df_page) < page_size:
            break

        page += 1
        time.sleep(0.3)

    if not all_frames:
        raise RuntimeError("沒有抓到任何資料")

    return pd.concat(all_frames, ignore_index=True).drop_duplicates(subset="sno")


def append_to_daily_csv(df: pd.DataFrame, fetch_time: datetime) -> Path:
    """把這次抓到的資料安全append到當天(台北時區)的CSV。"""
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    date_str = fetch_time.strftime("%Y-%m-%d")
    csv_path = DATA_DIR / f"{date_str}.csv"

    df = df.copy()
    df["fetch_time"] = fetch_time.strftime("%Y-%m-%dT%H:%M:%S+08:00")

    file_exists = csv_path.exists()
    # mode="a" + header 只在檔案不存在時寫，這是CSV安全append的標準做法
    df.to_csv(csv_path, mode="a", header=not file_exists, index=False, encoding="utf-8-sig")

    return csv_path


def main() -> int:
    fetch_time = datetime.now(TAIPEI_TZ)
    print(f"開始抓取 - {fetch_time.isoformat()}")

    try:
        df_all = fetch_all_stations()
    except Exception as e:
        print(f"[error] 抓取全市資料失敗: {e}")
        return 1  # 讓這次 Actions 執行標記失敗，但不影響下一次排程

    df_yingge = df_all[df_all["sarea"] == TARGET_AREA].copy()
    if df_yingge.empty:
        print(f"[error] 篩選不到 {TARGET_AREA} 的資料")
        return 1

    csv_path = append_to_daily_csv(df_yingge, fetch_time)
    print(f"已寫入 {len(df_yingge)} 筆到 {csv_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
