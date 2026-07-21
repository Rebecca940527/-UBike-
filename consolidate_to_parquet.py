# -*- coding: utf-8 -*-
"""
把 GitHub repo 累積的每日CSV (data/raw/*.csv) 彙整成單一 parquet，
供後續AFC比對、色階地圖、時序分析使用。

使用方式:
  1. 先 git pull 把GitHub Actions累積的最新資料拉回本機
     (repo資料夾建議放在跟妳其他研究資料同一層，方便管理)
  2. 執行本腳本，輸出一份帶時間序列的parquet到妳熟悉的研究資料夾
"""

import glob
import pandas as pd
from pathlib import Path

# GitHub repo 在本機的路徑 (git clone下來的資料夾)
REPO_DIR = Path(r"C:\Users\Rebec\OneDrive\Desktop\新北捷運公司研究\youbike-yingge-history")

# 彙整後輸出的位置，跟妳其他研究資料放一起
OUTPUT_PATH = Path(r"C:\Users\Rebec\OneDrive\Desktop\新北捷運公司研究\鶯歌區UBIKE_時間序列.parquet")


def main():
    csv_files = sorted(glob.glob(str(REPO_DIR / "data" / "raw" / "*.csv")))
    if not csv_files:
        print(f"在 {REPO_DIR / 'data' / 'raw'} 找不到任何CSV，請先確認已git pull最新資料")
        return

    print(f"找到 {len(csv_files)} 個每日CSV檔")

    dfs = []
    for f in csv_files:
        try:
            df = pd.read_csv(f, encoding="utf-8-sig")
            dfs.append(df)
        except Exception as e:
            print(f"[warn] 讀取失敗，略過: {f} ({e})")

    df_all = pd.concat(dfs, ignore_index=True)

    # 型別整理
    df_all["fetch_time"] = pd.to_datetime(df_all["fetch_time"], errors="coerce")
    for col in ["tot_quantity", "sbi_quantity", "bemp", "yb2_quantity", "eyb_quantity"]:
        if col in df_all.columns:
            df_all[col] = pd.to_numeric(df_all[col], errors="coerce")

    # 去除完全重複的列(例如同一次抓取因為重跑workflow意外重複寫入)
    dedup_cols = ["sno", "fetch_time"]
    before = len(df_all)
    df_all = df_all.drop_duplicates(subset=dedup_cols)
    print(f"去重複: {before} -> {len(df_all)} 筆")

    df_all = df_all.sort_values(["sno", "fetch_time"]).reset_index(drop=True)

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    df_all.to_parquet(OUTPUT_PATH, index=False, engine="pyarrow")

    print(f"已輸出: {OUTPUT_PATH}")
    print(f"資料範圍: {df_all['fetch_time'].min()} ~ {df_all['fetch_time'].max()}")
    print(f"站點數: {df_all['sno'].nunique()}，總筆數: {len(df_all)}")


if __name__ == "__main__":
    main()
