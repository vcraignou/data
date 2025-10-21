#!/usr/bin/env python3
"""
Daily price updater:
- Downloads adjusted close for configured assets since START
- Writes docs/prices.json and docs/prices.csv
- Designed for GitHub Actions daily run + GitHub Pages hosting

Requires: yfinance, pandas
"""
import json
import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import yfinance as yf

# ---------------- Config ----------------
START = "2015-01-01"

# Add/remove assets freely. Keys = short names you’ll use on the website.
ASSETS = {
    "BTC":  "BTC-USD",   # Bitcoin (USD)
    "Gold": "GC=F",      # Gold futures (continuous)
    # Optional extras:
    # "ETH":  "ETH-USD",
    # "SPX":  "^GSPC",
    # "NASDAQ": "^IXIC",
    # "DXY": "DX-Y.NYB",
    # "UST10Y": "^TNX",
}

OUTPUT_DIR = Path("docs")
JSON_PATH  = OUTPUT_DIR / "prices.json"
CSV_PATH   = OUTPUT_DIR / "prices.csv"

# Retry settings (for transient rate limits)
MAX_RETRIES = 3
SLEEP_SEC   = 3

# ---------------- Helpers ----------------
def fetch_close_series(ticker: str, name: str, start: str) -> pd.Series | None:
    """Download one ticker -> return adjusted Close series named `name`."""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            df = yf.download(ticker, start=start, auto_adjust=True, progress=False)
            if df is None or "Close" not in df or df["Close"].dropna().empty:
                print(f"[WARN] No usable Close for {name} ({ticker})")
                return None
            s = df["Close"]
            if isinstance(s, pd.DataFrame) and s.shape[1] == 1:
                s = s.squeeze("columns")
            s = s.dropna()
            s.name = name
            return s
        except Exception as e:
            print(f"[WARN] {name} ({ticker}) attempt {attempt}/{MAX_RETRIES} failed: {e}")
            time.sleep(SLEEP_SEC * attempt)
    print(f"[ERROR] Failed to fetch {name} after {MAX_RETRIES} retries.")
    return None


def build_dataframe() -> pd.DataFrame:
    series = []
    for name, ticker in ASSETS.items():
        s = fetch_close_series(ticker, name, START)
        if s is not None:
            series.append(s)
    if not series:
        raise RuntimeError("No price data available. (Network/rate limit?)")
    df = pd.concat(series, axis=1).sort_index().dropna(how="all")
    return df


def make_json_payload(df: pd.DataFrame) -> dict:
    # Emit rows as native Python scalars, one per date
    rows = []
    for dt, row in df.iterrows():
        item = {"date": dt.strftime("%Y-%m-%d")}
        for col in df.columns:
            val = row[col]
            # Ensure scalar float (no single-element Series)
            item[col] = float(val)
        rows.append(item)
    return {
        "asOf": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "start": df.index[0].strftime("%Y-%m-%d"),
        "assets": list(df.columns),
        "data": rows,
    }


def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    df = build_dataframe()

    # Write CSV (wide format)
    df.to_csv(CSV_PATH, index_label="date", float_format="%.8f")

    # Write JSON (records)
    payload = make_json_payload(df)
    with open(JSON_PATH, "w", encoding="utf-8") as f:
        json.dump(payload, f, separators=(",", ":"), ensure_ascii=False)

    print(f"[OK] Wrote {JSON_PATH} and {CSV_PATH}")
    print(f"[OK] Assets: {', '.join(df.columns)}")
    print(f"[OK] Dates: {df.index[0].date()} → {df.index[-1].date()}")


if __name__ == "__main__":
    main()
