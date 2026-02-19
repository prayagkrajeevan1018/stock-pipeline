"""
ingestion/fetch_stocks.py
─────────────────────────
Fetches daily OHLCV (Open, High, Low, Close, Volume) data for a list of
stock tickers using yfinance and saves each ticker as a Parquet file.
"""

import os
import logging
from datetime import datetime, timedelta

import yfinance as yf
import pandas as pd

# ─── Config ───────────────────────────────────────────────────────────────────

TICKERS = ["AAPL", "MSFT", "GOOGL", "AMZN", "META", "TSLA", "NVDA", "JPM"]

OUTPUT_DIR = os.environ.get("DATA_DIR", "./data/raw")

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


# ─── Helpers ──────────────────────────────────────────────────────────────────

def get_date_range(lookback_days: int = 1) -> tuple:
    end = datetime.today()
    start = end - timedelta(days=lookback_days)
    return start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")


def fetch_ticker(ticker: str, start: str, end: str) -> pd.DataFrame:
    logger.info(f"Fetching {ticker} from {start} to {end}")
    df = yf.download(ticker, start=start, end=end, progress=False, auto_adjust=True)

    if df.empty:
        logger.warning(f"No data returned for {ticker}")
        return pd.DataFrame()

    df = df.reset_index()

    # Fix for newer yfinance versions — columns come back as tuples like ('Close', 'AAPL')
    # We flatten them to just the first element e.g. 'Close' → 'close'
    df.columns = [
        col[0].lower() if isinstance(col, tuple) else col.lower()
        for col in df.columns
    ]

    df["ticker"] = ticker
    df["ingested_at"] = datetime.utcnow().isoformat()
    df = df.rename(columns={"date": "trade_date"})

    return df[["ticker", "trade_date", "open", "high", "low", "close", "volume", "ingested_at"]]


def save_parquet(df: pd.DataFrame, ticker: str) -> str:
    today = datetime.today().strftime("%Y-%m-%d")
    out_path = os.path.join(OUTPUT_DIR, f"{ticker}_{today}.parquet")
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    df.to_parquet(out_path, index=False)
    logger.info(f"Saved {len(df)} rows → {out_path}")
    return out_path


# ─── Main ─────────────────────────────────────────────────────────────────────

def run_ingestion(lookback_days: int = 7) -> list:
    start, end = get_date_range(lookback_days)
    saved_files = []

    for ticker in TICKERS:
        try:
            df = fetch_ticker(ticker, start, end)
            if not df.empty:
                path = save_parquet(df, ticker)
                saved_files.append(path)
        except Exception as e:
            logger.error(f"Failed to fetch {ticker}: {e}")

    logger.info(f"Ingestion complete. {len(saved_files)}/{len(TICKERS)} tickers saved.")
    return saved_files


if __name__ == "__main__":
    run_ingestion(lookback_days=365)