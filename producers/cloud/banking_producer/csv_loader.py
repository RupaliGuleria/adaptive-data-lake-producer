from __future__ import annotations

import logging
from typing import Any, Dict, Iterator

import pandas as pd

logger = logging.getLogger(__name__)

# Canonical internal name -> possible CSV column names (case-insensitive)
_COLUMN_ALIASES: Dict[str, list[str]] = {
    "transaction_id": ["transaction_id", "transactionid", "txn_id", "id"],
    "account_number": ["account_number", "accountnumber", "account_no", "acc_no"],
    "transaction_date": ["transaction_date", "date", "txn_date", "trans_date"],
    "transaction_type": ["transaction_type", "type", "txn_type", "trans_type"],
    "amount": ["amount", "transaction_amount", "txn_amount"],
    "balance": ["balance", "balance_after_transaction", "closing_balance"],
    "description": ["description", "narration", "remarks", "memo"],
}


def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Map common banking CSV column variants onto canonical payload names."""
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
    rename_map: Dict[str, str] = {}
    current_cols = set(df.columns)
    for canonical, aliases in _COLUMN_ALIASES.items():
        if canonical not in current_cols:
            for alias in aliases:
                if alias in current_cols:
                    rename_map[alias] = canonical
                    break
    if rename_map:
        df = df.rename(columns=rename_map)
    return df


def load_transactions(file_path: str) -> Iterator[Dict[str, Any]]:
    """Load banking transactions from CSV as JSON-ready dictionaries."""
    logger.info(f"Reading CSV: {file_path}")
    df = pd.read_csv(file_path, low_memory=False)
    df = _normalize_columns(df)
    # Replace NaN with None so JSON serialization is clean
    df = df.where(pd.notna(df), other=None)
    logger.info(f"Loaded {len(df):,} rows, columns: {list(df.columns)}")
    for row in df.to_dict(orient="records"):
        yield row
