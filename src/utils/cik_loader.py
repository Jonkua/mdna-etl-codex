from pathlib import Path
import csv
from typing import Set
from src.utils.logger import get_logger

logger = get_logger(__name__)


def load_cik_list(csv_path: Path) -> Set[str]:
    """Load a set of CIKs from a CSV file. The CSV is expected to contain
    CIKs and optionally tickers in the first two columns. Only the CIK column
    is used."""
    ciks: Set[str] = set()
    if not csv_path.exists():
        logger.warning(f"CIK CSV not found: {csv_path}")
        return ciks
    try:
        with open(csv_path, newline='', encoding='utf-8') as f:
            reader = csv.reader(f)
            for row in reader:
                if not row:
                    continue
                cik = row[0].strip()
                if cik:
                    # zero-pad to 10 digits
                    ciks.add(cik.zfill(10))
    except Exception as e:
        logger.error(f"Failed to load CIK CSV {csv_path}: {e}")
    return ciks