import csv
from pathlib import Path
from typing import Set


def load_cik_list(path: Path) -> Set[str]:
    """Load a set of CIKs from a single CSV file.

    The CSV is expected to have the CIK in the first column.
    Values are zero-padded to 10 digits.
    """
    ciks: Set[str] = set()
    if not path.exists():
        return ciks
    with open(path, newline='', encoding='utf-8-sig') as f:
        reader = csv.reader(f)
        for row in reader:
            if not row:
                continue
            cik = row[0].strip()
            if cik:
                cik_digits = ''.join(filter(str.isdigit, cik))
                if cik_digits:
                    ciks.add(cik_digits.zfill(10))
    return ciks


def load_ciks_from_directory(directory: Path) -> Set[str]:
    """Load CIKs from all CSV files within a directory."""
    all_ciks: Set[str] = set()
    if not directory.exists():
        return all_ciks
    for csv_path in directory.glob('*.csv'):
        all_ciks.update(load_cik_list(csv_path))
    return all_ciks