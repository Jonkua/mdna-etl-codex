import pytest
from pathlib import Path
from src.utils.cik_filter import load_cik_list, load_ciks_from_directory


def test_load_cik_list(tmp_path):
    csv_file = tmp_path / "ciks.csv"
    csv_file.write_text("cik,ticker\n123456,AAA\n789,BBB\n")
    ciks = load_cik_list(csv_file)
    assert ciks == {"0000123456", "0000000789"}


def test_load_ciks_from_directory(tmp_path):
    d = tmp_path / "dir"
    d.mkdir()
    (d / "a.csv").write_text("123456\n")
    (d / "b.csv").write_text("987654,XYZ\n")
    ciks = load_ciks_from_directory(d)
    assert ciks == {"0000123456", "0000987654"}