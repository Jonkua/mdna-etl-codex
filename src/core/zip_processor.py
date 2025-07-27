"""ZIP archive processor for handling compressed SEC filings with 10-Q fallback logic."""

import zipfile
import tempfile
from pathlib import Path
from typing import List, Dict, Optional, Set

from src.core.extractor import MDNAExtractor
from src.core.file_handler import FileHandler
from src.core.filing_manager import FilingManager
from src.utils.logger import get_logger, log_error
from config.settings import VALID_EXTENSIONS, ZIP_EXTENSIONS

logger = get_logger(__name__)


class ZipProcessor:
    """Handles processing of ZIP archives containing SEC filings."""

    def __init__(self, output_dir: Path):
        self.output_dir = Path(output_dir)
        self.extractor = MDNAExtractor(output_dir)
        self.file_handler = FileHandler()

    def _extract_cik_from_name(self, filename: str) -> Optional[str]:
        """Extract CIK from a filing filename."""
        import re
        m = re.search(r"edgar_data_(\d{1,10})", filename)
        if m:
            return m.group(1).zfill(10)
        m = re.search(r"(\d{4,10})", filename)
        return m.group(1).zfill(10) if m else None

    def _is_10k(self, filename: str) -> bool:
        return "10-K" in filename.upper() or "10K" in filename.upper()

    def process_zip_file(
        self,
        zip_path: Path,
        cik_filter: Optional[Set[str]] = None
    ) -> Dict[str, any]:
        """
        Process a single ZIP file.

        Args:
            zip_path: Path to ZIP file
            cik_filter: Optional set of CIK strings to filter files

        Returns:
            Processing statistics
        """
        logger.info(f"Processing ZIP file: {zip_path}")

        stats = {
            "zip_file": str(zip_path),
            "total_files": 0,
            "processed": 0,
            "failed": 0,
            "errors": []
        }

        try:
            with zipfile.ZipFile(zip_path, 'r') as zf:
                file_list = zf.namelist()
                text_files = [f for f in file_list if any(f.endswith(ext) for ext in VALID_EXTENSIONS)]

                # Apply CIK filter and 10-K form filter if provided
                if cik_filter is not None:
                    filtered = []
                    for fname in text_files:
                        cik = self._extract_cik_from_name(fname)
                        if cik and cik in cik_filter and self._is_10k(fname):
                            filtered.append(fname)
                    text_files = filtered

                stats["total_files"] = len(text_files)
                logger.info(f"Found {len(text_files)} text files in archive")

                with tempfile.TemporaryDirectory() as temp_dir:
                    temp_path = Path(temp_dir)
                    fm = FilingManager()
                    for file_name in text_files:
                        cik, year, form_type = fm._parse_filename_metadata(Path(file_name))
                        if cik_filter is not None:
                            if not cik or cik not in cik_filter:
                                continue
                            if not form_type or not form_type.startswith("10-K"):
                                continue
                        try:
                            zf.extract(file_name, temp_path)
                            file_path = temp_path / file_name
                            result = self.extractor.extract_from_file(file_path)
                            if result:
                                stats["processed"] += 1
                            else:
                                stats["failed"] += 1
                                stats["errors"].append({"file": file_name, "error": "Extraction failed"})
                        except Exception as e:
                            stats["failed"] += 1
                            stats["errors"].append({"file": file_name, "error": str(e)})
                            log_error(f"Error processing {file_name} from {zip_path}: {e}")
        except zipfile.BadZipFile:
            log_error(f"Invalid ZIP file: {zip_path}")
            stats["errors"].append({"file": str(zip_path), "error": "Invalid ZIP file"})
        except Exception as e:
            log_error(f"Error processing ZIP file {zip_path}: {e}")
            stats["errors"].append({"file": str(zip_path), "error": str(e)})

        return stats

    def process_directory(
        self,
        input_dir: Path,
        cik_filter: Optional[Set[str]] = None
    ) -> Dict[str, any]:
        """
        Process all ZIP files in a directory.

        Args:
            input_dir: Directory containing ZIP files
            cik_filter: Optional set of CIK strings to filter files

        Returns:
            Overall processing statistics
        """
        overall_stats = {
            "total_zips": 0,
            "total_files": 0,
            "processed": 0,
            "failed": 0,
            "zip_stats": []
        }

        zip_files = []
        for ext in ZIP_EXTENSIONS:
            zip_files.extend(input_dir.glob(f"*{ext}"))
        zip_files = list(set(zip_files))
        overall_stats["total_zips"] = len(zip_files)

        logger.info(f"Found {len(zip_files)} ZIP files to process")

        for zip_path in sorted(zip_files):
            stats = self.process_zip_file(zip_path, cik_filter=cik_filter)
            overall_stats["zip_stats"].append(stats)
            overall_stats["total_files"] += stats["total_files"]
            overall_stats["processed"] += stats["processed"]
            overall_stats["failed"] += stats["failed"]

        return overall_stats

    def process_mixed_directory(
            self,
            input_dir: Path,
            resolve_references: bool = True,
            cik_filter: Optional[Set[str]] = None
    ) -> Dict[str, any]:
        """
        Process directory containing both ZIP files and loose text files,
        applying 10-Q fallback logic centrally via FilingManager.

        Args:
            input_dir: Input directory
            resolve_references: Whether to attempt resolving incorporation by reference
            cik_filter: Optional set of CIK strings to filter files

        Returns:
            Combined processing statistics
        """
        stats = {
            "zip_results": {"total_files": 0, "processed": 0, "failed": 0},
            "text_results": {"total_files": 0, "processed": 0, "failed": 0},
            "combined": {"total_files": 0, "processed": 0, "failed": 0, "skipped_10q": 0},
            "errors": []
        }

        # 1) Discover all text files (from ZIPs and loose)
        zip_text_files: List[Path] = []
        for zip_path in {*input_dir.glob("*.zip"), *input_dir.glob("*.ZIP")}:
            try:
                with zipfile.ZipFile(zip_path, 'r') as zf:
                    for member in zf.namelist():
                        if any(member.endswith(ext) for ext in VALID_EXTENSIONS):
                            tmp = tempfile.mkdtemp()
                            zf.extract(member, tmp)
                            zip_text_files.append(Path(tmp) / member)
            except Exception as e:
                log_error(f"Error listing {zip_path}: {e}")

        loose_files: List[Path] = []
        for ext in VALID_EXTENSIONS:
            loose_files.extend(input_dir.glob(f"*{ext}"))

        def _match(fp: Path) -> bool:
            if cik_filter is None:
                return True
            cik = self._extract_cik_from_name(fp.name)
            return cik is not None and cik in cik_filter and self._is_10k(fp.name)

        if cik_filter is not None:
            zip_text_files = [fp for fp in zip_text_files if _match(fp)]
            loose_files = [fp for fp in loose_files if _match(fp)]

        # ─── Dedupe any duplicates (e.g. .txt vs .TXT) ───
        zip_text_files = list(dict.fromkeys(zip_text_files))
        loose_files = list(dict.fromkeys(loose_files))

        stats["zip_results"]["total_files"] = len(zip_text_files)
        stats["text_results"]["total_files"] = len(loose_files)

        all_text_files = zip_text_files + loose_files
        stats["combined"]["total_files"] = len(all_text_files)

        # 2) Register with FilingManager
        fm = FilingManager()
        for fp in all_text_files:
            cik, year, form_type = fm._parse_filename_metadata(fp)
            if cik_filter is not None:
                if not cik or cik not in cik_filter:
                    continue
                if form_type and not form_type.startswith("10-K"):
                    continue
            if cik and year and form_type:
                fm.add_filing(fp, cik, year, form_type)

        # 3) Select which to process and skip
        selection = fm._select_filings_to_process()
        to_process = set(selection["process"])
        to_skip = set(selection["skip"])

        # Initialize reference resolver if requested
        reference_resolver = None
        if resolve_references:
            from src.core.reference_resolver import ReferenceResolver
            reference_resolver = ReferenceResolver(input_dir)

        # 4) Process only selected filings
        for fp in to_process:
            try:
                result = self.extractor.extract_from_file(fp, reference_resolver)
                if result:
                    stats["combined"]["processed"] += 1
                    if fp in zip_text_files:
                        stats["zip_results"]["processed"] += 1
                    else:
                        stats["text_results"]["processed"] += 1
                else:
                    stats["combined"]["failed"] += 1
                    stats["errors"].append(str(fp))
                    if fp in zip_text_files:
                        stats["zip_results"]["failed"] += 1
                    else:
                        stats["text_results"]["failed"] += 1
            except Exception as e:
                stats["combined"]["failed"] += 1
                stats["errors"].append(f"{fp}: {e}")
                if fp in zip_text_files:
                    stats["zip_results"]["failed"] += 1
                else:
                    stats["text_results"]["failed"] += 1

        # 5) Count skipped 10-Qs
        for fp in to_skip:
            _, _, ft = fm._parse_filename_metadata(fp)
            if ft and ft.startswith("10-Q"):
                stats["combined"]["skipped_10q"] += 1

        return stats
