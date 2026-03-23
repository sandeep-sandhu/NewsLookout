#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
BZ2/JSON Pair to -> ZIP Archive Converter for NewsLookout Web Scraping Application
===============================================================================
Scans date-named sub-directories in an input archive directory, locates
{plugin_name}_{article_id}.html.bz2 / .json file pairs, validates each pair,
decompresses the raw HTML, and packs both files into a per-date .zip archive
written to the output directory.  Invalid pairs are quarantined under an
'errors' sub-folder together with a plain-text error report.

Expected input layout
---------------------
    input_dir/
        YYYY-MM-DD/
            mod_en_in_example_12345678.json
            mod_en_in_example_12345678.html.bz2
            mod_en_in_other_99999999.json
            mod_en_in_other_99999999.html.bz2
            ...

Output layout produced
----------------------
    output_dir/
        YYYY-MM-DD.zip          ← flat ZIP (no sub-folders), one per date
        ...
        errors/
            YYYY-MM-DD/
                mod_en_in_example_12345678.json        ← copied as-is
                mod_en_in_example_12345678.html.bz2    ← copied as-is
                mod_en_in_example_12345678_error.txt   ← human-readable report
            ...

Validation rules applied to each pair
--------------------------------------
  1. Filename stem must equal  f"{json['module']}_{json['uniqueID']}"
     (e.g. "mod_en_in_moneycontrol_13733378")
  2. JSON 'pubdate' field must be >= 1900-01-01 and <= today's date.

Usage
-----
    python convert_pairs_to_zip.py  <input_dir>  <output_dir>  [options]

    Options:
      --overwrite    Re-create ZIP even if it already exists in output_dir.
      --dry-run      Simulate all work without writing any files.
      --verbose      Enable DEBUG-level logging.
      --workers N    Number of parallel date-directories to process (default 1).
"""

import os
import sys
import re
import bz2
import json
import shutil
import logging
import argparse
import zipfile
import tempfile
from pathlib import Path
from datetime import date, datetime
from dataclasses import dataclass, field
from typing import List, Tuple, Dict, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed

try:
    from tqdm import tqdm
    HAS_TQDM = True
except ImportError:
    HAS_TQDM = False
    print("NOTE: tqdm not installed – progress bars disabled. "
          "Install with:  pip install tqdm", file=sys.stderr)

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
DATE_DIR_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
MIN_PUBDATE = date(1900, 1, 1)
HTML_BZ2_EXT = ".html.bz2"
JSON_EXT = ".json"
HTML_EXT = ".html"
ERRORS_SUBDIR = "errors"


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------
@dataclass
class FilePair:
    """A matched .json + .html.bz2 pair inside a single date directory."""
    stem: str           # e.g. "mod_en_in_moneycontrol_13733378"  (from filename)
    json_path: Path
    bz2_path: Path
    date_str: str       # "YYYY-MM-DD"
    corrected_stem: Optional[str] = None  # set when JSON fields imply a different stem

    @property
    def effective_stem(self) -> str:
        """The stem to use when writing into the ZIP (corrected if available)."""
        return self.corrected_stem if self.corrected_stem else self.stem

    def __str__(self) -> str:
        return f"{self.date_str}/{self.stem}"


@dataclass
class ConversionStats:
    """Accumulated statistics for the whole run."""
    total_dates: int = 0
    total_pairs: int = 0
    successful: int = 0
    failed_validation: int = 0
    failed_conversion: int = 0
    skipped_dates: int = 0
    orphaned_json: int = 0
    orphaned_bz2: int = 0
    bytes_read_bz2: int = 0
    bytes_written_zip: int = 0
    errors: List[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _tqdm_wrap(iterable, **kwargs):
    """Return tqdm-wrapped iterable when available, plain iterable otherwise."""
    if HAS_TQDM:
        return tqdm(iterable, **kwargs)
    return iterable


def _parse_pubdate(date_str: str) -> Optional[date]:
    """Parse a pubdate string in YYYY-MM-DD format; return None on failure."""
    try:
        return datetime.strptime(date_str, "%Y-%m-%d").date()
    except (ValueError, TypeError):
        return None


# ---------------------------------------------------------------------------
# Pair discovery
# ---------------------------------------------------------------------------
def find_date_directories(input_dir: Path) -> List[Path]:
    """
    Return all immediate sub-directories of *input_dir* whose names match
    the YYYY-MM-DD pattern, sorted chronologically.
    """
    date_dirs = [
        d for d in sorted(input_dir.iterdir())
        if d.is_dir() and DATE_DIR_RE.match(d.name)
    ]
    logger.info("Found %d date-directories under %s", len(date_dirs), input_dir)
    return date_dirs


def find_file_pairs(date_dir: Path) -> Tuple[List[FilePair], List[Path], List[Path]]:
    """
    Discover matched .json / .html.bz2 pairs in *date_dir*.

    Returns
    -------
    pairs          : list of matched FilePair objects
    orphaned_json  : .json files with no matching .html.bz2
    orphaned_bz2   : .html.bz2 files with no matching .json
    """
    date_str = date_dir.name

    # Collect stems from each extension
    json_stems = {
        p.name[: -len(JSON_EXT)]: p
        for p in date_dir.iterdir()
        if p.is_file() and p.name.endswith(JSON_EXT)
    }
    bz2_stems = {
        p.name[: -len(HTML_BZ2_EXT)]: p
        for p in date_dir.iterdir()
        if p.is_file() and p.name.endswith(HTML_BZ2_EXT)
    }

    common_stems = set(json_stems) & set(bz2_stems)
    orphaned_json = [json_stems[s] for s in (set(json_stems) - common_stems)]
    orphaned_bz2 = [bz2_stems[s] for s in (set(bz2_stems) - common_stems)]

    pairs = [
        FilePair(
            stem=stem,
            json_path=json_stems[stem],
            bz2_path=bz2_stems[stem],
            date_str=date_str,
        )
        for stem in sorted(common_stems)
    ]

    logger.debug(
        "%s: %d pairs, %d orphaned JSON, %d orphaned BZ2",
        date_str, len(pairs), len(orphaned_json), len(orphaned_bz2),
    )
    return pairs, orphaned_json, orphaned_bz2


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------
def validate_pair(pair: FilePair) -> List[str]:
    """
    Run all validation checks on a FilePair.

    Returns a (possibly empty) list of human-readable error descriptions.
    An empty list means the pair is valid.
    """
    errors: List[str] = []

    # ── 1. Read and parse the JSON file ────────────────────────────────────
    try:
        with open(pair.json_path, "rt", encoding="utf-8") as fp:
            data = json.load(fp)
    except json.JSONDecodeError as exc:
        errors.append(f"JSON parse error: {exc}")
        return errors   # no point continuing if JSON is unreadable
    except OSError as exc:
        errors.append(f"Cannot read JSON file: {exc}")
        return errors

    # ── 2. Filename format check ────────────────────────────────────────────
    plugin_name = data.get("module", "")
    unique_id = data.get("uniqueID", "")

    if not plugin_name:
        errors.append(
            "JSON field 'module' is missing or empty; cannot verify filename."
        )
    if not unique_id:
        errors.append(
            "JSON field 'uniqueID' is missing or empty; cannot verify filename."
        )

    if plugin_name and unique_id:
        expected_stem = f"{plugin_name}_{unique_id}"
        if pair.stem != expected_stem:
            # Don't quarantine — JSON content is authoritative.
            # Record the correct stem so the ZIP entry is saved under the
            # right name. Log a warning so the rename is fully traceable.
            pair.corrected_stem = expected_stem
            logger.warning(
                "%s: Filename mismatch – on-disk stem is '%s', "
                "JSON fields imply '%s'. "
                "Pair will be stored in ZIP as '%s'.",
                pair, pair.stem, expected_stem, expected_stem,
            )
        else:
            logger.debug("%s: filename format OK (%s)", pair, pair.stem)

    # ── 3. Publication date range check ────────────────────────────────────
    raw_pubdate = data.get("pubdate", "")
    today = date.today()
    pubdate = _parse_pubdate(str(raw_pubdate))

    if pubdate is None:
        errors.append(
            f"Cannot parse pubdate '{raw_pubdate}' as YYYY-MM-DD."
        )
    elif pubdate < MIN_PUBDATE:
        errors.append(
            f"pubdate '{pubdate}' is before {MIN_PUBDATE} (minimum allowed)."
        )
    elif pubdate > today:
        errors.append(
            f"pubdate '{pubdate}' is in the future (today is {today})."
        )
    else:
        logger.debug("%s: pubdate OK (%s)", pair, pubdate)

    return errors


# ---------------------------------------------------------------------------
# Conversion
# ---------------------------------------------------------------------------
def decompress_bz2(bz2_path: Path) -> bytes:
    """
    Decompress a .html.bz2 file and return the raw HTML as bytes.

    Raises
    ------
    OSError / EOFError / ValueError  on read/decompress failure.
    """
    logger.debug("Decompressing %s", bz2_path)
    with bz2.open(bz2_path, "rb") as fp:
        html_bytes = fp.read()
    logger.debug("Decompressed %s → %d bytes", bz2_path.name, len(html_bytes))
    return html_bytes


# ---------------------------------------------------------------------------
# Error quarantine
# ---------------------------------------------------------------------------
def quarantine_pair(
    pair: FilePair,
    error_messages: List[str],
    errors_dir: Path,
    dry_run: bool,
) -> None:
    """
    Copy the raw file pair and a plain-text error report to the errors
    sub-directory so a human can inspect and reprocess them later.

    Directory layout created:
        errors_dir / YYYY-MM-DD / {stem}.json
                                   {stem}.html.bz2
                                   {stem}_error.txt
    """
    dest_dir = errors_dir / pair.date_str
    error_txt_path = dest_dir / f"{pair.stem}_error.txt"
    error_body = (
        f"File pair: {pair.stem}\n"
        f"Date directory: {pair.date_str}\n"
        f"Detected at: {datetime.now().isoformat(timespec='seconds')}\n"
        f"\nValidation errors:\n"
        + "\n".join(f"  • {msg}" for msg in error_messages)
        + "\n"
    )

    if dry_run:
        logger.info(
            "[DRY RUN] Would quarantine %s → %s  Errors: %s",
            pair, dest_dir, "; ".join(error_messages),
        )
        return

    dest_dir.mkdir(parents=True, exist_ok=True)
    shutil.copy2(pair.json_path, dest_dir / pair.json_path.name)
    shutil.copy2(pair.bz2_path,  dest_dir / pair.bz2_path.name)

    with open(error_txt_path, "wt", encoding="utf-8") as fp:
        fp.write(error_body)

    logger.warning(
        "Quarantined invalid pair %s → %s  [%s]",
        pair, dest_dir, "; ".join(error_messages),
    )


def quarantine_orphan(
    orphan_path: Path,
    reason: str,
    date_str: str,
    errors_dir: Path,
    dry_run: bool,
) -> None:
    """Copy a single orphaned file (no matching counterpart) to the errors dir."""
    dest_dir = errors_dir / date_str
    error_txt = dest_dir / f"{orphan_path.name}_error.txt"
    error_body = (
        f"Orphaned file: {orphan_path.name}\n"
        f"Date directory: {date_str}\n"
        f"Detected at: {datetime.now().isoformat(timespec='seconds')}\n"
        f"\nReason: {reason}\n"
    )

    if dry_run:
        logger.info("[DRY RUN] Would quarantine orphan %s/%s", date_str, orphan_path.name)
        return

    dest_dir.mkdir(parents=True, exist_ok=True)
    shutil.copy2(orphan_path, dest_dir / orphan_path.name)
    with open(error_txt, "wt", encoding="utf-8") as fp:
        fp.write(error_body)

    logger.warning("Quarantined orphan %s/%s: %s", date_str, orphan_path.name, reason)


# ---------------------------------------------------------------------------
# Per-date ZIP creation
# ---------------------------------------------------------------------------
def convert_date_directory(
    date_dir: Path,
    output_dir: Path,
    errors_dir: Path,
    overwrite: bool,
    dry_run: bool,
) -> ConversionStats:
    """
    Process all file pairs in one date directory and build (or update) the
    corresponding  YYYY-MM-DD.zip  in output_dir.

    Returns a ConversionStats with counts for this date only.
    """
    date_str = date_dir.name
    zip_path = output_dir / f"{date_str}.zip"
    stats = ConversionStats(total_dates=1)

    logger.info("── Processing date: %s ──────────────────────────────", date_str)

    # ── Discover pairs and orphans ──────────────────────────────────────────
    pairs, orphaned_json, orphaned_bz2 = find_file_pairs(date_dir)
    stats.total_pairs = len(pairs)
    stats.orphaned_json = len(orphaned_json)
    stats.orphaned_bz2 = len(orphaned_bz2)

    # Quarantine orphans immediately
    for op in orphaned_json:
        quarantine_orphan(
            op, "No matching .html.bz2 file found", date_str, errors_dir, dry_run
        )
    for op in orphaned_bz2:
        quarantine_orphan(
            op, "No matching .json file found", date_str, errors_dir, dry_run
        )

    if not pairs:
        logger.info("%s: No valid file pairs found; skipping ZIP creation.", date_str)
        stats.skipped_dates = 1
        return stats

    # ── Skip if ZIP already exists and overwrite not requested ─────────────
    if zip_path.exists() and not overwrite:
        logger.warning(
            "%s: ZIP already exists at %s; skipping (use --overwrite to force).",
            date_str, zip_path,
        )
        stats.skipped_dates = 1
        return stats

    # ── Validate all pairs first; separate valid from invalid ───────────────
    valid_pairs: List[FilePair] = []
    for pair in pairs:
        validation_errors = validate_pair(pair)
        if validation_errors:
            stats.failed_validation += 1
            stats.errors.append(
                f"{pair}: {'; '.join(validation_errors)}"
            )
            quarantine_pair(pair, validation_errors, errors_dir, dry_run)
        else:
            valid_pairs.append(pair)

    logger.info(
        "%s: %d/%d pairs passed validation.", date_str, len(valid_pairs), len(pairs)
    )

    if not valid_pairs:
        logger.warning("%s: No valid pairs remain; ZIP will not be created.", date_str)
        stats.skipped_dates = 1
        return stats

    # ── Build ZIP (via a temp file for atomic write) ────────────────────────
    if dry_run:
        for pair in valid_pairs:
            logger.info("[DRY RUN] Would add %s.html + %s.json to %s", pair.effective_stem, pair.effective_stem, zip_path)
            stats.successful += 1
        return stats

    tmp_zip_path: Optional[Path] = None
    try:
        tmp_fd, tmp_zip_str = tempfile.mkstemp(
            dir=output_dir, prefix=f".tmp_{date_str}_", suffix=".zip"
        )
        os.close(tmp_fd)
        tmp_zip_path = Path(tmp_zip_str)

        with zipfile.ZipFile(tmp_zip_path, "w", compression=zipfile.ZIP_DEFLATED,
                             compresslevel=6) as zf:
            for pair in valid_pairs:
                # ── Decompress HTML ─────────────────────────────────────────
                try:
                    html_bytes = decompress_bz2(pair.bz2_path)
                    stats.bytes_read_bz2 += pair.bz2_path.stat().st_size
                except Exception as exc:
                    logger.error(
                        "%s: Failed to decompress %s: %s", pair, pair.bz2_path.name, exc
                    )
                    stats.failed_conversion += 1
                    stats.errors.append(f"{pair}: decompression error – {exc}")
                    quarantine_pair(
                        pair,
                        [f"Decompression failed: {exc}"],
                        errors_dir,
                        dry_run=False,
                    )
                    continue

                # ── Write decompressed HTML into ZIP ────────────────────────
                html_arcname = f"{pair.effective_stem}{HTML_EXT}"
                zf.writestr(html_arcname, html_bytes)
                logger.debug("%s: Added %s (%d bytes)", date_str, html_arcname, len(html_bytes))

                # ── Write JSON into ZIP ──────────────────────────────────────
                json_arcname = f"{pair.effective_stem}{JSON_EXT}"
                zf.write(pair.json_path, arcname=json_arcname)
                logger.debug("%s: Added %s", date_str, json_arcname)

                stats.successful += 1
                if pair.corrected_stem:
                    logger.info("  ✓ %s  (renamed from '%s')", pair.effective_stem, pair.stem)
                else:
                    logger.info("  ✓ %s", pair.stem)

        # ── Verify the written ZIP ──────────────────────────────────────────
        _verify_zip(tmp_zip_path, valid_pairs, stats)

        # ── Atomic rename temp → final ──────────────────────────────────────
        shutil.move(str(tmp_zip_path), zip_path)
        stats.bytes_written_zip += zip_path.stat().st_size
        logger.info(
            "%s: ZIP written → %s  (%.1f KB)",
            date_str, zip_path.name, zip_path.stat().st_size / 1024,
        )

    except Exception as exc:
        logger.error("%s: Unexpected error creating ZIP: %s", date_str, exc, exc_info=True)
        stats.errors.append(f"{date_str}: ZIP creation failed – {exc}")
    finally:
        if tmp_zip_path and tmp_zip_path.exists():
            try:
                tmp_zip_path.unlink()
            except OSError:
                pass

    return stats


def _verify_zip(zip_path: Path, expected_pairs: List[FilePair], stats: ConversionStats) -> None:
    """
    Open the newly written ZIP and confirm every expected file is present and
    the archive passes ZipFile's own integrity check.  Logs warnings on any
    discrepancy but does NOT raise (the caller handles the returned stats).
    """
    try:
        with zipfile.ZipFile(zip_path, "r") as zf:
            bad = zf.testzip()
            if bad:
                logger.error("ZIP integrity check failed; first bad file: %s", bad)
                stats.errors.append(f"ZIP integrity failure: first bad entry = {bad}")
                return

            archived_names = set(zf.namelist())
            for pair in expected_pairs:
                for ext in (HTML_EXT, JSON_EXT):
                    expected = f"{pair.effective_stem}{ext}"
                    if expected not in archived_names:
                        logger.warning("ZIP is missing expected entry: %s", expected)
                        stats.errors.append(f"Missing from ZIP: {expected}")

        logger.debug("ZIP verification passed: %s", zip_path.name)
    except zipfile.BadZipFile as exc:
        logger.error("ZIP verification failed – bad ZIP file: %s", exc)
        stats.errors.append(f"ZIP verification failed: {exc}")


# ---------------------------------------------------------------------------
# Main orchestrator
# ---------------------------------------------------------------------------
class ArchiveConverter:
    """
    Orchestrates scanning, validation, and ZIP creation across all date
    directories in the input archive.
    """

    def __init__(
        self,
        input_dir: str,
        output_dir: str,
        overwrite: bool = False,
        dry_run: bool = False,
        workers: int = 1,
    ):
        self.input_dir  = Path(input_dir).resolve()
        self.output_dir = Path(output_dir).resolve()
        self.errors_dir = self.output_dir / ERRORS_SUBDIR
        self.overwrite  = overwrite
        self.dry_run    = dry_run
        self.workers    = max(1, workers)

        if not self.input_dir.exists():
            raise ValueError(f"Input directory does not exist: {self.input_dir}")

        if not self.dry_run:
            self.output_dir.mkdir(parents=True, exist_ok=True)
            self.errors_dir.mkdir(parents=True, exist_ok=True)

        logger.info("ArchiveConverter initialised")
        logger.info("  Input  directory : %s", self.input_dir)
        logger.info("  Output directory : %s", self.output_dir)
        logger.info("  Errors directory : %s", self.errors_dir)
        logger.info("  Overwrite        : %s", self.overwrite)
        logger.info("  Dry run          : %s", self.dry_run)
        logger.info("  Worker threads   : %s", self.workers)

    # ── Public API ──────────────────────────────────────────────────────────
    def convert_all(self) -> ConversionStats:
        """
        Discover all date directories and convert each one.  Returns aggregate
        statistics for the whole run.
        """
        date_dirs = find_date_directories(self.input_dir)
        total_stats = ConversionStats(total_dates=len(date_dirs))

        if not date_dirs:
            logger.warning("No YYYY-MM-DD sub-directories found in %s", self.input_dir)
            return total_stats

        logger.info("Starting conversion of %d date-directories …", len(date_dirs))

        if self.workers == 1:
            self._convert_sequential(date_dirs, total_stats)
        else:
            self._convert_parallel(date_dirs, total_stats)

        return total_stats

    def generate_report(self, stats: ConversionStats) -> None:
        """Log a human-readable summary of the completed run."""
        bar = "=" * 64
        logger.info(bar)
        logger.info("CONVERSION SUMMARY")
        logger.info(bar)
        logger.info("Date directories scanned   : %d", stats.total_dates)
        logger.info("Date directories skipped   : %d", stats.skipped_dates)
        logger.info("Total file pairs found     : %d", stats.total_pairs)
        logger.info("Successfully converted     : %d", stats.successful)
        logger.info("Failed validation          : %d", stats.failed_validation)
        logger.info("Failed conversion (I/O)    : %d", stats.failed_conversion)
        logger.info("Orphaned JSON files        : %d", stats.orphaned_json)
        logger.info("Orphaned BZ2 files         : %d", stats.orphaned_bz2)
        if stats.bytes_read_bz2 > 0:
            logger.info(
                "Total BZ2 bytes read       : %.2f MB",
                stats.bytes_read_bz2 / 1_048_576,
            )
        if stats.bytes_written_zip > 0:
            logger.info(
                "Total ZIP bytes written    : %.2f MB",
                stats.bytes_written_zip / 1_048_576,
            )
            if stats.bytes_read_bz2 > 0:
                ratio = (stats.bytes_written_zip / stats.bytes_read_bz2) * 100
                logger.info("Size ratio (ZIP/BZ2)       : %.1f%%", ratio)
        logger.info(bar)

        if stats.errors:
            logger.warning("Errors encountered (%d total):", len(stats.errors))
            for msg in stats.errors:
                logger.warning("  • %s", msg)
            logger.info(bar)

    # ── Internal helpers ────────────────────────────────────────────────────
    def _process_one_date(self, date_dir: Path) -> ConversionStats:
        """Thin wrapper used by both sequential and parallel paths."""
        return convert_date_directory(
            date_dir=date_dir,
            output_dir=self.output_dir,
            errors_dir=self.errors_dir,
            overwrite=self.overwrite,
            dry_run=self.dry_run,
        )

    def _merge_stats(self, total: ConversionStats, partial: ConversionStats) -> None:
        """Accumulate per-date stats into the running total."""
        # total_dates already set from the full list size; don't double-count
        total.total_pairs      += partial.total_pairs
        total.successful       += partial.successful
        total.failed_validation += partial.failed_validation
        total.failed_conversion += partial.failed_conversion
        total.skipped_dates    += partial.skipped_dates
        total.orphaned_json    += partial.orphaned_json
        total.orphaned_bz2     += partial.orphaned_bz2
        total.bytes_read_bz2   += partial.bytes_read_bz2
        total.bytes_written_zip += partial.bytes_written_zip
        total.errors.extend(partial.errors)

    def _convert_sequential(
        self, date_dirs: List[Path], total_stats: ConversionStats
    ) -> None:
        iterable = _tqdm_wrap(
            date_dirs,
            desc="Date directories",
            unit="date",
            dynamic_ncols=True,
            colour="cyan",
        )
        for date_dir in iterable:
            if HAS_TQDM:
                iterable.set_postfix_str(date_dir.name)  # type: ignore[union-attr]
            partial = self._process_one_date(date_dir)
            self._merge_stats(total_stats, partial)

    def _convert_parallel(
        self, date_dirs: List[Path], total_stats: ConversionStats
    ) -> None:
        logger.info("Using %d parallel worker threads.", self.workers)
        futures_map = {}

        progress = _tqdm_wrap(
            date_dirs,
            total=len(date_dirs),
            desc="Date directories",
            unit="date",
            dynamic_ncols=True,
            colour="cyan",
        ) if HAS_TQDM else None

        with ThreadPoolExecutor(max_workers=self.workers) as executor:
            for date_dir in date_dirs:
                future = executor.submit(self._process_one_date, date_dir)
                futures_map[future] = date_dir

            for future in as_completed(futures_map):
                date_dir = futures_map[future]
                try:
                    partial = future.result()
                    self._merge_stats(total_stats, partial)
                except Exception as exc:
                    logger.error("Worker failed for %s: %s", date_dir.name, exc)
                    total_stats.errors.append(f"{date_dir.name}: worker error – {exc}")
                finally:
                    if progress:
                        progress.update(1)
                        progress.set_postfix_str(date_dir.name)  # type: ignore[union-attr]

        if progress:
            progress.close()  # type: ignore[union-attr]


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Convert NewsLookout BZ2/JSON file pairs stored in YYYY-MM-DD "
            "sub-directories into per-date .zip archives. "
            "Invalid pairs are quarantined under <output_dir>/errors/."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Example:\n"
            "  python convert_pairs_to_zip.py  ./data  ./archive\n"
            "  python convert_pairs_to_zip.py  ./data  ./archive  --overwrite --workers 4\n"
            "  python convert_pairs_to_zip.py  ./data  ./archive  --dry-run --verbose\n"
        ),
    )

    parser.add_argument(
        "input_dir",
        help="Root directory containing YYYY-MM-DD sub-directories with file pairs.",
    )
    parser.add_argument(
        "output_dir",
        help="Directory where per-date .zip files and the errors/ folder will be written.",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Re-create a .zip archive even if it already exists in output_dir.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Simulate all operations without writing any files.",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable DEBUG-level logging.",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=1,
        metavar="N",
        help="Number of parallel worker threads for processing date directories (default: 1).",
    )
    return parser


def main() -> int:
    parser = _build_arg_parser()
    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
        logger.debug("Verbose / DEBUG logging enabled.")

    if args.dry_run:
        logger.info("DRY-RUN mode – no files will be created or modified.")

    try:
        converter = ArchiveConverter(
            input_dir=args.input_dir,
            output_dir=args.output_dir,
            overwrite=args.overwrite,
            dry_run=args.dry_run,
            workers=args.workers,
        )
        stats = converter.convert_all()
        converter.generate_report(stats)

        # Return non-zero exit code when any errors occurred
        if stats.failed_validation > 0 or stats.failed_conversion > 0:
            return 1
        return 0

    except ValueError as exc:
        logger.error("Configuration error: %s", exc)
        return 2
    except Exception as exc:
        logger.error("Fatal error: %s", exc, exc_info=True)
        return 3


if __name__ == "__main__":
    sys.exit(main())


# # end of file ##
