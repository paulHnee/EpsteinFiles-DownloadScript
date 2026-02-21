"""
DOJ Epstein Files Downloader
Downloads files from justice.gov/epstein/files across all 12 DataSets.
For each file number, detects the actual file type via Content-Type header
and downloads it in one request. Checks .pdf, .mov, .png, .jpeg, .jpg.

Features:
  - Concurrent scanning & downloading (thread pool)
  - Retry with exponential backoff
  - Content-Length integrity check
  - Magic bytes validation
  - Age-verification cookie with periodic refresh
  - tqdm progress bar
  - manifest.csv log of all downloads
  - --verify mode to check existing files
  - --start-from to resume mid-dataset
"""

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from concurrent.futures import ThreadPoolExecutor, as_completed
import os
import csv
import time
import logging
import argparse
import threading
from datetime import datetime

try:
    from tqdm import tqdm
    HAS_TQDM = True
except ImportError:
    HAS_TQDM = False

# ──────────────────────────────────────────────────────────────
# Configuration
# ──────────────────────────────────────────────────────────────
BASE_URL = "https://www.justice.gov/epstein/files/"
EXTENSIONS = [".pdf", ".mov", ".png", ".jpeg", ".jpg"]
DEFAULT_DOWNLOAD_DIR = r"E:\Epstein"
REQUEST_TIMEOUT = 30
CHUNK_SIZE = 8192
COOKIE_REFRESH_INTERVAL = 3000  # seconds (~50 min, cookie lasts 60 min)
DEFAULT_DELAY = 0.5              # seconds between requests per worker
DEFAULT_WORKERS = 3              # conservative default to avoid rate limiting

# Content-Type → file extension mapping
CONTENT_TYPE_MAP = {
    "application/pdf":  ".pdf",
    "application/octet-stream": None,
    "video/quicktime":  ".mov",
    "video/mp4":        ".mov",
    "image/png":        ".png",
    "image/jpeg":       ".jpg",
    "image/jpg":        ".jpg",
}

REJECT_CONTENT_TYPES = {"text/html", "text/plain", "application/xhtml+xml"}

MAGIC_BYTES = {
    ".pdf":  b"%PDF",
    ".png":  b"\x89PNG",
    ".jpg":  b"\xff\xd8\xff",
    ".jpeg": b"\xff\xd8\xff",
}

DATASETS = [
    (1,  1),
    (2,  3159),
    (3,  3380),
    (4,  5705),
    (5,  8409),
    (6,  8585),
    (7,  9016),
    (8,  9676),
    (9,  39025),
    (10, 1262782),
    (11, 2205655),
    (12, 2730265),
]

ABSOLUTE_END = 2731852

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


# ──────────────────────────────────────────────────────────────
# Session factory with retry + connection pooling
# ──────────────────────────────────────────────────────────────
def create_session(pool_size=10):
    """Create a requests session with retry logic and tuned connection pool."""
    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                      "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    })

    retry_strategy = Retry(
        total=3,
        backoff_factor=1,           # 1s, 2s, 4s
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "HEAD"],
    )
    adapter = HTTPAdapter(
        max_retries=retry_strategy,
        pool_connections=pool_size,
        pool_maxsize=pool_size,
    )
    session.mount("https://", adapter)
    session.mount("http://", adapter)

    refresh_age_cookie(session)
    return session


def refresh_age_cookie(session):
    """Set the DOJ age-verification cookie."""
    session.cookies.set("justiceGovAgeVerified", "true", domain="www.justice.gov")


# ──────────────────────────────────────────────────────────────
# Cookie refresh timer
# ──────────────────────────────────────────────────────────────
class CookieRefresher:
    """Periodically refreshes the age-verification cookie."""

    def __init__(self, session, interval=COOKIE_REFRESH_INTERVAL):
        self.session = session
        self.interval = interval
        self.last_refresh = time.time()
        self._lock = threading.Lock()

    def check(self):
        with self._lock:
            if time.time() - self.last_refresh > self.interval:
                refresh_age_cookie(self.session)
                self.last_refresh = time.time()
                log.info("  Refreshed age-verification cookie")


# ──────────────────────────────────────────────────────────────
# Manifest CSV logger
# ──────────────────────────────────────────────────────────────
class ManifestWriter:
    """Thread-safe CSV writer for download manifest."""

    def __init__(self, filepath):
        self.filepath = filepath
        self._lock = threading.Lock()
        write_header = not os.path.exists(filepath)
        self._file = open(filepath, "a", newline="", encoding="utf-8")
        self._writer = csv.writer(self._file)
        if write_header:
            self._writer.writerow([
                "timestamp", "dataset", "filename", "size_bytes",
                "content_type", "status",
            ])
            self._file.flush()

    def log(self, dataset, filename, size_bytes, content_type, status):
        with self._lock:
            self._writer.writerow([
                datetime.now().isoformat(timespec="seconds"),
                dataset, filename, size_bytes, content_type, status,
            ])
            self._file.flush()

    def close(self):
        self._file.close()


# ──────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────
def build_url(dataset_num, file_num, ext):
    filename = f"EFTA{file_num:08d}{ext}"
    return f"{BASE_URL}DataSet%20{dataset_num}/{filename}"


def detect_extension(content_type):
    if not content_type:
        return None
    ct = content_type.split(";")[0].strip().lower()
    return CONTENT_TYPE_MAP.get(ct)


def is_valid_content_type(content_type):
    if not content_type:
        return True
    ct = content_type.split(";")[0].strip().lower()
    return ct not in REJECT_CONTENT_TYPES


def validate_magic_bytes(filepath, ext):
    expected = MAGIC_BYTES.get(ext)
    if expected is None:
        try:
            with open(filepath, "rb") as f:
                head = f.read(64)
            if head.lstrip().startswith((b"<", b"<!DOCTYPE", b"<!doctype")):
                return False
            return True
        except OSError:
            return False
    try:
        with open(filepath, "rb") as f:
            head = f.read(len(expected))
        return head.startswith(expected)
    except OSError:
        return False


def find_existing_file(download_dir, dataset_num, file_num):
    """Return the path of an existing file for this number, or None."""
    folder = os.path.join(download_dir, f"DataSet {dataset_num}")
    base = f"EFTA{file_num:08d}"
    for ext in EXTENSIONS:
        path = os.path.join(folder, base + ext)
        if os.path.exists(path) and os.path.getsize(path) > 0:
            return path
    return None


# ──────────────────────────────────────────────────────────────
# Single file download (used by workers)
# ──────────────────────────────────────────────────────────────
def try_download_file(session, dataset_num, file_num, download_dir, cookie_refresher, delay):
    """
    Try each extension for a file number.
    Returns (success: bool, filename: str|None, size: int, content_type: str).
    """
    cookie_refresher.check()
    base = f"EFTA{file_num:08d}"

    for ext in EXTENSIONS:
        url = build_url(dataset_num, file_num, ext)
        try:
            resp = session.get(url, timeout=REQUEST_TIMEOUT, stream=True)

            if resp.status_code == 429:
                log.warning(f"  Rate limited (429) on {url} — pausing 10s")
                time.sleep(10)
                resp.close()
                continue
            if resp.status_code == 200:
                content_type = resp.headers.get("Content-Type", "")

                if not is_valid_content_type(content_type):
                    resp.close()
                    continue

                real_ext = detect_extension(content_type)
                if real_ext is None:
                    real_ext = ext

                filename = base + real_ext
                folder = os.path.join(download_dir, f"DataSet {dataset_num}")
                os.makedirs(folder, exist_ok=True)
                filepath = os.path.join(folder, filename)

                # Content-Length is only reliable when there's no compression
                is_compressed = bool(resp.headers.get("Content-Encoding"))
                expected_size = resp.headers.get("Content-Length")
                expected_size = int(expected_size) if (expected_size and not is_compressed) else None

                # Stream to disk
                written = 0
                with open(filepath, "wb") as f:
                    for chunk in resp.iter_content(chunk_size=CHUNK_SIZE):
                        f.write(chunk)
                        written += len(chunk)

                # Content-Length integrity check (skipped for compressed responses)
                if expected_size is not None and written != expected_size:
                    os.remove(filepath)
                    log.warning(
                        f"  Truncated: {filename} "
                        f"(got {written} bytes, expected {expected_size})"
                    )
                    continue

                # Magic bytes validation
                if not validate_magic_bytes(filepath, real_ext):
                    os.remove(filepath)
                    log.warning(
                        f"  Rejected: DataSet {dataset_num} / {filename}  "
                        f"(invalid content, not a real {real_ext} file)"
                    )
                    continue

                return True, filename, written, content_type

            resp.close()

        except requests.RequestException as e:
            log.debug(f"  Request error for {url}: {e}")

        if delay:
            time.sleep(delay)

    return False, None, 0, ""


# ──────────────────────────────────────────────────────────────
# Concurrent dataset scanner
# ──────────────────────────────────────────────────────────────
def process_dataset(
    session, dataset_num, start, end_limit, max_misses,
    download_dir, workers, delay, cookie_refresher, manifest
):
    """
    Scan file numbers concurrently using a thread pool.
    Submits batches of file numbers and tracks consecutive misses.
    """
    found = 0
    skipped = 0
    downloaded = 0
    consecutive_misses = 0
    current = start
    batch_size = workers * 2  # keep the pool fed

    pbar = None
    if HAS_TQDM:
        pbar = tqdm(
            desc=f"  DataSet {dataset_num}",
            unit=" files",
            dynamic_ncols=True,
        )

    while current <= end_limit and consecutive_misses < max_misses:
        # Build a batch of file numbers to check
        batch = []
        for i in range(batch_size):
            num = current + i
            if num > end_limit:
                break
            batch.append(num)
        if not batch:
            break

        # Submit batch to thread pool
        futures = {}
        with ThreadPoolExecutor(max_workers=workers) as pool:
            for file_num in batch:
                existing = find_existing_file(download_dir, dataset_num, file_num)
                if existing:
                    found += 1
                    skipped += 1
                    consecutive_misses = 0
                    if pbar is not None:
                        pbar.update(1)
                        pbar.set_postfix(found=found, new=downloaded, skip=skipped)
                    continue
                fut = pool.submit(
                    try_download_file,
                    session, dataset_num, file_num, download_dir, cookie_refresher, delay,
                )
                futures[fut] = file_num

            # Collect results in submission order for correct consecutive miss tracking
            results = {}
            for fut in as_completed(futures):
                file_num = futures[fut]
                results[file_num] = fut.result()

        # Process results in order
        for file_num in sorted(results.keys()):
            success, filename, size, content_type = results[file_num]
            if success:
                found += 1
                downloaded += 1
                consecutive_misses = 0
                manifest.log(dataset_num, filename, size, content_type, "ok")
                if pbar is not None:
                    pbar.update(1)
                    pbar.set_postfix(found=found, new=downloaded, skip=skipped)
            else:
                consecutive_misses += 1
                if pbar is not None:
                    pbar.update(1)

            if consecutive_misses >= max_misses:
                break

        current += len(batch)

    if pbar is not None:
        pbar.set_postfix(found=found, new=downloaded, skip=skipped)
        pbar.close()

    log.info(
        f"  DataSet {dataset_num} done — {found} files "
        f"({downloaded} new, {skipped} skipped, stopped at EFTA{current - 1:08d})"
    )
    return found, downloaded, skipped


# ──────────────────────────────────────────────────────────────
# Verify mode
# ──────────────────────────────────────────────────────────────
def verify_existing_files(download_dir):
    """Check all downloaded files for corruption using magic bytes."""
    corrupt = []
    checked = 0

    all_files = []
    for ds_num in range(1, 13):
        folder = os.path.join(download_dir, f"DataSet {ds_num}")
        if not os.path.isdir(folder):
            continue
        for fname in os.listdir(folder):
            fpath = os.path.join(folder, fname)
            if os.path.isfile(fpath):
                all_files.append((ds_num, fname, fpath))

    if not all_files:
        log.info("No files found to verify.")
        return []

    iterator = all_files
    if HAS_TQDM:
        iterator = tqdm(all_files, desc="  Verifying", unit=" files", dynamic_ncols=True)

    for ds_num, fname, fpath in iterator:
        ext = os.path.splitext(fname)[1].lower()
        if not validate_magic_bytes(fpath, ext):
            corrupt.append((ds_num, fname, fpath))
            log.warning(f"  CORRUPT: DataSet {ds_num} / {fname}")
        checked += 1

    log.info(f"  Verified {checked} files — {len(corrupt)} corrupt")
    return corrupt


# ──────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(
        description="Download DOJ Epstein files",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "-o", "--output", default=DEFAULT_DOWNLOAD_DIR,
        help=f"Download directory (default: {DEFAULT_DOWNLOAD_DIR})",
    )
    parser.add_argument(
        "-w", "--workers", type=int, default=DEFAULT_WORKERS,
        help=f"Concurrent download threads (default: {DEFAULT_WORKERS})",
    )
    parser.add_argument(
        "-m", "--max-misses", type=int, default=50,
        help="Stop scanning a dataset after N consecutive misses (default: 50)",
    )
    parser.add_argument(
        "-d", "--delay", type=float, default=DEFAULT_DELAY,
        help=f"Delay in seconds between requests per worker (default: {DEFAULT_DELAY})",
    )
    parser.add_argument(
        "--datasets", type=str, default=None,
        help="Comma-separated dataset numbers to process (e.g. 1,2,12). Default: all",
    )
    parser.add_argument(
        "--start-from", type=int, default=None,
        help="Start scanning from this EFTA file number (overrides dataset start)",
    )
    parser.add_argument(
        "--verify", action="store_true",
        help="Verify existing files for corruption (no downloading)",
    )
    args = parser.parse_args()

    download_dir = args.output
    os.makedirs(download_dir, exist_ok=True)

    # ── Verify mode ──
    if args.verify:
        log.info("=" * 60)
        log.info("VERIFY MODE — checking existing files")
        log.info("=" * 60)
        corrupt = verify_existing_files(download_dir)
        if corrupt:
            log.info(f"\nCorrupt files ({len(corrupt)}):")
            for ds, fname, fpath in corrupt:
                log.info(f"  DataSet {ds} / {fname}")
            log.info("\nRe-run without --verify to re-download these files.")
            log.info("Delete the corrupt files first, then re-run.")
        else:
            log.info("\nAll files are valid.")
        return

    # ── Download mode ──
    if args.datasets:
        selected = set(int(x.strip()) for x in args.datasets.split(","))
        datasets = [(ds, s) for ds, s in DATASETS if ds in selected]
    else:
        datasets = DATASETS

    session = create_session(pool_size=args.workers + 2)
    cookie_refresher = CookieRefresher(session)

    manifest_path = os.path.join(download_dir, "manifest.csv")
    manifest = ManifestWriter(manifest_path)

    # Build end limits
    sorted_ds = sorted(DATASETS, key=lambda x: x[1])
    end_limits = {}
    for i, (ds, start) in enumerate(sorted_ds):
        if i + 1 < len(sorted_ds):
            end_limits[ds] = sorted_ds[i + 1][1] - 1
        else:
            end_limits[ds] = ABSOLUTE_END

    total_found = 0
    total_downloaded = 0
    total_skipped = 0

    log.info("=" * 60)
    log.info("DOJ Epstein Files Downloader")
    log.info(f"  Output   : {download_dir}")
    log.info(f"  Workers  : {args.workers}")
    log.info(f"  Delay    : {args.delay}s per worker")
    log.info(f"  Misses   : {args.max_misses}")
    log.info(f"  Manifest : {manifest_path}")
    if args.start_from:
        log.info(f"  Start at : EFTA{args.start_from:08d}")
    if not HAS_TQDM:
        log.info("  (install tqdm for progress bars: pip install tqdm)")
    log.info("=" * 60)

    for ds_num, ds_start in datasets:
        end = end_limits[ds_num]

        # --start-from: if the number falls in this dataset's range, use it
        start = ds_start
        if args.start_from is not None:
            if ds_start <= args.start_from <= end:
                start = args.start_from
            elif args.start_from > end:
                log.info(f"\nDataSet {ds_num}: skipped (start-from is past this dataset)")
                continue

        range_size = end - start + 1
        log.info(
            f"\nDataSet {ds_num}: EFTA{start:08d} -> EFTA{end:08d} "
            f"(range: {range_size:,})"
        )

        found, downloaded, skipped = process_dataset(
            session, ds_num, start, end, args.max_misses,
            download_dir, args.workers, args.delay, cookie_refresher, manifest,
        )
        total_found += found
        total_downloaded += downloaded
        total_skipped += skipped

    manifest.close()

    log.info("\n" + "=" * 60)
    log.info("COMPLETE")
    log.info(f"  Total files  : {total_found}")
    log.info(f"  New downloads: {total_downloaded}")
    log.info(f"  Already had  : {total_skipped}")
    log.info(f"  Manifest     : {manifest_path}")
    log.info("=" * 60)


if __name__ == "__main__":
    main()
