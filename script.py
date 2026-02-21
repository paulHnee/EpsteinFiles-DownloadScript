"""
DOJ Epstein Files Downloader
Downloads files from justice.gov/epstein/files across all 12 DataSets.
For each file number, detects the actual file type via Content-Type header
and downloads it in one request. Checks .pdf, .mov, .png, .jpeg, .jpg.
"""

import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
import os
import time
import logging
import argparse
import mimetypes

# ──────────────────────────────────────────────────────────────
# Configuration
# ──────────────────────────────────────────────────────────────
BASE_URL = "https://www.justice.gov/epstein/files/"
# Try pdf first (most common), then images/video
EXTENSIONS = [".pdf", ".mov", ".png", ".jpeg", ".jpg"]
DEFAULT_DOWNLOAD_DIR = r"E:\Epstein"
REQUEST_TIMEOUT = 30
CHUNK_SIZE = 8192

# Content-Type → file extension mapping
CONTENT_TYPE_MAP = {
    "application/pdf":  ".pdf",
    "video/quicktime":  ".mov",
    "video/mp4":        ".mov",
    "image/png":        ".png",
    "image/jpeg":       ".jpg",
    "image/jpg":        ".jpg",
}

# Each tuple: (dataset_number, first_known_file_number)
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
# Helpers
# ──────────────────────────────────────────────────────────────
def build_url(dataset_num, file_num, ext):
    filename = f"EFTA{file_num:08d}{ext}"
    return f"{BASE_URL}DataSet%20{dataset_num}/{filename}"


def detect_extension(content_type):
    """Determine file extension from the Content-Type header."""
    if not content_type:
        return None
    # Strip parameters like charset
    ct = content_type.split(";")[0].strip().lower()
    return CONTENT_TYPE_MAP.get(ct)


def already_downloaded(download_dir, dataset_num, file_num):
    """Check if any version of this file number already exists locally."""
    folder = os.path.join(download_dir, f"DataSet {dataset_num}")
    base = f"EFTA{file_num:08d}"
    for ext in EXTENSIONS:
        path = os.path.join(folder, base + ext)
        if os.path.exists(path) and os.path.getsize(path) > 0:
            return True
    return False


def try_download_file(session, dataset_num, file_num, download_dir, delay):
    """
    Try each extension for a file number. On the first 200 response:
    - Read Content-Type to detect real file type
    - Stream-download immediately
    - Return (True, filename) on success
    Returns (False, None) if file doesn't exist in any format.
    """
    base = f"EFTA{file_num:08d}"

    for ext in EXTENSIONS:
        url = build_url(dataset_num, file_num, ext)
        try:
            resp = session.get(url, timeout=REQUEST_TIMEOUT, stream=True)

            if resp.status_code == 200:
                # Detect real type from Content-Type header
                real_ext = detect_extension(resp.headers.get("Content-Type"))
                if real_ext is None:
                    real_ext = ext  # Fall back to the extension we requested

                filename = base + real_ext
                folder = os.path.join(download_dir, f"DataSet {dataset_num}")
                os.makedirs(folder, exist_ok=True)
                filepath = os.path.join(folder, filename)

                # Stream to disk
                with open(filepath, "wb") as f:
                    for chunk in resp.iter_content(chunk_size=CHUNK_SIZE):
                        f.write(chunk)

                size_kb = os.path.getsize(filepath) / 1024
                content_type = resp.headers.get("Content-Type", "unknown")
                log.info(
                    f"  Downloaded: DataSet {dataset_num} / {filename}  "
                    f"({size_kb:.0f} KB, type: {content_type})"
                )
                return True, filename

            # Close the response if not 200
            resp.close()

        except requests.RequestException:
            pass

        if delay:
            time.sleep(delay)

    return False, None


# ──────────────────────────────────────────────────────────────
# Scan + Download per dataset
# ──────────────────────────────────────────────────────────────
def process_dataset(session, dataset_num, start, end_limit, max_misses, delay, download_dir):
    """
    Scan through file numbers for a dataset.
    For each number, check if it exists (trying all extensions).
    Downloads immediately on hit. Stops after max_misses consecutive misses.
    """
    found = 0
    skipped = 0
    consecutive_misses = 0
    current = start

    while current <= end_limit and consecutive_misses < max_misses:

        # Skip if already on disk
        if already_downloaded(download_dir, dataset_num, current):
            found += 1
            skipped += 1
            consecutive_misses = 0
            current += 1
            continue

        success, filename = try_download_file(
            session, dataset_num, current, download_dir, delay
        )

        if success:
            found += 1
            consecutive_misses = 0
        else:
            consecutive_misses += 1

        current += 1

        if current % 100 == 0:
            log.info(
                f"  DataSet {dataset_num}: scanned to EFTA{current:08d}  "
                f"(found={found}, skipped={skipped}, misses={consecutive_misses})"
            )

    log.info(
        f"  DataSet {dataset_num} done — {found} files "
        f"({skipped} already existed, stopped at EFTA{current - 1:08d})"
    )
    return found, skipped


# ──────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="Download DOJ Epstein files")
    parser.add_argument(
        "-o", "--output", default=DEFAULT_DOWNLOAD_DIR,
        help=f"Download directory (default: {DEFAULT_DOWNLOAD_DIR})",
    )
    parser.add_argument(
        "-m", "--max-misses", type=int, default=50,
        help="Stop scanning a dataset after N consecutive misses (default: 50)",
    )
    parser.add_argument(
        "-d", "--delay", type=float, default=0.05,
        help="Delay in seconds between requests (default: 0.05)",
    )
    parser.add_argument(
        "--datasets", type=str, default=None,
        help="Comma-separated dataset numbers to process (e.g. 1,2,12). Default: all",
    )
    args = parser.parse_args()

    # Filter datasets if requested
    if args.datasets:
        selected = set(int(x.strip()) for x in args.datasets.split(","))
        datasets = [(ds, s) for ds, s in DATASETS if ds in selected]
    else:
        datasets = DATASETS

    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                      "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    })

    download_dir = args.output
    os.makedirs(download_dir, exist_ok=True)

    # Build end limits: next dataset's start - 1, or ABSOLUTE_END
    sorted_ds = sorted(DATASETS, key=lambda x: x[1])
    end_limits = {}
    for i, (ds, start) in enumerate(sorted_ds):
        if i + 1 < len(sorted_ds):
            end_limits[ds] = sorted_ds[i + 1][1] - 1
        else:
            end_limits[ds] = ABSOLUTE_END

    # ── Process each dataset ──
    total_found = 0
    total_skipped = 0

    log.info("=" * 60)
    log.info("DOJ Epstein Files Downloader")
    log.info(f"Output: {download_dir}")
    log.info(f"Max consecutive misses: {args.max_misses}")
    log.info(f"Delay: {args.delay}s")
    log.info("=" * 60)

    for ds_num, start in datasets:
        end = end_limits[ds_num]
        range_size = end - start + 1
        log.info(
            f"\nDataSet {ds_num}: EFTA{start:08d} → EFTA{end:08d} "
            f"(range: {range_size:,})"
        )
        found, skipped = process_dataset(
            session, ds_num, start, end, args.max_misses, args.delay, download_dir
        )
        total_found += found
        total_skipped += skipped

    log.info("\n" + "=" * 60)
    log.info("COMPLETE")
    log.info(f"  Total files  : {total_found}")
    log.info(f"  New downloads: {total_found - total_skipped}")
    log.info(f"  Already had  : {total_skipped}")
    log.info("=" * 60)


if __name__ == "__main__":
    main()
