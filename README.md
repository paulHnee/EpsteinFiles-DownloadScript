# DOJ Epstein Files Downloader

Python script to download publicly released documents from the [DOJ Epstein files](https://www.justice.gov/epstein/) across all 12 DataSets.

## Features

- Concurrent scanning & downloading (configurable thread pool)
- Retry with exponential backoff (3 retries on 429/5xx errors)
- Connection pool tuning for high throughput
- Content-Type detection and magic bytes validation
- Content-Length integrity check (catches truncated downloads)
- Age-verification cookie with automatic refresh (~50 min cycle)
- Progress bar via `tqdm` (optional)
- `manifest.csv` log of every downloaded file
- `--verify` mode to check existing files for corruption
- `--start-from` to resume scanning mid-dataset
- Skips files already on disk (resume support)

## Requirements

```
pip install requests tqdm
```

`tqdm` is optional — the script works without it but won't show progress bars.

## Usage

```bash
# Download all datasets
python script.py

# Download to a custom directory
python script.py -o "C:\Downloads\Epstein"

# Download only specific datasets
python script.py --datasets 1,2,12

# Use 5 concurrent threads with a 1s delay (be gentle on the server)
python script.py -w 5 -d 1.0

# Increase miss threshold for more thorough scanning
python script.py --max-misses 200

# Resume from a specific file number
python script.py --start-from 5000

# Verify existing files for corruption (no downloading)
python script.py --verify
```

## Options

| Flag | Default | Description |
|------|---------|-------------|
| `-o`, `--output` | `E:\Epstein` | Download directory |
| `-w`, `--workers` | `3` | Concurrent download threads |
| `-m`, `--max-misses` | `50` | Stop scanning after N consecutive 404s |
| `-d`, `--delay` | `0.5` | Min seconds between requests **globally** across all workers (e.g. `0.5` = 2 req/s) |
| `--datasets` | all | Comma-separated dataset numbers (e.g. `1,2,12`) |
| `--start-from` | — | Start scanning from this EFTA file number |
| `--verify` | — | Check existing files for corruption (no downloads) |

## DataSets

| DataSet | First File | URL Pattern |
|---------|-----------|-------------|
| 1 | EFTA00000001 | `DataSet 1/EFTA*.pdf` |
| 2 | EFTA00003159 | `DataSet 2/EFTA*.pdf` |
| 3 | EFTA00003380 | `DataSet 3/EFTA*.pdf` |
| 4 | EFTA00005705 | `DataSet 4/EFTA*.pdf` |
| 5 | EFTA00008409 | `DataSet 5/EFTA*.pdf` |
| 6 | EFTA00008585 | `DataSet 6/EFTA*.pdf` |
| 7 | EFTA00009016 | `DataSet 7/EFTA*.pdf` |
| 8 | EFTA00009676 | `DataSet 8/EFTA*.pdf` |
| 9 | EFTA00039025 | `DataSet 9/EFTA*.pdf` |
| 10 | EFTA01262782 | `DataSet 10/EFTA*.pdf` |
| 11 | EFTA02205655 | `DataSet 11/EFTA*.pdf` |
| 12 | EFTA02730265 | `DataSet 12/EFTA*.pdf` |

Last known file: `EFTA02731852` (DataSet 12)

## Storage Estimate

| Files | Avg. Size | Total Storage |
|-------|-----------|---------------|
| 2,700,000 | 400 KB | **~1.01 TB** |

Note: Actual sizes will vary — scanned PDFs and images may be significantly larger, text-heavy documents smaller. Plan for extra headroom.

## Output Structure

```
E:\Epstein\
├── DataSet 1\
│   ├── EFTA00000001.pdf
│   ├── EFTA00000002.jpg
│   └── ...
├── DataSet 2\
│   └── ...
├── DataSet 12\
│   └── EFTA02731852.pdf
└── manifest.csv
```

### manifest.csv

Every successful download is logged with timestamp, dataset, filename, size, and content type:

```csv
timestamp,dataset,filename,size_bytes,content_type,status
2026-02-21T14:30:00,1,EFTA00000001.pdf,373984,application/pdf,ok
```
