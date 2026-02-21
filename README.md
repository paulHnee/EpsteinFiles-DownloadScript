# DOJ Epstein Files Downloader

Python script to download publicly released documents from the [DOJ Epstein files](https://www.justice.gov/epstein/) across all 12 DataSets.

## Features

- Scans file numbers sequentially within each DataSet
- Detects file type via `Content-Type` header (PDF, MOV, PNG, JPEG, JPG)
- Downloads in a single pass (no separate existence check)
- Skips files already downloaded (resume support)
- Stops scanning a DataSet after N consecutive misses
- Configurable delay between requests

## Requirements

```
pip install requests
```

## Usage

```bash
# Download all datasets to E:\Epstein\
python script.py

# Download to a custom directory
python script.py -o "C:\Downloads\Epstein"

# Download only specific datasets
python script.py --datasets 1,2,12

# Increase miss threshold for more thorough scanning
python script.py --max-misses 200

# Adjust request delay (seconds)
python script.py --delay 0.1
```

## Options

| Flag | Default | Description |
|------|---------|-------------|
| `-o`, `--output` | `E:\Epstein` | Download directory |
| `-m`, `--max-misses` | `50` | Stop scanning after N consecutive 404s |
| `-d`, `--delay` | `0.05` | Delay between requests in seconds |
| `--datasets` | all | Comma-separated dataset numbers (e.g. `1,2,12`) |

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

## Output Structure

```
E:\Epstein\
├── DataSet 1\
│   ├── EFTA00000001.pdf
│   ├── EFTA00000002.jpg
│   └── ...
├── DataSet 2\
│   └── ...
└── DataSet 12\
    └── EFTA02731852.pdf
```
