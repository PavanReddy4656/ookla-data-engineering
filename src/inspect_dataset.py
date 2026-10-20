"""Inspect the public Ookla fixed-broadband file before downloading it.

Running this script with no extra arguments only prints the source URL.
It does not contact the server, download the file, or read any records.

From the project folder in PowerShell, the three commands are:

    python src/inspect_dataset.py
    python src/inspect_dataset.py --check-size
    python src/inspect_dataset.py --download

--check-size asks the server how big the file is and does not save it.
--download saves the file only because you explicitly asked for that.
"""

import argparse
import urllib.request
from pathlib import Path


# The official Ookla Open Data file for fixed broadband performance, Q4 2024.
# This is only the address of the file. Storing it here does not download it.
OOKLA_FIXED_BROADBAND_Q4_2024_URL = (
    "https://ookla-open-data.s3.amazonaws.com/parquet/performance/"
    "type=fixed/year=2024/quarter=4/"
    "2024-10-01_performance_fixed_tiles.parquet"
)

# __file__ is this script. parent is src/, and parent.parent is the project folder.
# pathlib builds the save location in a way that works on Windows and PowerShell.
PROJECT_ROOT = Path(__file__).resolve().parent.parent
RAW_DATA_DIR = PROJECT_ROOT / "data" / "raw"
RAW_FILE_PATH = RAW_DATA_DIR / "2024-10-01_performance_fixed_tiles.parquet"


def explain_before_download() -> None:
    """Print the URL and why the file size must be checked before a download."""
    print("Ookla fixed broadband Parquet URL (Q4 2024):")
    print(OOKLA_FIXED_BROADBAND_Q4_2024_URL)
    print()
    print(
        "We need to check the remote file size before downloading. "
        "A size check is a small request that does not save the file. "
        "It tells us how large the download is before anything is written "
        f"to {RAW_FILE_PATH}."
    )
    print("This script does not read columns or process records.")


def fetch_remote_file_size(url: str) -> int:
    """Return the remote file size in bytes without downloading the file.

    A HEAD request asks for information about the file, not the file itself.
    Content-Length is the server's reported size in bytes.
    """
    request = urllib.request.Request(url, method="HEAD")
    with urllib.request.urlopen(request) as response:
        size_header = response.headers.get("Content-Length")

    if size_header is None:
        raise RuntimeError(
            "The server did not report Content-Length, so the file size is unknown."
        )

    return int(size_header)


def download_raw_file(url: str, destination: Path) -> None:
    """Download the public file to destination. Call this only when asked."""
    destination.parent.mkdir(parents=True, exist_ok=True)
    print(f"Downloading to {destination}")
    urllib.request.urlretrieve(url, destination)
    print("Download finished.")


def main() -> None:
    """Print the plan, then check size or download only if a flag was passed."""
    parser = argparse.ArgumentParser(
        description=(
            "Show the Ookla Q4 2024 fixed broadband file URL. "
            "Check its size or download it only when you pass a flag."
        )
    )
    parser.add_argument(
        "--check-size",
        action="store_true",
        help="Ask the server for the file size. This does not download the file.",
    )
    parser.add_argument(
        "--download",
        action="store_true",
        help="Download the file into data/raw. Does nothing unless you pass this flag.",
    )
    args = parser.parse_args()

    explain_before_download()

    if not args.check_size and not args.download:
        print()
        print("No download was requested. Nothing was saved.")
        return

    # A download must follow a size check, even when both flags are passed together.
    size_bytes = fetch_remote_file_size(OOKLA_FIXED_BROADBAND_Q4_2024_URL)
    size_mb = size_bytes / (1024 * 1024)
    print()
    print(f"Remote file size: {size_bytes} bytes ({size_mb:.1f} MB).")

    if args.download:
        print("Download was explicitly requested.")
        download_raw_file(OOKLA_FIXED_BROADBAND_Q4_2024_URL, RAW_FILE_PATH)
    else:
        print("Size check only. The file was not downloaded.")


# This guard runs main() only when you execute this file directly.
# Importing the file from another script will not print or download anything.
if __name__ == "__main__":
    main()
