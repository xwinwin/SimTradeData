#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Unified CLI for downloading PTrade-compatible HDF5 data files

This provides a simple interface to download data using BaoStock backend.
"""

import argparse
import sys
from pathlib import Path

# Add parent directory to path to import download scripts
sys.path.insert(0, str(Path(__file__).parent))

from download_fundamentals_hdf5 import download_all_data as download_fundamentals
from download_unified_hdf5 import download_all_data as download_unified


def main():
    """Main CLI entry point"""
    parser = argparse.ArgumentParser(
        description="Download PTrade-compatible HDF5 data using BaoStock",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Full download (all data types)
  python download.py all

  # Download only market data
  python download.py market

  # Download only fundamentals
  python download.py fundamentals

  # Incremental update (last 30 days)
  python download.py market --incremental 30
  python download.py fundamentals --incremental 30

  # Full incremental update
  python download.py all --incremental 30
        """,
    )

    parser.add_argument(
        "data_type",
        choices=["all", "market", "fundamentals"],
        help="Type of data to download",
    )

    parser.add_argument(
        "--incremental",
        type=int,
        metavar="DAYS",
        help="Incremental update: only update last N days for existing stocks",
    )

    args = parser.parse_args()

    print("=" * 70)
    print("PTrade-Compatible Data Downloader")
    print("Using BaoStock as data source")
    print("=" * 70)

    try:
        if args.data_type in ["market", "all"]:
            print(
                "\n[1/{}] Downloading market data...".format(
                    2 if args.data_type == "all" else 1
                )
            )
            download_unified(incremental_days=args.incremental)

        if args.data_type in ["fundamentals", "all"]:
            step = 2 if args.data_type == "all" else 1
            total = 2 if args.data_type == "all" else 1
            print("\n[{}/{}] Downloading fundamentals data...".format(step, total))
            download_fundamentals(incremental_days=args.incremental)

        print("\n" + "=" * 70)
        print("Download completed successfully!")
        print("=" * 70)

    except KeyboardInterrupt:
        print("\n\nInterrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n\nError: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
