#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Export DuckDB data to PTrade Parquet format

Usage:
    poetry run python scripts/export_parquet.py
    poetry run python scripts/export_parquet.py --output /path/to/output
"""

import argparse
import logging
from pathlib import Path

from simtradedata.writers.duckdb_writer import DEFAULT_DB_PATH, DuckDBWriter

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def export_to_parquet(db_path: str, output_dir: str) -> None:
    """
    Export DuckDB to PTrade Parquet format

    Args:
        db_path: Path to DuckDB database
        output_dir: Output directory for Parquet files
    """
    print("=" * 70)
    print("SimTradeData Parquet Export")
    print("=" * 70)
    print(f"Source: {db_path}")
    print(f"Output: {output_dir}")
    print("=" * 70)

    db_file = Path(db_path)
    if not db_file.exists():
        print(f"\nError: Database not found: {db_path}")
        print("Run download_efficient.py first to download data.")
        return

    writer = DuckDBWriter(db_path=db_path)

    try:
        writer.export_to_parquet(output_dir)

        output_path = Path(output_dir)

        def count_files(subdir: str) -> int:
            path = output_path / subdir
            return len(list(path.glob("*.parquet"))) if path.exists() else 0

        def get_dir_size(subdir: str) -> float:
            path = output_path / subdir
            if not path.exists():
                return 0
            return sum(f.stat().st_size for f in path.glob("*.parquet")) / (1024 * 1024)

        print("\nExport Statistics:")
        print(f"  stocks/: {count_files('stocks')} files, {get_dir_size('stocks'):.1f} MB")
        print(f"  exrights/: {count_files('exrights')} files, {get_dir_size('exrights'):.1f} MB")
        print(
            f"  fundamentals/: {count_files('fundamentals')} files, "
            f"{get_dir_size('fundamentals'):.1f} MB"
        )
        print(
            f"  valuation/: {count_files('valuation')} files, "
            f"{get_dir_size('valuation'):.1f} MB"
        )
        print(f"  metadata/: {count_files('metadata')} files, {get_dir_size('metadata'):.1f} MB")

        adj_pre = output_path / "ptrade_adj_pre.parquet"
        adj_post = output_path / "ptrade_adj_post.parquet"
        if adj_pre.exists():
            print(f"  ptrade_adj_pre.parquet: {adj_pre.stat().st_size / (1024*1024):.1f} MB")
        if adj_post.exists():
            print(f"  ptrade_adj_post.parquet: {adj_post.stat().st_size / (1024*1024):.1f} MB")

        print("\nExport complete!")

    finally:
        writer.close()


def main():
    parser = argparse.ArgumentParser(
        description="Export DuckDB to PTrade Parquet format"
    )
    parser.add_argument(
        "--db",
        type=str,
        default=DEFAULT_DB_PATH,
        help=f"DuckDB database path (default: {DEFAULT_DB_PATH})",
    )
    parser.add_argument(
        "--output",
        type=str,
        default="data/parquet",
        help="Output directory for Parquet files (default: data/parquet)",
    )

    args = parser.parse_args()

    export_to_parquet(args.db, args.output)


if __name__ == "__main__":
    main()
