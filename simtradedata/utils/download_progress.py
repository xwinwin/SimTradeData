"""
Download progress tracker for resume capability

This module manages download state to enable resume from interruptions.
"""

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import List

logger = logging.getLogger(__name__)


class DownloadProgress:
    """
    Track download progress for resume capability

    Progress file format:
    {
        "session_id": "20250101_120000",
        "start_time": "2025-01-01 12:00:00",
        "last_update": "2025-01-01 12:30:00",
        "config": {
            "start_date": "2025-01-01",
            "end_date": "2025-01-10",
            "skip_fundamentals": false,
            "skip_metadata": false
        },
        "completed": ["000001.SZ", "000002.SZ", ...],
        "failed": {
            "000003.SZ": {"error": "timeout", "timestamp": "..."},
            ...
        },
        "partial": {
            "000004.SZ": {
                "market": true,
                "valuation": true,
                "fundamentals": false,
                "adjust_factor": true
            }
        },
        "stats": {
            "total": 5000,
            "completed": 150,
            "failed": 5,
            "remaining": 4845
        }
    }
    """

    def __init__(self, progress_file: str = "data/download_progress.json"):
        self.progress_file = Path(progress_file)
        self.session_id = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.data = self._load_or_create()

    def _load_or_create(self) -> dict:
        """Load existing progress or create new"""
        if self.progress_file.exists():
            try:
                with open(self.progress_file, 'r') as f:
                    data = json.load(f)
                    logger.info(f"Loaded progress from {self.progress_file}")
                    logger.info(f"  Previous session: {data.get('session_id')}")
                    logger.info(f"  Completed: {len(data.get('completed', []))}")
                    logger.info(f"  Failed: {len(data.get('failed', {}))}")
                    return data
            except Exception as e:
                logger.warning(f"Failed to load progress file: {e}, creating new")

        return {
            "session_id": self.session_id,
            "start_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "last_update": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "config": {},
            "completed": [],
            "failed": {},
            "partial": {},
            "stats": {
                "total": 0,
                "completed": 0,
                "failed": 0,
                "remaining": 0
            }
        }

    def save(self):
        """Save progress to file"""
        self.data["last_update"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # Create directory if needed
        self.progress_file.parent.mkdir(parents=True, exist_ok=True)

        # Atomic write
        temp_file = self.progress_file.with_suffix('.tmp')
        with open(temp_file, 'w') as f:
            json.dump(self.data, f, indent=2, ensure_ascii=False)
        temp_file.replace(self.progress_file)

        logger.debug(f"Saved progress to {self.progress_file}")

    def update_config(self, config: dict):
        """Update download configuration"""
        self.data["config"] = config
        self.save()

    def mark_completed(self, symbol: str):
        """Mark a stock as completed"""
        if symbol not in self.data["completed"]:
            self.data["completed"].append(symbol)

        # Remove from failed/partial if exists
        self.data["failed"].pop(symbol, None)
        self.data["partial"].pop(symbol, None)

        # Update stats
        self.data["stats"]["completed"] = len(self.data["completed"])
        self.data["stats"]["remaining"] = (
            self.data["stats"]["total"] -
            self.data["stats"]["completed"] -
            self.data["stats"]["failed"]
        )

        # Save every 10 completions to reduce I/O
        if len(self.data["completed"]) % 10 == 0:
            self.save()

    def mark_failed(self, symbol: str, error: str):
        """Mark a stock as failed"""
        self.data["failed"][symbol] = {
            "error": error,
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }

        # Update stats
        self.data["stats"]["failed"] = len(self.data["failed"])
        self.data["stats"]["remaining"] = (
            self.data["stats"]["total"] -
            self.data["stats"]["completed"] -
            self.data["stats"]["failed"]
        )

        self.save()

    def mark_partial(self, symbol: str, completion_status: dict):
        """
        Mark a stock as partially downloaded

        Args:
            symbol: Stock code
            completion_status: Dict like {"market": True, "fundamentals": False}
        """
        self.data["partial"][symbol] = completion_status
        self.save()

    def get_pending_stocks(self, all_stocks: List[str]) -> List[str]:
        """
        Get list of stocks that need to be downloaded

        Returns stocks in priority order:
        1. Previously failed stocks (for retry)
        2. Partially downloaded stocks
        3. New stocks not yet attempted
        """
        completed = set(self.data["completed"])

        # Priority 1: Failed stocks (retry)
        failed = list(self.data["failed"].keys())

        # Priority 2: Partial stocks
        partial = list(self.data["partial"].keys())

        # Priority 3: New stocks
        new = [s for s in all_stocks if s not in completed and s not in failed and s not in partial]

        pending = failed + partial + new

        logger.info(f"Pending stocks breakdown:")
        logger.info(f"  Failed (retry): {len(failed)}")
        logger.info(f"  Partial: {len(partial)}")
        logger.info(f"  New: {len(new)}")
        logger.info(f"  Total pending: {len(pending)}")

        return pending

    def set_total(self, total: int):
        """Set total number of stocks"""
        self.data["stats"]["total"] = total
        self.data["stats"]["remaining"] = (
            total -
            self.data["stats"]["completed"] -
            self.data["stats"]["failed"]
        )
        self.save()

    def get_completion_rate(self) -> float:
        """Get completion percentage"""
        total = self.data["stats"]["total"]
        if total == 0:
            return 0.0
        return (self.data["stats"]["completed"] / total) * 100

    def reset(self):
        """Reset progress (start fresh)"""
        self.data = {
            "session_id": self.session_id,
            "start_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "last_update": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "config": {},
            "completed": [],
            "failed": {},
            "partial": {},
            "stats": {
                "total": 0,
                "completed": 0,
                "failed": 0,
                "remaining": 0
            }
        }
        if self.progress_file.exists():
            self.progress_file.unlink()
        logger.info("Progress reset")

    def print_summary(self):
        """Print progress summary"""
        print("\n" + "=" * 70)
        print("Download Progress Summary")
        print("=" * 70)
        print(f"Session ID: {self.data['session_id']}")
        print(f"Started: {self.data['start_time']}")
        print(f"Last updated: {self.data['last_update']}")
        print(f"\nProgress:")
        print(f"  Total stocks: {self.data['stats']['total']}")
        print(f"  Completed: {self.data['stats']['completed']} ({self.get_completion_rate():.1f}%)")
        print(f"  Failed: {self.data['stats']['failed']}")
        print(f"  Remaining: {self.data['stats']['remaining']}")

        if self.data["failed"]:
            print(f"\nFailed stocks (first 10):")
            for symbol in list(self.data["failed"].keys())[:10]:
                error = self.data["failed"][symbol]["error"]
                print(f"  {symbol}: {error}")

        print("=" * 70 + "\n")
