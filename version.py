"""
Auto-version from git history.
Format: YYYY.MM.DD.N  (N = commit count on that date)
Example: 2026.03.02.3 = 3rd commit on March 2, 2026

Falls back to date-only if git is unavailable (e.g. no .git directory).
"""

import subprocess
from datetime import datetime, timezone


def _get_version() -> str:
    """Read version from the latest git commit."""
    try:
        # Get date + count of commits on that date for HEAD
        log = subprocess.check_output(
            ["git", "log", "--format=%aI", "--date=short"],
            stderr=subprocess.DEVNULL,
            text=True,
        ).strip().splitlines()

        if not log:
            raise ValueError("empty log")

        # Latest commit date (author date in ISO format)
        latest_iso = log[0]
        latest_date = latest_iso[:10]  # "2026-03-02"
        y, m, d = latest_date.split("-")

        # Count how many commits share this date
        n = sum(1 for line in log if line[:10] == latest_date)

        return f"{y}.{m}.{d}.{n}"
    except Exception:
        # Fallback: today's date with .0
        now = datetime.now(timezone.utc)
        return now.strftime("%Y.%m.%d.0")


__version__ = _get_version()
