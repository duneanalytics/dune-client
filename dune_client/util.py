"""Utility methods for package."""

import importlib
from datetime import UTC, datetime

DUNE_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"


def postgres_date(date_str: str) -> datetime:
    """Parse a postgres compatible date string into datetime object"""
    return datetime.strptime(date_str, DUNE_DATE_FORMAT)


def get_package_version(package_name: str) -> str | None:
    """
    Returns the package version by `package_name` using the importlib.metadata module
    which is available in Python 3.8 and later.
    """
    try:
        return importlib.metadata.version(package_name)
    except importlib.metadata.PackageNotFoundError:
        return None


def age_in_hours(timestamp: datetime) -> float:
    """
    Returns the time (in hours) between now and `timestamp`
    """
    result_age = datetime.now(UTC) - timestamp
    return result_age.total_seconds() / (60 * 60)
