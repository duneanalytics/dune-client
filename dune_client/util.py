"""Utility methods for package."""

from datetime import datetime, timezone
from typing import Optional

DUNE_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"


def postgres_date(date_str: str) -> datetime:
    """Parse a postgres compatible date string into datetime object"""
    return datetime.strptime(date_str, DUNE_DATE_FORMAT)


def get_package_version(package_name: str) -> Optional[str]:
    """
    Returns the package version by `package_name`
    """
    try:
        # pylint: disable=import-outside-toplevel
        import pkg_resources

        return pkg_resources.get_distribution(package_name).version
    except ModuleNotFoundError:
        # Python 3.12 does not have pkg_resources!
        return None
    except pkg_resources.DistributionNotFound:
        return None


def age_in_hours(timestamp: datetime) -> float:
    """
    Returns the time (in hours) between now and `timestamp`
    """
    result_age = datetime.now(timezone.utc) - timestamp
    return result_age.total_seconds() / (60 * 60)
