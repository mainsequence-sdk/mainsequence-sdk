import datetime

import pytz
import QuantLib as ql


def to_ql_date(dt: datetime.date) -> ql.Date:
    """
    Converts a Python datetime.date object to a QuantLib.Date object.

    Args:
        dt: The datetime.date object to convert.

    Returns:
        The corresponding QuantLib.Date object.
    """
    return ql.Date(dt.day, dt.month, dt.year)


def to_py_date(qld: ql.Date) -> datetime.date:
    """
    Converts a QuantLib.Date object to a Python datetime.date object.

    Args:
        qld: The QuantLib.Date object to convert.

    Returns:
        The corresponding datetime.date object.
    """
    return datetime.datetime(qld.year(), qld.month(), qld.dayOfMonth(), tzinfo=pytz.utc)
