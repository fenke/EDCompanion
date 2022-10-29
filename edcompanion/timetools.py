#pylint: disable=missing-module-docstring
#pylint: disable=missing-function-docstring
#pylint: disable=invalid-name

import datetime
import pytz
from dateutil import parser
from numpy import datetime64, timedelta64

def make_datetime(d):
    # use parser to convert from string representation
    if isinstance(d, str):
        return parser.parse(d)
    # ints and floats are asumed to be unix timestamps (seconds sinds 1/1/1970)
    elif isinstance(d, int):
        return datetime.datetime.fromtimestamp(d)
    elif isinstance(d, float):
        return datetime.datetime.fromtimestamp(d)
    # for other cases use the object's string cast and parse that
    else:
        return parser.parse(str(d))


def make_naive_utc(d):
    if d.tzinfo is None or d.tzinfo.utcoffset(d) is None:
        timezone = pytz.timezone("UTC")
        return timezone.localize(d)
    return d

# specializes to rounding down into seconds, minutes, hours and days
def make_datetime_sec(d):
    return make_datetime(d).replace(microsecond=0)

def make_datetime_min(d):
    return make_datetime(d).replace(microsecond=0, second=0)

def make_datetime_hour(d):
    return make_datetime(d).replace(microsecond=0, second=0, minute=0)

def make_datetime_day(d):
    return make_datetime(d).replace(microsecond=0, second=0, minute=0, hour=0)


def unix_time(some_time=None):
    """Convert to unix (epoch) timestamp"""
    if some_time is None:
        return datetime.datetime.utcnow().timestamp()
    elif isinstance(some_time, datetime64):
        return (
                    datetime64(some_time,"ms")
                        - datetime64('1970-01-01T00:00:00')
                ) / timedelta64(1, 's')
    elif isinstance(some_time, datetime.date):
        return some_time.timestamp()
    else:
        return make_datetime(some_time).timestamp()

def iso_datetime(timestamp):
    """Convert numpy datetime to ISO string"""
    return make_datetime(timestamp).isoformat()

def dt64(some_time, time_resolution="s"):
    """Convert unix epoch timestamp to numpy datetime64"""
    return datetime64(int(round(make_datetime(some_time).timestamp())), time_resolution)


