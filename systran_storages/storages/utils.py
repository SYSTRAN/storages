from datetime import datetime


def datetime_to_timestamp(date):
    if hasattr(date, "timestamp") and callable(date.timestamp):
        return date.timestamp()
    if date.tzinfo:
        epoch_date = datetime(1970, 1, 1, tzinfo=date.tzinfo)
    else:
        epoch_date = datetime(1970, 1, 1)
    timestamp = (date - epoch_date).total_seconds()
    return timestamp
