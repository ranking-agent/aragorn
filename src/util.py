"""Common Aragorn Utilities."""
import datetime


def create_log_entry(msg: str, err_level, timestamp = datetime.datetime.now().isoformat(), code=None) -> dict:
    # load the data
    ret_val = {"timestamp": timestamp, "level": err_level, "message": msg, "code": code}

    # return to the caller
    return ret_val
