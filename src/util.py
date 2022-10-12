import datetime


def create_log_entry(msg: str, err_level, code=None) -> dict:
    # load the data
    ret_val = {"timestamp": datetime.datetime.now().isoformat(), "level": err_level, "message": msg, "code": code}

    # return to the caller
    return ret_val
