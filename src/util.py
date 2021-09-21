from datetime import datetime

def create_log_entry(msg: str, err_level, code=None) -> dict:
    now = datetime.now()

    # load the data
    ret_val = {
        'time stamp': now.strftime("%m/%d/%Y-%H:%M:%S"),
        'level': err_level,
        'message': msg,
        'code': code
    }

    # return to the caller
    return ret_val