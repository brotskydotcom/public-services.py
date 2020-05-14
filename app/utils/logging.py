import sys
import os.path


def log_error(context: str) -> str:
    """Log a message about an exception, and return the message"""
    exc_type, exc_obj, exc_tb = sys.exc_info()
    fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
    message = f"{context}: " \
              f"{exc_type}, {fname}, {exc_tb.tb_lineno}"
    print(message, file=sys.stderr)
    return message


