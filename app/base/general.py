#  MIT License
#
#  Copyright (c) 2020 Daniel C. Brotsky
#
#  Permission is hereby granted, free of charge, to any person obtaining a copy
#  of this software and associated documentation files (the "Software"), to deal
#  in the Software without restriction, including without limitation the rights
#  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
#  copies of the Software, and to permit persons to whom the Software is
#  furnished to do so, subject to the following conditions:
#
#  The above copyright notice and this permission notice shall be included in all
#  copies or substantial portions of the Software.
#
#  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
#  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
#  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
#  AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
#  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
#  OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
#  SOFTWARE.

import os.path

#  MIT License
#
#
#  Permission is hereby granted, free of charge, to any person obtaining a copy
#  of this software and associated documentation files (the "Software"), to deal
#  in the Software without restriction, including without limitation the rights
#  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
#  copies of the Software, and to permit persons to whom the Software is
#  furnished to do so, subject to the following conditions:
#
#  The above copyright notice and this permission notice shall be included in all
#  copies or substantial portions of the Software.
#
#  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
#  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
#  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
#  AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
#  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
#  OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
#  SOFTWARE.
import sys
from datetime import datetime
from enum import Enum
from typing import Optional

Environment = Enum("Environment", "DEV STAGE PROD")


def lookup_env(name: str) -> Environment:
    if name in ("DEV", "DEVELOPMENT", "QA", "TEST"):
        return Environment.DEV
    elif name in ("STAGE", "STAGING"):
        return Environment.STAGE
    else:
        return Environment.PROD


def env() -> Environment:
    """Return the current process environment"""
    return lookup_env(os.getenv("ENVIRONMENT"))


def prinl(*args, **kwargs):
    """
    Like print but adds the process id to the front of the line,
    so when you are reading Heroku logs you can tell which
    process is actually logging.

    Also does an unbuffered print by default.
    """
    print(f"[{os.getpid()}]", *args, flush=True, **kwargs)


def prinlv(*args, **kwargs):
    """
    Like `prinl` but for more verbose logging.
    """
    spec = os.getenv("VERBOSE_LOGGING", "")
    if spec and spec != "0":
        prinl(*args, **kwargs)


def log_error(context: str) -> str:
    """Log a message about an exception, and return the message"""
    exc_type, exc_obj, exc_tb = sys.exc_info()
    f_name = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
    message = f"{context}: " f"{f_name}, {exc_tb.tb_lineno}: {repr(exc_obj)}"
    prinl(message, file=sys.stderr)
    return message


class Timestamp:
    """
    A Timestamp is a unique ID string based on a datetime (default now)
    that can be queried for its age.  It's guaranteed not to have
    colons in it so that it can be concatenated via colon.
    """

    def __init__(self, time: Optional[datetime] = None):
        """
        Return a date-based, monotonically-increasing ID that's
        reasonably likely to be unique for this computer and
        that doesn't have any colons in it.
        """
        if not isinstance(time, datetime):
            self.val = datetime.now()
        else:
            self.val = time
        id_base = self.val.strftime("%Y%m%d.%H%M%S.%f")
        self.id = f"{id_base}.{os.getpid()}"

    def __str__(self):
        return self.id

    def __repr__(self):
        def fullname():
            """code from https://stackoverflow.com/a/13653312/558006"""
            module = self.__class__.__module__
            if module is None or module == str.__class__.__module__:
                return self.__class__.__name__
            return module + "." + self.__class__.__name__

        return f"{fullname()}({repr(self.val)})"

    def age_in_minutes(self) -> float:
        age = datetime.now() - self.val
        return age.total_seconds() / 60.0

    @classmethod
    def from_string(cls, val: str):
        parts = val.split(".")
        if len(parts) != 4:
            raise ValueError("Not a valid timestamp: {val}")
        date, time, msecs, _ = parts
        if len(date) != 8 or len(time) != 6 or len(msecs) != 6:
            raise ValueError("Not a valid timestamp: {val}")
        time = datetime(
            int(date[0:4]),
            int(date[4:6]),
            int(date[6:8]),
            int(time[0:2]),
            int(time[2:4]),
            int(time[4:6]),
            int(msecs),
        )
        self = cls(time)
        return self
