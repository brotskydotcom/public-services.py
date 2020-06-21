# Copyright (c) 2019 Daniel C. Brotsky.  All rights reserved.
import sys

from app.workers.fetch_transfer import transfer_all_forms

if __name__ == "__main__":
    names = []
    for name in sys.argv[1:]:
        names.append(name)
    if names:
        transfer_all_forms(names)
    else:
        raise ValueError("No form names were specified as arguments")
