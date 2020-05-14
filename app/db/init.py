# Copyright (c) 2019 Daniel C. Brotsky.  All rights reserved.
import databases

from .model import db_get_url

# Initialize the database singleton used by all modules
database = databases.Database(db_get_url())
