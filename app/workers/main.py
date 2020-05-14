# Copyright (c) 2019 Daniel C. Brotsky.  All rights reserved.

from .webhook_transfer import transfer_all_webhook_items
from ..db import database


async def app():
    await database.connect()
    await transfer_all_webhook_items()
    await database.disconnect()
