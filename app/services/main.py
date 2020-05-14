# Copyright (c) 2019 Daniel C. Brotsky.  All rights reserved.
from fastapi import FastAPI

from ..db import database
from .an import an

# create the webapp
app = FastAPI()

# add the sub-APIs
app.include_router(
    an,
    prefix="/action_network",
    tags=["action_network"]
)


@app.on_event('startup')
async def startup():
    await database.connect()


@app.on_event('shutdown')
async def shutdown():
    await database.disconnect()
