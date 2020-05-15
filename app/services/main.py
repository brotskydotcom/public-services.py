# Copyright (c) 2019 Daniel C. Brotsky.  All rights reserved.
import os

from fastapi import FastAPI

from .an import an
from ..db import database

ENVIRONMENT = os.getenv('ENVIRONMENT', 'PROD')

# create the webapp
if ENVIRONMENT == 'DEV':
    app = FastAPI()
else:
    app = FastAPI(openapi_url=None, docs_url=None, redoc_url=None)

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
