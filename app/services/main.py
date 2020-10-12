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

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

from .an import an
from .control import control
from .mobilize import mobilize
from ..base import env, Environment
from ..db import ItemListStore
from ..utils import MapContext
from ..workers import EmbeddedWorkers

if env() in (Environment.DEV, Environment.STAGE):
    app = FastAPI()
else:
    app = FastAPI(openapi_url=None, docs_url=None, redoc_url=None)

# mounts an "independent" app on the /docs path that handles static files
app.mount("/docs", StaticFiles(directory="docs"), name="docs")
# add the sub-APIs
app.include_router(an, prefix="/action_network", tags=["action_network"])
app.include_router(control, prefix="/control", tags=["control"])
app.include_router(mobilize, prefix="/mobilize", tags=["mobilize"])


@app.on_event("startup")
async def startup():
    MapContext.initialize()
    await ItemListStore.initialize()
    await EmbeddedWorkers.start()


@app.on_event("shutdown")
async def shutdown():
    await EmbeddedWorkers.stop()
    await ItemListStore.finalize()
    MapContext.finalize()
