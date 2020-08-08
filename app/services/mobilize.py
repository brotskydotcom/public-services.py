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

import asyncio
import pickle
import pandas as pd

from typing import Dict
from fastapi import APIRouter
from fastapi import FastAPI, File, UploadFile, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from starlette.responses import JSONResponse

from ..utils import prinl, log_error, MapContext, env, Timestamp
from ..db import redis, ItemListStore as Store
from ..workers import process_all_item_lists

mobilize = APIRouter()
templates = Jinja2Templates(directory="templates")


def database_error(context: str) -> JSONResponse:
    message = log_error(f"Database error: {context}")
    return JSONResponse(status_code=502, content={"detail": message})


@mobilize.post("/transfercsv", response_class=HTMLResponse)
async def transfer_csv(
    request: Request, file: UploadFile = File(...), force_transfer: bool = False
):
    if file.filename.endswith(".csv"):
        try:
            df = pd.read_csv(file.file, na_filter=False)
        except:
            message = log_error("Error reading csv file")
            return templates.TemplateResponse(
                "upload_error.html", {"request": request, "msg": message}
            )

        df = df.astype(str)
        data = df.values.tolist()
        headings = df.columns.values.tolist()
        list_key = f"{env().name}:{Timestamp()}:0"
        items = [pickle.dumps(("shift", dict(zip(headings, row)))) for row in data]
        try:
            await redis.db.rpush(list_key, *items)
            await Store.add_new_list(list_key)
        except redis.Error:
            message = log_error("Database error while saving received items")
            return templates.TemplateResponse(
                "upload_error.html", {"request": request, "msg": message,},
            )
        prinl(f"Accepted {len(items)} item(s) from Mobilize CSV.")
        if force_transfer:
            prinl(f"Running transfer task over received item(s).")
            asyncio.create_task(process_all_item_lists())
        return templates.TemplateResponse(
            "upload_success.html",
            {"request": request, "num_shifts": len(items), "filename": file.filename},
        )
    else:
        return templates.TemplateResponse(
            "upload_error.html",
            {
                "request": request,
                "msg": f"File {file.filename} should be CSV file type",
            },
        )
