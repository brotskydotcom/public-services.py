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
from typing import List

import pandas as pd
from fastapi import APIRouter
from fastapi import File, UploadFile, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from starlette.responses import JSONResponse

from ..db import redis, ItemListStore as Store
from ..utils import prinl, log_error, env, Timestamp
from ..workers import process_all_item_lists

mobilize = APIRouter()
templates = Jinja2Templates(directory="templates")


def database_error(context: str) -> JSONResponse:
    message = log_error(f"Database error: {context}")
    return JSONResponse(status_code=502, content={"detail": message})


@mobilize.post(
    "/transfer-event-csv",
    response_class=HTMLResponse,
    summary="Post a CSV file of events to process.",
)
async def transfer_event_csv(
    request: Request, file: UploadFile = File(...), force_transfer: bool = False
):
    """
    Given a CSV file of event information from Mobilize,
    validate that it's got the expected headers and then
    create a transfer item for each row in the CSV.
    """
    if file.filename.endswith(".csv"):
        try:
            df = pd.read_csv(file.file, na_filter=False)
        except:
            message = log_error("Error reading csv file")
            return templates.TemplateResponse(
                "upload_error.html", {"request": request, "msg": message}
            )
        df = df.astype(str)
        headings = df.columns.values.tolist()
        # fix #45: check that the spreadsheet has critical columns:
        for field in ("event_owner_email_address", "event_owner_email_address"):
            if field not in headings:
                message = f"Not a Mobilize event spreadsheet: missing field '{field}'"
                prinl(f"Rejecting CSV file '{file.filename}': {message}")
                return templates.TemplateResponse(
                    "upload_error.html", {"request": request, "msg": message}
                )
        count = await process_csv_rows(
            "event", headings, df.values.tolist(), force_transfer
        )
        if count < 0:
            message = log_error("Database error while saving received events")
            return templates.TemplateResponse(
                "upload_error.html", {"request": request, "msg": message}
            )
        return templates.TemplateResponse(
            "upload_success.html",
            {
                "request": request,
                "type": "events",
                "number": count,
                "filename": file.filename,
            },
        )
    else:
        return templates.TemplateResponse(
            "upload_error.html",
            {
                "request": request,
                "msg": f"File {file.filename} should be CSV file type",
            },
        )


@mobilize.post(
    "/transfer-shift-csv",
    response_class=HTMLResponse,
    summary="Post a CSV file of shifts to process.",
)
async def transfer_shift_csv(
    request: Request, file: UploadFile = File(...), force_transfer: bool = False
):
    """
    Given a CSV file of shift information from Mobilize,
    validate that it's got the expected headers and then
    create a transfer item for each row in the CSV.
    """
    if file.filename.endswith(".csv"):
        try:
            df = pd.read_csv(file.file, na_filter=False)
        except:
            message = log_error("Error reading csv file")
            return templates.TemplateResponse(
                "upload_error.html", {"request": request, "msg": message}
            )

        df = df.astype(str)
        headings = df.columns.values.tolist()
        # fix #45: check that the spreadsheet has critical columns:
        for field in ("email", "signup created time", "signup updated time"):
            if field not in headings:
                message = f"Not a Mobilize shift spreadsheet: missing field '{field}'"
                prinl(f"Rejecting CSV file '{file.filename}': {message}")
                return templates.TemplateResponse(
                    "upload_error.html", {"request": request, "msg": message}
                )
        count = await process_csv_rows(
            "shift", headings, df.values.tolist(), force_transfer
        )
        if count < 0:
            message = log_error("Database error while saving received items")
            return templates.TemplateResponse(
                "upload_error.html", {"request": request, "msg": message}
            )
        return templates.TemplateResponse(
            "upload_success.html",
            {
                "request": request,
                "type": "shifts",
                "number": count,
                "filename": file.filename,
            },
        )
    else:
        return templates.TemplateResponse(
            "upload_error.html",
            {
                "request": request,
                "msg": f"File {file.filename} should be CSV file type",
            },
        )


async def process_csv_rows(
    kind: str, headings: List, data: List[List], force_transfer: bool
) -> int:
    list_key = f"{env().name}:{Timestamp()}:0"
    items = [pickle.dumps((kind, dict(zip(headings, row)))) for row in data]
    try:
        await redis.db.rpush(list_key, *items)
        await Store.add_new_list(list_key)
    except redis.Error:
        return -1
    prinl(f"Accepted {len(items)} item(s) from Mobilize CSV.")
    if force_transfer:
        prinl(f"Running transfer task over received item(s).")
        asyncio.create_task(process_all_item_lists())
    return len(items)
