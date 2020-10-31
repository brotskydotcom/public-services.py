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
import os
import pickle
from typing import List

import pandas as pd
from fastapi import APIRouter
from fastapi import File, UploadFile, Request, Form
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from ..base import prinl, log_error, env, Environment, Timestamp
from ..db import redis, ItemListStore as Store

mobilize = APIRouter()
templates = Jinja2Templates(directory="templates")

if env() == Environment.PROD:
    event_passwords = os.getenv("EVENT_PASSWORDS", "").split(":")
    shift_passwords = os.getenv("SHIFT_PASSWORDS", "").split(":")
else:
    event_passwords = os.getenv("EVENT_PASSWORDS", "events").split(":")
    shift_passwords = os.getenv("SHIFT_PASSWORDS", "shifts").split(":")


@mobilize.post(
    "/transfer-event-csv",
    response_class=HTMLResponse,
    summary="Post a CSV file of events to process.",
)
async def transfer_event_csv(
    request: Request, password: str = Form(":::"), file: UploadFile = File(...)
):
    """
    Given a CSV file of event information from Mobilize,
    post the file content for processing by a worker.
    """
    if password not in event_passwords:
        message = "Incorrect events password"
        return templates.TemplateResponse(
            "upload_error.html", {"request": request, "msg": message}
        )
    return await transfer_csv(request, "event", file)


@mobilize.post(
    "/transfer-shift-csv",
    response_class=HTMLResponse,
    summary="Post a CSV file of shifts to process.",
)
async def transfer_shift_csv(
    request: Request, password: str = Form(":::"), file: UploadFile = File(...)
):
    """
    Given a CSV file of shift information from Mobilize,
    post the file content for processing by a worker.
    """
    if password not in shift_passwords:
        message = "Incorrect shifts password"
        return templates.TemplateResponse(
            "upload_error.html", {"request": request, "msg": message}
        )
    return await transfer_csv(request, "shift", file)


async def transfer_csv(request: Request, file_type: str, file: UploadFile):
    """
    Given a CSV file of information from Mobilize,
    post the file content for processing by a worker.

    The file_type parameter specifies what kind of info is in the file.
    It should be the name of a MapContext, and be pluralized by adding 's'.
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
        rows = df.values.tolist()
        # fix #45: check that the spreadsheet has critical columns:
        columns = {
            "event": ("event_owner_email_address", "event_owner_email_address"),
            "shift": ("email", "signup created time", "signup updated time"),
        }
        for field in columns[file_type]:
            if field not in headings:
                message = (
                    f"Not a Mobilize {file_type}s spreadsheet: "
                    f"missing field '{field}'"
                )
                prinl(f"Rejecting CSV file '{file.filename}': {message}")
                return templates.TemplateResponse(
                    "upload_error.html", {"request": request, "msg": message}
                )
        if not await process_csv_rows(file_type, headings, rows):
            message = log_error("Database error while saving CSV file contents")
            return templates.TemplateResponse(
                "upload_error.html", {"request": request, "msg": message}
            )
        return templates.TemplateResponse(
            "upload_success.html",
            {
                "request": request,
                "type": f"{file_type}s",
                "number": len(rows),
                "filename": file.filename,
            },
        )
    else:
        return templates.TemplateResponse(
            "upload_error.html",
            {
                "request": request,
                "msg": f"File {file.filename} should be a CSV file",
            },
        )


async def process_csv_rows(kind: str, headings: List, data: List[List]) -> bool:
    list_key = f"{env().name}|{Timestamp()}:{kind}:0"
    item = pickle.dumps((headings, data))
    try:
        await redis.db.rpush(list_key, item)
        await Store.add_new_list("csv", list_key)
    except redis.Error:
        return False
    prinl(f"Accepted Mobilize CSV file")
    return True
