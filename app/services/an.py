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
import os
from typing import Dict, List, Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from starlette.responses import JSONResponse

from ..db import database
from ..db.model import an_form_items as ani
from ..utils import ANSubmission, log_error
from ..workers import transfer_all_webhook_items

ENVIRONMENT = os.getenv("ENVIRONMENT", "PROD")

an = APIRouter()


class Submission(BaseModel):
    """The resource posted by an Action Network webhook"""

    id: int
    form_name: str
    body: Any


class WebHookResponse(BaseModel):
    accepted: int


class DatabaseErrorResponse(BaseModel):
    detail: str


def database_error(context: str) -> JSONResponse:
    message = log_error(f"Database error: {context}")
    return JSONResponse(status_code=502, content={"detail": message})


@an.post(
    "/notification",
    status_code=200,
    response_model=WebHookResponse,
    responses={
        502: {
            "model": DatabaseErrorResponse,
            "description": "Database error during processing",
        }
    },
    summary="Receiver for new webhook messages. Valid hooks are "
    "processed immediately unless query parameter "
    "'delay_processing' is specified as 'true'",
)
async def receive_notification(body: List[Dict], delay_processing: bool = False):
    """
    Receive a notification from an Action Network webhook.

    See https://actionnetwork.org/docs/webhooks for details.
    """
    print(f"Received webhook with {len(body)} hash(es).")
    items = ANSubmission.find_items(data=body)
    if items:
        try:
            async with database.transaction():
                for item in items:
                    bi_query = ani.insert().values(
                        form_name=item.form_name, body=item.as_json()
                    )
                    await database.execute(bi_query)
        except:
            return database_error("While saving received items")
    print(f"Accepted {len(items)} item(s) from webhook.")
    if items and not delay_processing:
        print(f"Running worker task to transfer received item(s).")
        asyncio.create_task(transfer_all_webhook_items())
    return WebHookResponse(accepted=len(items))


@an.get(
    "/submissions",
    status_code=200,
    response_model=List[Submission],
    responses={
        502: {
            "model": DatabaseErrorResponse,
            "description": "Database error during processing",
        }
    },
    summary="Fetch items from prior posted webhooks",
)
async def get_item(start_id: int = 0, max_results: int = 100):
    """
    Return a block of notified items,
    starting at the next item after _start_id_
    and containing at most _max_results_ items.
    """
    if ENVIRONMENT != "DEV":
        raise HTTPException(403, detail="Access to production data is not allowed")
    query = (
        ani.select()
        .where(ani.c.id > start_id)
        .order_by(ani.c.id.asc())
        .limit(max_results)
    )
    results = []
    try:
        async for row in database.iterate(query):
            results.append(
                Submission(
                    id=row[ani.c.id],
                    form_name=row[ani.c.form_name],
                    body=row[ani.c.body],
                )
            )
    except:
        return database_error("While retrieving items")
    return results
