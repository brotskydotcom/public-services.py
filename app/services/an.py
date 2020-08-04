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
from typing import Dict, List, Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from starlette.responses import JSONResponse

from ..db import redis, ItemListStore as Store
from ..utils import ANHash, prinl, log_error, env, Timestamp, Environment
from ..workers import process_all_item_lists

an = APIRouter()


class Submission(BaseModel):
    """The resource posted by an Action Network web hook"""

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
    summary="Receiver for new webhook messages. "
    "Specify query parameter 'force_transfer' as 'true' "
    "to create a explicit task to transfer the webhook.",
)
async def receive_notification(body: List[Dict], force_transfer: bool = False):
    """
    Receive a notification from an Action Network web hook.

    See https://actionnetwork.org/docs/webhooks for details.
    """
    prinl(f"Received webhook with {len(body)} hash(es).")
    items = ANHash.find_items(data=body)
    if items:
        values = [pickle.dumps((item.form_name, item.body)) for item in items]
        list_key = f"{env().name}:{Timestamp()}:0"
        try:
            await redis.db.rpush(list_key, *values)
            await Store.add_new_list(list_key)
        except redis.Error:
            return database_error(f"while saving received items")
    prinl(f"Accepted {len(items)} item(s) from webhook.")
    if force_transfer:
        prinl(f"Running transfer task over received item(s).")
        asyncio.create_task(process_all_item_lists())
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
async def get_pending_items():
    """
    Return all the notified items that haven't yet been processed.
    """
    ani_key: str = redis.get_key("Submitted Items")
    if env() is Environment.PROD:
        raise HTTPException(403, detail="Access to production data is not allowed")
    prinl("Retrieving all pending submissions...")
    results = []
    try:
        submissions = await redis.db.lrange(ani_key, 0, -1, encoding="ascii")
        for submission in submissions:
            items = await redis.db.lrange(submission, 0, -1)
            for item in items:
                form_name, body = pickle.loads(item)
                results.append(Submission(form_name=form_name, body=body))
    except redis.Error:
        return database_error("while retrieving submissions")
    prinl(f"Returning {len(results)} pending submissions.")
    return results
