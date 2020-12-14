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

import pickle
from typing import Dict, List, Any

from fastapi import APIRouter
from pydantic import BaseModel
from starlette.responses import JSONResponse

from ..base import prinl, prinlv, log_error, env, Timestamp
from ..db import redis, ItemListStore as Store
from ..utils import ANHash

an = APIRouter()


class Submission(BaseModel):
    """The resource posted by an Action Network web hook"""

    form_name: str
    body: Any


class WebHookResponse(BaseModel):
    accepted: int


class ErrorResponse(BaseModel):
    detail: str


def database_error(context: str) -> JSONResponse:
    message = log_error(f"Database error: {context}")
    return JSONResponse(status_code=502, content={"detail": message})


def other_error(context: str) -> JSONResponse:
    message = log_error(f"Unexpected error: {context}")
    return JSONResponse(status_code=500, content={"detail": message})


@an.post(
    "/notification",
    status_code=200,
    response_model=WebHookResponse,
    responses={
        500: {
            "model": ErrorResponse,
            "description": "Unexpected error during processing",
        },
        502: {
            "model": ErrorResponse,
            "description": "Database error during processing",
        },
    },
    summary="Receiver for Action Network web hook messages.",
)
async def receive_notification(body: List[Dict]):
    """
    Receive a notification from an Action Network web hook.

    See https://actionnetwork.org/docs/webhooks for details.
    """
    prinlv(f"Received webhook with {len(body)} hash(es).")
    items = ANHash.find_items(data=body)
    if items:
        values = [pickle.dumps((item.form_name, item.body)) for item in items]
        list_key = f"{env().name}|{Timestamp()}:0"
        try:
            await redis.db.rpush(list_key, *values)
            await Store.add_new_list("webhook", list_key)
        except redis.Error:
            return database_error(f"while saving received items")
        except:
            return other_error(f"while saving received items")
    if len(items):
        prinlv(f"Accepted {len(items)} item(s) from webhook.")
    else:
        prinl(f"Warning: No valid items in webhook: {body}")
    return WebHookResponse(accepted=len(items))
