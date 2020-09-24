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
from enum import Enum
from typing import Optional

from fastapi import APIRouter
from pydantic.main import BaseModel
from starlette.responses import JSONResponse

from ..base import prinl, log_error
from ..db import redis, ItemListStore as Store

control = APIRouter()


class DeferralAction(str, Enum):
    reprocess = "reprocess"
    discard = "discard"


class DeferralResponse(BaseModel):
    deferred_count: int = 0
    restarted_count: int = 0
    discarded_count: int = 0


class DatabaseErrorResponse(BaseModel):
    detail: str


def database_error(context: str) -> JSONResponse:
    message = log_error(f"Database error: {context}")
    return JSONResponse(status_code=502, content={"detail": message})


@control.get(
    "/deferrals",
    status_code=200,
    response_model=DeferralResponse,
    responses={
        502: {
            "model": DatabaseErrorResponse,
            "description": "Database error during processing",
        }
    },
    summary="Manage deferred items lists.",
)
async def deferrals(action: Optional[DeferralAction] = None):
    """
    Report the count of deferred item lists, and restart their
    processing or discard them if requested.
    """
    try:
        if action == DeferralAction.reprocess:
            count = 0
            while key := await Store.select_for_undeferral():
                count += 1
                await Store.add_new_list(key)
                await Store.remove_deferred_list(key)
            prinl(f"Restarted {count} deferred item lists.")
            return DeferralResponse(restarted_count=count)
        elif action == DeferralAction.discard:
            count = 0
            while key := await Store.select_for_undeferral():
                count += 1
                await Store.remove_deferred_list(key)
                await redis.db.delete(key)
            prinl(f"Discarded {count} deferred item lists.")
            return DeferralResponse(discarded_count=count)
        else:
            count = await Store.get_deferred_count()
            prinl(f"There are {count} deferred item lists.")
            return DeferralResponse(deferred_count=count)
    except redis.Error:
        return database_error(f"while saving received items")
