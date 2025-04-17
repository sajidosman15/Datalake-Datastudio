
import time
import structlog

from typing import Literal
from fastapi import FastAPI, Request
from asgi_correlation_id import CorrelationIdMiddleware
from asgi_correlation_id.middleware import is_valid_uuid4
from starlette.middleware.base import BaseHTTPMiddleware
from uuid import uuid4

from app.api.v1 import (
    data
)

app = FastAPI(title="Data Studio API", version="1.0.0")
logger = structlog.get_logger()

@app.middleware("http")
async def log_user_id(request: Request, call_next):
    user_id = request.headers.get("user-id", "unknown")
    logger.info(
        "Request started",
        url=str(request.url),
        user_id=user_id,
    )
    response = await call_next(request)
    logger.info(
        "Request completed",
        url=str(request.url),
        user_id=user_id,
    )
    return response

app.add_middleware(
    CorrelationIdMiddleware,
    header_name="X-Request-ID",
    update_request_header=True,
    generator=lambda: uuid4().hex,
    validator=is_valid_uuid4,
    transformer=lambda a: a,
)

class TimingMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        start_time = time.time()
        response = await call_next(request)
        process_time = time.time() - start_time
        response.headers["X-Process-Time"] = str(process_time)
        return response


app.add_middleware(TimingMiddleware)

app.include_router(data.router, prefix="/api/v1")

@app.get("/")
def read_root() -> Literal["Service is running..."]:
    return "Service is running..."
