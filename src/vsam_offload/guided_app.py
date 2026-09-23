"""Authenticated, Azure-only HTTP surface for the reference implementation."""

import hashlib
import hmac
import logging
import os
import re
import threading
import time
from pathlib import Path
from urllib.parse import quote, urlsplit
from uuid import UUID

from azure.core.exceptions import AzureError
from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import FileResponse, JSONResponse, Response
from pydantic import BaseModel, Field, StrictStr
from starlette._utils import get_route_path

from .guided_cloud import AzureCloud, ConfigurationError
from .guided_pipeline import GuidedError, GuidedPipeline

COOKIE = "guided_session"
SESSION_SECONDS = 8 * 60 * 60
KINDS = {"source", "landing", "parsed", "events", "checkpoints"}
logger = logging.getLogger(__name__)


class SessionInput(BaseModel):
    token: StrictStr = Field(max_length=4096)


def _origin(value):
    if not re.fullmatch(r"https?://[^/?#\s\\]+", value):
        raise ValueError("Invalid origin")
    parsed = urlsplit(value)
    if not parsed.hostname or parsed.username is not None or parsed.password is not None or parsed.netloc.endswith(":"):
        raise ValueError("Invalid origin")
    port = parsed.port if parsed.port is not None else (443 if parsed.scheme == "https" else 80)
    return parsed.scheme, parsed.hostname, port


def _signature(key, payload):
    return hmac.new(key.encode(), ("guided-session:" + payload).encode(), hashlib.sha256).hexdigest()


def _authenticated(cookie, key):
    if not cookie or len(cookie) > 160:
        return False
    try:
        issued, expires, signature = cookie.split(".")
        payload = f"{issued}.{expires}"
        if not hmac.compare_digest(_signature(key, payload).encode(), signature.encode()):
            return False
        start, end, now = int(issued), int(expires), int(time.time())
        return start <= now < end and 0 < end - start <= SESSION_SECONDS
    except (ValueError, TypeError, UnicodeError):
        return False


def _run_id(run_id: str):
    try:
        if str(UUID(run_id)) == run_id:
            return run_id
    except ValueError:
        pass
    raise HTTPException(400, "Invalid run id")


def create_app(service=None, auth_key=None):
    """Inject a pipeline and access token in tests; otherwise use real Azure lazily."""
    app = FastAPI(openapi_url=None, docs_url=None, redoc_url=None)
    lock = threading.Lock()

    def key():
        value = auth_key if auth_key is not None else os.environ.get("DEMO_ACCESS_TOKEN")
        return value if isinstance(value, str) and value else None

    def pipeline():
        nonlocal service
        with lock:
            if service is None:
                service = GuidedPipeline(AzureCloud())
        return service

    def failure(error):
        logger.error("Guided API failure: %s", type(error).__name__)
        status, detail = 500, "Internal server error"
        if isinstance(error, GuidedError):
            status = error.status if isinstance(error.status, int) and 400 <= error.status <= 599 else 500
            if status != 500 and isinstance(error.message, str):
                detail = re.sub(r"https?://\S+", "[redacted]", error.message)
                secret = key()
                if secret:
                    detail = detail.replace(secret, "[redacted]")
                detail = "".join(char for char in detail if char.isprintable())[:300]
        elif isinstance(error, AzureError):
            status, detail = 502, "Azure service unavailable"
        return JSONResponse({"detail": detail}, status_code=status)

    @app.exception_handler(GuidedError)
    @app.exception_handler(AzureError)
    async def service_error(request, error):
        return failure(error)

    @app.exception_handler(RequestValidationError)
    async def invalid_request(request, error):
        # FastAPI's default validation response can echo the submitted token.
        return JSONResponse({"detail": "Invalid request"}, status_code=422)

    @app.middleware("http")
    async def protect(request: Request, call_next):
        path = get_route_path(request.scope)
        is_api = path == "/api" or path.startswith("/api/")
        response = None
        if is_api:
            if request.method not in {"GET", "HEAD", "OPTIONS"}:
                valid = request.headers.get("x-demo-request") == "1"
                origins = request.headers.getlist("origin")
                if origins:
                    try:
                        valid = valid and len(origins) == 1 and _origin(origins[0]) == _origin(
                            f"{request.url.scheme}://{request.url.netloc}"
                        )
                    except ValueError:
                        valid = False
                if not valid:
                    response = JSONResponse({"detail": "Forbidden request"}, status_code=403)
            if response is None:
                secret = key()
                login = request.method == "POST" and path == "/api/session"
                if not secret:
                    response = JSONResponse({"detail": "Access token is not configured"}, status_code=503)
                elif not login and not _authenticated(request.cookies.get(COOKIE), secret):
                    response = JSONResponse({"detail": "Authentication required"}, status_code=401)
        if response is None:
            try:
                response = await call_next(request)
            except Exception as error:
                response = failure(error)
        if is_api:
            response.headers["Cache-Control"] = "no-store"
        return response

    @app.get("/")
    def index():
        return FileResponse(Path(__file__).with_name("guided_ui.html"))

    @app.get("/healthz")
    def health():
        return {"status": "ok", "mode": "azure"}

    @app.post("/api/session")
    def login(body: SessionInput, request: Request):
        secret = key()
        if not secret:
            raise HTTPException(503, "Access token is not configured")
        if not hmac.compare_digest(body.token.encode(), secret.encode()):
            raise HTTPException(401, "Invalid access token")
        issued = int(time.time())
        payload = f"{issued}.{issued + SESSION_SECONDS}"
        response = JSONResponse({"authenticated": True})
        local_http = request.url.scheme == "http" and request.url.hostname in {"localhost", "127.0.0.1"}
        response.set_cookie(
            COOKIE, f"{payload}.{_signature(secret, payload)}", max_age=SESSION_SECONDS,
            secure=not local_http, httponly=True, samesite="strict", path="/",
        )
        return response

    @app.post("/api/logout")
    def logout(request: Request):
        response = JSONResponse({"authenticated": False})
        local_http = request.url.scheme == "http" and request.url.hostname in {"localhost", "127.0.0.1"}
        response.delete_cookie(COOKIE, secure=not local_http, httponly=True, samesite="strict", path="/")
        return response

    @app.get("/api/config")
    def config(current=Depends(pipeline)):
        return current.config()

    @app.get("/api/runs")
    def runs(current=Depends(pipeline)):
        return current.list_runs()

    @app.post("/api/runs")
    def create_run(current=Depends(pipeline)):
        return current.create_run()

    @app.get("/api/runs/{run_id}")
    def get_run(run_id=Depends(_run_id), current=Depends(pipeline)):
        return current.get_run(run_id)

    @app.post("/api/runs/{run_id}/steps/{step}")
    def step(step: str, run_id=Depends(_run_id), current=Depends(pipeline)):
        return current.step(run_id, step)

    @app.post("/api/runs/{run_id}/change")
    def change(run_id=Depends(_run_id), current=Depends(pipeline)):
        return current.change(run_id)

    @app.get("/api/runs/{run_id}/artifacts/{kind}")
    def artifact(kind: str, run_id=Depends(_run_id), current=Depends(pipeline)):
        if kind not in KINDS:
            raise HTTPException(404, "Unknown artifact")
        content, media, filename = current.artifact(run_id, kind)
        return Response(content, media_type=media, headers={
            "Content-Disposition": f"attachment; filename*=UTF-8''{quote(filename, safe='')}",
            "X-Content-Type-Options": "nosniff",
        })

    return app


app = create_app()
