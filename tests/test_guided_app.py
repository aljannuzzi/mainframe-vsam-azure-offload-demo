import threading
import time
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import Mock

import pytest
from azure.core.exceptions import AzureError
from fastapi.testclient import TestClient

from vsam_offload import guided_app as module

TOKEN = "test-secret-ñ"
RUN = "12345678-1234-4234-8234-123456789abc"
HEADERS = {"X-Demo-Request": "1", "Origin": "https://demo.example"}
PATHS = [
    ("GET", "/api/config"), ("GET", "/api/runs"), ("POST", "/api/runs"),
    ("GET", f"/api/runs/{RUN}"), ("POST", f"/api/runs/{RUN}/steps/parse"),
    ("POST", f"/api/runs/{RUN}/change"), ("GET", f"/api/runs/{RUN}/artifacts/source"),
    ("POST", "/api/logout"), ("GET", "/api/unknown"), ("OPTIONS", "/api/config"),
]


@pytest.fixture
def service():
    result = Mock()
    result.config.return_value = {"mode": "azure"}
    result.list_runs.return_value = []
    result.create_run.return_value = {"id": RUN}
    result.get_run.return_value = {"id": RUN}
    result.step.return_value = {"step": "parse"}
    result.change.return_value = {"changed": True}
    result.artifact.return_value = (b"record", "application/octet-stream", "source.bin")
    return result


@pytest.fixture
def client(service):
    with TestClient(module.create_app(service, TOKEN), base_url="https://demo.example") as client:
        yield client


def login(client, token=TOKEN, headers=HEADERS):
    return client.post("/api/session", json={"token": token}, headers=headers)


@pytest.mark.parametrize("method,path", PATHS)
def test_all_api_requires_auth(client, service, method, path):
    response = client.request(method, path, headers=HEADERS)
    assert response.status_code == 401
    assert response.headers["cache-control"] == "no-store"
    assert not service.mock_calls


def test_login_and_logout(client):
    assert login(client, "wrong").status_code == 401
    assert login(client).status_code == 200
    assert client.get("/api/config").json() == {"mode": "azure"}
    assert client.post("/api/logout", headers=HEADERS).status_code == 200
    assert client.get("/api/config").status_code == 401


@pytest.mark.parametrize("payload", [{}, {"token": 1}, {"token": [TOKEN]}, {"token": TOKEN * 1000}])
def test_login_validation_does_not_echo_secrets(client, payload):
    response = client.post("/api/session", json=payload, headers=HEADERS)
    assert response.status_code == 422
    assert TOKEN not in response.text


@pytest.mark.parametrize("path", ["/api/session", "/api/runs", f"/api/runs/{RUN}/change"])
@pytest.mark.parametrize("headers", [
    {}, {"X-Demo-Request": "0"}, {**HEADERS, "Origin": "https://evil.example"},
    {**HEADERS, "Origin": "null"}, {**HEADERS, "Origin": "https://demo.example/path"},
    {**HEADERS, "Origin": "http://demo.example"}, {**HEADERS, "Origin": "https://demo.example:444"},
    {**HEADERS, "Origin": "https://user@demo.example"}, {**HEADERS, "Origin": "https://demo.example?"},
    {**HEADERS, "Origin": "https://demo.example:bad"},
    {**HEADERS, "Origin": "https://demo.example:"},
])
def test_mutations_reject_bad_headers(client, service, path, headers):
    login(client)
    assert client.post(path, json={"token": TOKEN}, headers=headers).status_code == 403
    assert not service.mock_calls


@pytest.mark.parametrize("headers", [{"X-Demo-Request": "1"}, HEADERS,
                                     {**HEADERS, "Origin": "https://demo.example:443"}])
def test_mutations_accept_same_origin_or_absent_origin(client, headers):
    assert login(client, headers=headers).status_code == 200
    assert client.post("/api/runs", headers=headers).status_code == 200


@pytest.mark.parametrize("url,secure", [
    ("https://demo.example", True), ("http://demo.example", True),
    ("http://localhost", False), ("http://127.0.0.1:8080", False),
    ("https://localhost", True), ("http://localhost.evil.example", True),
])
def test_cookie_flags(service, url, secure):
    with TestClient(module.create_app(service, TOKEN), base_url=url) as client:
        response = login(client, headers={"X-Demo-Request": "1", "X-Forwarded-Proto": "http",
                                         "X-Forwarded-Host": "localhost"})
        cookie = response.headers["set-cookie"].lower()
        assert ("; secure" in cookie) is secure
        assert "httponly" in cookie and "samesite=strict" in cookie and "path=/" in cookie
        assert f"max-age={module.SESSION_SECONDS}" in cookie


@pytest.mark.parametrize("case", ["tamper", "expired", "future", "too-long", "malformed"])
def test_cookie_rejects_invalid_or_expired_session(client, monkeypatch, case):
    monkeypatch.setattr(module.time, "time", lambda: 100_000)
    login(client)
    cookie = client.cookies.get(module.COOKIE)
    if case == "tamper":
        cookie = cookie[:-1] + ("0" if cookie[-1] != "0" else "1")
    elif case == "expired":
        monkeypatch.setattr(module.time, "time", lambda: 100_000 + module.SESSION_SECONDS)
    elif case == "future":
        monkeypatch.setattr(module.time, "time", lambda: 99_999)
    elif case == "too-long":
        payload = f"100000.{100_000 + module.SESSION_SECONDS + 1}"
        cookie = f"{payload}.{module._signature(TOKEN, payload)}"
    else:
        cookie = "not.a.session"
    client.cookies.clear()
    assert client.get("/api/config", headers={"Cookie": f"{module.COOKIE}={cookie}"}).status_code == 401


def test_routes_and_downloads(client, service):
    login(client)
    assert client.get("/api/runs").json() == []
    assert client.post("/api/runs", headers=HEADERS).json() == {"id": RUN}
    assert client.get(f"/api/runs/{RUN}").json() == {"id": RUN}
    assert client.post(f"/api/runs/{RUN}/steps/parse", headers=HEADERS).json() == {"step": "parse"}
    assert client.post(f"/api/runs/{RUN}/change", headers=HEADERS).json() == {"changed": True}
    service.get_run.assert_called_once_with(RUN)
    service.step.assert_called_once_with(RUN, "parse")
    service.change.assert_called_once_with(RUN)
    for kind in module.KINDS:
        response = client.get(f"/api/runs/{RUN}/artifacts/{kind}")
        assert response.content == b"record"
        assert response.headers["content-type"] == "application/octet-stream"
        assert response.headers["content-disposition"] == "attachment; filename*=UTF-8''source.bin"
        assert response.headers["cache-control"] == "no-store"
    assert client.get(f"/api/runs/{RUN}/artifacts/secrets").status_code == 404
    assert client.post(f"/api/runs/{RUN}/replay", headers=HEADERS).status_code == 404


@pytest.mark.parametrize("run_id", ["invalid", RUN.upper(), RUN.replace("-", ""), "{" + RUN + "}"])
def test_noncanonical_run_ids(client, service, run_id):
    login(client)
    assert client.get(f"/api/runs/{run_id}").status_code == 400
    assert not service.mock_calls


def test_missing_token_and_environment_configuration(service, monkeypatch):
    monkeypatch.delenv("DEMO_ACCESS_TOKEN", raising=False)
    with TestClient(module.create_app(service), base_url="https://demo.example") as client:
        assert login(client).status_code == 503
        assert client.get("/api/config").status_code == 503
        monkeypatch.setenv("DEMO_ACCESS_TOKEN", TOKEN)
        assert login(client).status_code == 200
        assert client.get("/api/config").status_code == 200
        monkeypatch.setenv("DEMO_ACCESS_TOKEN", "rotated")
        assert client.get("/api/config").status_code == 401


@pytest.mark.parametrize("error_type,status", [
    (module.GuidedError, 409), (module.ConfigurationError, 503),
    (AzureError, 502), (RuntimeError, 500),
])
def test_errors_are_sanitized(client, service, caplog, error_type, status):
    # The transport contract specifies attributes, not an exception constructor.
    error = error_type.__new__(error_type)
    message = f"Operation failed {TOKEN} https://private.example/?sig=private\n"
    Exception.__init__(error, message)
    error.status, error.message = status, message
    service.config.side_effect = error
    login(client)
    response = client.get("/api/config")
    assert response.status_code == status
    assert "detail" in response.json()
    assert TOKEN not in response.text + caplog.text
    assert "private.example" not in response.text + caplog.text
    assert "private" not in response.text + caplog.text
    assert error_type.__name__ in caplog.text


def test_public_endpoints_and_disabled_docs(service, monkeypatch):
    cloud = Mock(side_effect=AssertionError("Must not initialize Azure"))
    monkeypatch.setattr(module, "AzureCloud", cloud)
    with TestClient(module.create_app(auth_key=TOKEN)) as client:
        assert client.get("/healthz").json() == {"status": "ok", "mode": "azure"}
        for path in ["/docs", "/redoc", "/openapi.json"]:
            assert client.get(path).status_code == 404
        assert client.get("/api/config").status_code == 401
    root = next(route.endpoint for route in module.app.routes if route.path == "/")
    assert root().path == module.Path(module.__file__).with_name("guided_ui.html")
    cloud.assert_not_called()


def test_lazy_initialization_is_threadsafe_and_off_event_loop(service, monkeypatch):
    threads = []

    def initialize():
        threads.append(threading.current_thread().name)
        time.sleep(0.02)
        return object()

    cloud = Mock(side_effect=initialize)
    pipeline = Mock(return_value=service)
    monkeypatch.setattr(module, "AzureCloud", cloud)
    monkeypatch.setattr(module, "GuidedPipeline", pipeline)
    app = module.create_app(auth_key=TOKEN)

    def request(_):
        with TestClient(app, base_url="https://demo.example") as client:
            assert login(client).status_code == 200
            assert client.get("/api/config").status_code == 200

    cloud.assert_not_called()
    with ThreadPoolExecutor(max_workers=4) as pool:
        list(pool.map(request, range(8)))
    cloud.assert_called_once()
    pipeline.assert_called_once()
    assert all("AnyIO worker thread" in name for name in threads)


def test_mounted_app_cannot_bypass_security(service):
    parent = module.FastAPI()
    parent.mount("/demo", module.create_app(service, TOKEN))
    with TestClient(parent, base_url="https://demo.example") as client:
        for method, path in PATHS:
            response = client.request(method, "/demo" + path, headers=HEADERS)
            assert response.status_code == 401
            assert response.headers["cache-control"] == "no-store"
        assert client.post("/demo/api/session", json={"token": TOKEN}).status_code == 403
        assert client.post("/demo/api/session", json={"token": TOKEN}, headers=HEADERS).status_code == 200
        assert client.post("/demo/api/runs").status_code == 403
        assert client.get("/demo/api/config").status_code == 200


def test_failed_initialization_never_falls_back(service, monkeypatch):
    cloud = Mock(side_effect=AzureError("secret Azure error"))
    pipeline = Mock(return_value=service)
    monkeypatch.setattr(module, "AzureCloud", cloud)
    monkeypatch.setattr(module, "GuidedPipeline", pipeline)
    with TestClient(module.create_app(auth_key=TOKEN), base_url="https://demo.example") as client:
        login(client)
        for _ in range(2):
            response = client.get("/api/config")
            assert response.status_code == 502
            assert "secret" not in response.text
        assert cloud.call_count == 2
        pipeline.assert_not_called()
        cloud.side_effect = None
        assert client.get("/api/config").status_code == 200
        pipeline.assert_called_once()
