#!/usr/bin/env python3
"""Tiny App-Service-style IMDS token server backed by `az account get-access-token`.

Azure SDKs (both Go's `azidentity` and .NET's `ManagedIdentityCredential`)
auto-detect this environment when both ``IDENTITY_ENDPOINT`` and
``IDENTITY_HEADER`` are set: they POST/GET to the endpoint with the
``X-IDENTITY-HEADER`` header and a ``resource`` query parameter, expecting an
IMDS-format JSON response.

This server thinly wraps the local ``az`` CLI so the e2e harness can vend
real Entra tokens without exposing the host's full ``~/.azure`` directory to
every client container.
"""

from __future__ import annotations

import datetime
import json
import os
import subprocess
import sys
import urllib.parse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

IDENTITY_HEADER = os.environ.get("IDENTITY_HEADER", "")
LISTEN_HOST = os.environ.get("TOKEN_SERVER_HOST", "0.0.0.0")
LISTEN_PORT = int(os.environ.get("TOKEN_SERVER_PORT", "8080"))


def _now() -> str:
    return datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _log(msg: str) -> None:
    print(f"[{_now()}] {msg}", flush=True)


def _fetch_token(resource: str) -> dict:
    """Invoke ``az account get-access-token`` for the given resource."""
    proc = subprocess.run(
        ["az", "account", "get-access-token", "--resource", resource, "--output", "json"],
        capture_output=True,
        text=True,
        check=False,
    )
    if proc.returncode != 0:
        raise RuntimeError(f"az failed: {proc.stderr.strip() or proc.stdout.strip()}")

    payload = json.loads(proc.stdout)
    expires_on_str = payload.get("expiresOn")
    expires_on_int = 0
    if expires_on_str:
        try:
            expires_on_int = int(
                datetime.datetime.fromisoformat(
                    expires_on_str.replace("Z", "+00:00")
                ).timestamp()
            )
        except (TypeError, ValueError):
            expires_on_int = 0

    now_int = int(datetime.datetime.now(datetime.timezone.utc).timestamp())

    return {
        "access_token": payload.get("accessToken"),
        "token_type": "Bearer",
        "expires_on": str(expires_on_int),
        "expires_in": str(max(0, expires_on_int - now_int)),
        "resource": resource,
    }


class TokenHandler(BaseHTTPRequestHandler):
    def log_message(self, format: str, *args: object) -> None:  # noqa: A002
        _log(f"{self.address_string()} - {format % args}")

    def _write_json(self, status: int, body: dict) -> None:
        encoded = json.dumps(body).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)

    def _check_identity_header(self) -> bool:
        if not IDENTITY_HEADER:
            return True
        return self.headers.get("X-IDENTITY-HEADER") == IDENTITY_HEADER

    def do_GET(self) -> None:  # noqa: N802
        parsed = urllib.parse.urlparse(self.path)
        if parsed.path == "/healthz":
            self._write_json(200, {"status": "ok"})
            return

        if parsed.path not in (
            "/metadata/identity/oauth2/token",
            "/token",
            "/oauth2/token",
        ):
            self._write_json(404, {"error": f"unknown path: {parsed.path}"})
            return

        if not self._check_identity_header():
            self._write_json(
                401,
                {
                    "error": "Unauthorized",
                    "message": "missing or invalid X-IDENTITY-HEADER",
                },
            )
            return

        params = urllib.parse.parse_qs(parsed.query)
        resource_values = params.get("resource", [""])
        resource = resource_values[0]
        if not resource:
            self._write_json(400, {"error": "missing resource query parameter"})
            return

        try:
            token = _fetch_token(resource)
        except Exception as exc:  # noqa: BLE001
            _log(f"token fetch failed for {resource}: {exc}")
            self._write_json(500, {"error": str(exc)})
            return

        self._write_json(200, token)


def main() -> int:
    _log(
        f"starting token-server on {LISTEN_HOST}:{LISTEN_PORT} "
        f"(IDENTITY_HEADER {'set' if IDENTITY_HEADER else 'unset (no auth check)'})"
    )

    try:
        _fetch_token("https://management.azure.com/")
        _log("smoke test ok — az credential available")
    except Exception as exc:  # noqa: BLE001
        _log(f"WARNING: az smoke test failed: {exc}")

    server = ThreadingHTTPServer((LISTEN_HOST, LISTEN_PORT), TokenHandler)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        return 0
    finally:
        server.server_close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
