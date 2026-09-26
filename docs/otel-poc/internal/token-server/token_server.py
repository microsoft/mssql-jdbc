#!/usr/bin/env python3
"""Sandbox App-Service-style token broker, adapted from fork 9d46cd4 (MIT).

Delegates an existing CLI identity, NOT managed identity. Never logs requests,
headers, CLI diagnostics or token responses. No login and no startup cloud probe.
"""

import hmac
import json
import os
import subprocess
import time
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import parse_qs, urlsplit


ALLOWED_RESOURCES = frozenset((
    "https://storage.azure.com/",
    "https://eventhubs.azure.net/",
    "https://management.azure.com/",
))
DEFAULT_RESOURCES = "https://storage.azure.com/,https://eventhubs.azure.net/"
TOKEN_PATH = "/metadata/identity/oauth2/token"


class TokenBroker:
    def __init__(self, secret, resources):
        selected = frozenset(resources.split(","))
        if (len(secret) < 32 or len(secret) > 256 or not secret.isascii()
                or any(ord(char) < 33 or ord(char) > 126 for char in secret)
                or not selected or not selected.issubset(ALLOWED_RESOURCES)):
            raise ValueError("Invalid broker configuration")
        self.secret = secret.encode("ascii")
        self.resources = selected
        self.cache = {}  # At most three tokens, memory-only; HTTPServer is serial.
        self.retry_after = {}

    def authorized(self, header):
        return hmac.compare_digest(header.encode("utf-8"), self.secret)

    def fetch(self, resource):
        if resource not in self.resources:
            raise ValueError("Resource not allowed")
        now = int(time.time())
        cached = self.cache.get(resource)
        if cached and int(cached["expires_on"]) > now + 120:
            return dict(cached, expires_in=str(int(cached["expires_on"]) - now))
        if self.retry_after.get(resource, 0) > now:
            raise RuntimeError("Credential unavailable")
        # Bound attempts even when several SDK clients retry a failed refresh.
        self.retry_after[resource] = now + 30
        proc = subprocess.run(
            ["az", "account", "get-access-token", "--resource", resource,
             "--output", "json", "--only-show-errors"],
            stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True,
            check=False, timeout=20,
        )
        if proc.returncode != 0:
            raise RuntimeError("Credential unavailable")
        payload = json.loads(proc.stdout)
        # Modern CLI provides UTC POSIX expires_on; avoid ambiguous local expiresOn.
        expiry = int(payload["expires_on"])
        token = payload["accessToken"]
        if not isinstance(token, str) or not token or expiry <= int(time.time()) + 120:
            raise RuntimeError("Credential unavailable")
        result = {"access_token": token, "token_type": "Bearer", "resource": resource,
                  "expires_on": str(expiry), "expires_in": str(expiry - int(time.time()))}
        self.cache[resource] = result
        return result


class TokenHandler(BaseHTTPRequestHandler):
    server_version = "SandboxBroker"
    sys_version = ""

    def log_message(self, *args):
        pass  # Base-class access/error logs can contain untrusted URLs.

    def send_error(self, code, message=None, explain=None):
        self.write_json(code, {"error": "Request rejected"})

    def write_json(self, status, body):
        encoded = json.dumps(body).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(encoded)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(encoded)

    def do_GET(self):
        if self.path == "/healthz":
            self.write_json(200, {"status": "ready"})
            return  # Liveness only; never mint a token for health checks.
        broker = self.server.broker
        headers = self.headers.get_all("X-IDENTITY-HEADER", [])
        if len(headers) != 1 or not broker.authorized(headers[0]):
            self.write_json(401, {"error": "Unauthorized"})
            return
        try:
            parsed = urlsplit(self.path)
            if len(self.path) > 2048 or parsed.path != TOKEN_PATH or parsed.scheme or parsed.netloc:
                raise ValueError()
            params = parse_qs(parsed.query, strict_parsing=True, max_num_fields=4)
            # Reject caller-supplied identity selectors; the broker delegates one CLI identity.
            if set(params) - {"resource", "api-version"} or len(params.get("resource", [])) != 1:
                raise ValueError()
            resource = params["resource"][0]
            if resource not in broker.resources:
                self.write_json(403, {"error": "Resource not allowed"})
                return
        except ValueError:
            self.write_json(400, {"error": "Invalid request"})
            return
        try:
            result = broker.fetch(resource)
        except Exception:
            # Fixed diagnostic; never include a CLI error, request value or exception text.
            self.write_json(503, {"error": "Credential unavailable"})
            return
        self.write_json(200, result)


class BrokerServer(HTTPServer):
    def get_request(self):
        connection, address = super().get_request()
        connection.settimeout(5)
        return connection, address

    def handle_error(self, request, client_address):
        pass  # Do not dump handler state or response exceptions.


def main():
    try:
        broker = TokenBroker(os.environ.get("IDENTITY_HEADER", ""),
                             os.environ.get("TOKEN_ALLOWED_RESOURCES", DEFAULT_RESOURCES))
    except ValueError:
        raise SystemExit("Invalid broker configuration") from None
    with BrokerServer(("0.0.0.0", 8080), TokenHandler) as server:
        server.broker = broker
        print("broker ready", flush=True)
        server.serve_forever()


if __name__ == "__main__":
    main()