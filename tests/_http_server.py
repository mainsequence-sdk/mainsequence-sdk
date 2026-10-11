"""A local HTTP server for tests that must cross the real HTTP boundary.

Each route answers one ``(method, path)``. The server records every request it
receives, so a test can tell how often the client sent it. Routes stop writing
as soon as the server shuts down, so a test never waits for a slow route.
"""

from __future__ import annotations

import json
import threading
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


@contextmanager
def serve(routes):
    """Serve ``routes`` ({(method, path): route}) and yield (base URL, calls)."""
    calls = []
    stop = threading.Event()

    class Handler(BaseHTTPRequestHandler):
        def handle_request(self):
            length = int(self.headers.get("Content-Length") or 0)
            body = self.rfile.read(length) if length else b""
            calls.append((self.command, self.path, body))
            route = routes.get((self.command, self.path))
            try:
                if route is None:
                    self.send_error(404)
                else:
                    route(self, stop)
            except (BrokenPipeError, ConnectionResetError):
                pass

        do_GET = do_POST = handle_request

        def log_message(self, *args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.01})
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}/", calls
    finally:
        stop.set()
        server.shutdown()
        thread.join()
        server.server_close()


def answer_json(status, payload, *, content_type="application/json"):
    """Answer at once with ``payload`` as JSON."""
    data = json.dumps(payload).encode("utf-8")

    def route(handler, stop):
        handler.send_response(status)
        handler.send_header("Content-Type", content_type)
        handler.send_header("Content-Length", str(len(data)))
        handler.end_headers()
        handler.wfile.write(data)

    return route


def trickle_body(*, interval, size):
    """Send the headers at once, then one body byte every ``interval`` seconds."""

    def route(handler, stop):
        handler.send_response(200)
        handler.send_header("Content-Type", "application/json")
        handler.send_header("Content-Length", str(size))
        handler.end_headers()
        for _ in range(size):
            if stop.wait(interval):
                return
            handler.wfile.write(b" ")

    return route


def trickle_headers(*, interval):
    """Send even the status line and headers one byte every ``interval`` seconds."""
    raw = b"HTTP/1.0 200 OK\r\nContent-Type: application/json\r\nContent-Length: 2\r\n\r\n{}"

    def route(handler, stop):
        for byte in raw:
            if stop.wait(interval):
                return
            handler.wfile.write(bytes([byte]))

    return route


def answer_after(delay, status, payload):
    """Answer with ``payload`` after ``delay`` seconds of silence."""
    answer = answer_json(status, payload)

    def route(handler, stop):
        if stop.wait(delay):
            return
        answer(handler, stop)

    return route


def answer_sequence(*routes):
    """Answer with each route in turn, then keep answering with the last one."""
    pending = list(routes)

    def route(handler, stop):
        current = pending.pop(0) if len(pending) > 1 else pending[0]
        current(handler, stop)

    return route


def never_answer():
    """Read the request and never send anything back."""

    def route(handler, stop):
        stop.wait()

    return route
