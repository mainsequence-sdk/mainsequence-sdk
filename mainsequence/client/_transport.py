"""One bounded retry policy for SDK HTTP sessions."""

import contextvars
import math
import threading
import time
from contextlib import contextmanager, suppress
from contextvars import ContextVar

import requests
from requests.adapters import HTTPAdapter
from urllib3.exceptions import InvalidHeader
from urllib3.util import Retry, Timeout

DEFAULT_ALLOWED_METHODS = frozenset({"HEAD", "GET", "OPTIONS"})
DEFAULT_STATUS_FORCELIST = (429, 500, 502, 503, 504)
DEFAULT_TIMEOUT = (5.0, 120.0)


class RequestBudget:
    def __init__(self, timeout):
        if timeout is None:
            timeout = DEFAULT_TIMEOUT
        if isinstance(timeout, Timeout):
            connect, read = timeout.connect_timeout, timeout.read_timeout
            total = timeout.total
        elif isinstance(timeout, tuple):
            connect, read = timeout
            total = None
        else:
            connect = read = total = timeout
        self.connect = self._seconds(connect)
        self.read = self._seconds(read)
        self.deadline = time.monotonic() + self._seconds(
            self.connect + self.read if total is None else total
        )

    @staticmethod
    def _seconds(value):
        seconds = float(value)
        if not math.isfinite(seconds) or seconds <= 0:
            raise ValueError("Request timeouts must be positive, finite seconds.")
        return seconds

    @property
    def remaining(self):
        return self.deadline - time.monotonic()

    def timeout(self):
        remaining = self.remaining
        if remaining <= 0:
            raise requests.Timeout("The SDK request's total timeout budget is exhausted.")
        return Timeout(
            total=remaining,
            connect=min(self.connect, remaining),
            read=min(self.read, remaining),
        )


_ACTIVE_BUDGET: ContextVar[RequestBudget | None] = ContextVar("sdk_request_budget", default=None)


@contextmanager
def request_budget(timeout):
    budget = RequestBudget(timeout)
    parent = _ACTIVE_BUDGET.get()
    if parent is not None:
        budget.deadline = min(budget.deadline, parent.deadline)
    token = _ACTIVE_BUDGET.set(budget)
    try:
        yield budget
    finally:
        _ACTIVE_BUDGET.reset(token)


def remaining_timeout(timeout):
    budget = _ACTIVE_BUDGET.get()
    if budget is None:
        return timeout
    connect, read = timeout
    remaining = budget.timeout().total
    return Timeout(total=remaining, connect=min(connect, remaining), read=min(read, remaining))


def request_within_budget(method, url, *, timeout, **kwargs):
    """Send one request, never retried, whose whole exchange ends by one deadline.

    ``timeout`` has the shared transport's meaning: a number is one total budget
    in seconds, and a ``(connect, read)`` tuple keeps those phase limits with a
    total equal to their sum. An enclosing :func:`request_budget` can only
    shorten it. The deadline covers connecting, sending, the response headers
    and the whole body, so a server that keeps sending bytes slowly cannot
    extend it.

    A socket read that keeps receiving bytes never times out on its own, so the
    exchange runs on a helper thread and the caller stops waiting at the
    deadline. Then the response, when it has started, is shut down and
    ``requests.Timeout`` is raised. The request may already have reached the
    server.
    """
    with request_budget(timeout) as budget:
        attempt_timeout = budget.timeout()
        lock = threading.Lock()
        finished = threading.Event()
        state = {"abandoned": False, "response": None, "outcome": None}

        def exchange():
            response = None
            try:
                with requests.Session() as session:
                    response = session.request(
                        method, url, timeout=attempt_timeout, stream=True, **kwargs
                    )
                    with lock:
                        state["response"] = response
                        abandoned = state["abandoned"]
                    if not abandoned:
                        # Read the body here, so the caller's deadline bounds it too.
                        _ = response.content
                outcome = response
            except BaseException as exc:  # handed to the waiting caller
                outcome = exc
            with lock:
                state["outcome"] = outcome
                abandoned = state["abandoned"]
            if abandoned and response is not None:
                response.close()
            finished.set()

        threading.Thread(
            target=contextvars.copy_context().run,
            args=(exchange,),
            name="mainsequence-request-within-budget",
            daemon=True,
        ).start()
        try:
            finished.wait(budget.remaining)
        finally:
            with lock:
                outcome = state["outcome"]
                if outcome is None:
                    state["abandoned"] = True
                started = state["response"]
            if outcome is None and started is not None:
                # Wakes a read that is still waiting for bytes.
                with suppress(OSError, RuntimeError, ValueError):
                    started.raw.shutdown()
        if outcome is None:
            raise requests.Timeout(
                "The SDK request did not finish within its total timeout budget."
            )
        if isinstance(outcome, BaseException):
            raise outcome
        return outcome


class DeadlineHTTPAdapter(HTTPAdapter):
    def __init__(self, *, retries, backoff_factor):
        if not isinstance(retries, int) or retries < 0:
            raise ValueError("retries must be a non-negative integer.")
        if not math.isfinite(backoff_factor) or backoff_factor < 0:
            raise ValueError("backoff_factor must be non-negative and finite.")
        # urllib3 must never add a second retry loop or resend a write on connect failure.
        super().__init__(
            max_retries=Retry(
                total=0,
                connect=0,
                read=False,
                status=0,
                other=0,
                allowed_methods=DEFAULT_ALLOWED_METHODS,
            )
        )
        self.retries = retries
        self.backoff_factor = backoff_factor

    def send(self, request, stream=False, timeout=None, verify=True, cert=None, proxies=None):
        budget = RequestBudget(timeout)
        parent = _ACTIVE_BUDGET.get()
        if parent is not None:
            budget.deadline = min(budget.deadline, parent.deadline)
        retryable = request.method in DEFAULT_ALLOWED_METHODS
        for attempt in range(self.retries + 1):
            response = None
            error = None
            try:
                response = super().send(
                    request,
                    stream=stream,
                    timeout=budget.timeout(),
                    verify=verify,
                    cert=cert,
                    proxies=proxies,
                )
                if not stream:
                    # Read before deciding to retry, so body failures share the same budget.
                    _ = response.content
                if not retryable or response.status_code not in DEFAULT_STATUS_FORCELIST:
                    return response
            except requests.exceptions.SSLError:
                raise
            except (
                requests.ConnectionError,
                requests.Timeout,
                requests.exceptions.ChunkedEncodingError,
            ) as exc:
                error = exc

            delay = min(self.backoff_factor * 2**attempt, Retry.DEFAULT_BACKOFF_MAX)
            if error is None:
                try:
                    retry_after = self.max_retries.get_retry_after(response)
                except InvalidHeader:
                    retry_after = None
                if retry_after is not None:
                    delay = max(delay, retry_after)

            if not retryable or attempt == self.retries or delay >= budget.remaining:
                if error is not None:
                    if response is not None:
                        response.close()
                    raise error
                return response
            if response is not None:
                response.close()
            time.sleep(delay)

        raise AssertionError("Unreachable retry state")
