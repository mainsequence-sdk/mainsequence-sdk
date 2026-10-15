"""Team membership answers an application may reuse (ADR 0036, #225)."""

from __future__ import annotations

import threading
import time
from collections import OrderedDict

# At most this many answers per process, and at most this many teams in one
# call to the platform, which answers no more in one call.
MAX_CACHED_ANSWERS = 10_000
MAX_TEAMS_PER_CALL = 100

# The trusted release UID, Environment UID, person UID and team UID.
AnswerKey = tuple[str, str, str, str]


def _now() -> float:
    return time.monotonic()


class TeamMembershipCache:
    """Yes-or-no answers across requests, oldest dropped first.

    The key starts with the release and Environment the verifier trusts, so an
    answer obtained for one never answers the other.
    """

    def __init__(self, max_answers: int = MAX_CACHED_ANSWERS):
        self._answers: OrderedDict[AnswerKey, tuple[bool, float]] = OrderedDict()
        self._max_answers = max_answers
        self._lock = threading.Lock()

    def get(self, key: AnswerKey, *, max_age: float) -> bool | None:
        """The stored answer when it is younger than ``max_age`` seconds."""
        if max_age <= 0:
            return None
        with self._lock:
            entry = self._answers.get(key)
        if entry is None:
            return None
        answer, stored_at = entry
        return answer if _now() - stored_at < max_age else None

    def put(self, key: AnswerKey, answer: bool) -> None:
        with self._lock:
            self._answers[key] = (answer, _now())
            self._answers.move_to_end(key)
            while len(self._answers) > self._max_answers:
                self._answers.popitem(last=False)

    def __len__(self) -> int:
        with self._lock:
            return len(self._answers)
