"""Constant reads are kept for ten minutes, with cachetools 6 to 8.

cachetools 8 refuses ``cachedmethod`` on a class method, which the cache used before.
"""

import threading

import pytest

from mainsequence.client.models_foundry import Constant


@pytest.fixture
def api(monkeypatch):
    """Stand in for the API behind ``Constant.filter`` and ``Constant.get``.

    Each read records its arguments and answers a new object, so a test can
    tell an answer from the cache from a second one.
    """
    calls = []
    for name in ("filter", "get"):
        owner = next(base for base in Constant.__mro__[1:] if name in vars(base))

        def read(cls, _name=name, **kwargs):
            calls.append((_name, kwargs))
            return object()

        monkeypatch.setattr(owner, name, classmethod(read))
    Constant._filter_cache.clear()
    Constant._get_cache.clear()
    yield calls
    Constant._filter_cache.clear()
    Constant._get_cache.clear()


def test_a_constant_read_asks_the_api_once_for_the_same_arguments(api):
    listed = Constant.filter(name__in=["B", "A"])
    found = Constant.get(name="RATE")

    # The same arguments, in any order, answer from the cache.
    assert Constant.filter(name__in=["A", "B"]) is listed
    assert Constant.get(name="RATE") is found
    # Other arguments ask again.
    assert Constant.get(name="OTHER") is not found
    assert api == [
        ("filter", {"name__in": ["B", "A"]}),
        ("get", {"name": "RATE"}),
        ("get", {"name": "OTHER"}),
    ]


def test_invalidating_the_filter_cache_asks_the_api_again(api):
    listed = Constant.filter(category="CURVE")
    Constant.invalidate_filter_cache()

    assert Constant.filter(category="CURVE") is not listed
    assert [name for name, _ in api] == ["filter", "filter"]


def test_the_api_call_runs_outside_the_cache_lock(api, monkeypatch):
    """A slow read must not hold back reads for other arguments."""
    started, release = threading.Event(), threading.Event()
    owner = next(base for base in Constant.__mro__[1:] if "get" in vars(base))

    def slow(cls, **kwargs):
        if kwargs["name"] == "SLOW":
            started.set()
            release.wait(5)
        return object()

    monkeypatch.setattr(owner, "get", classmethod(slow))
    reader = threading.Thread(target=Constant.get, kwargs={"name": "SLOW"})
    reader.start()
    try:
        assert started.wait(5)
        other = threading.Thread(target=Constant.get, kwargs={"name": "FAST"})
        other.start()
        other.join(2)
        assert not other.is_alive()
    finally:
        release.set()
        reader.join(5)
