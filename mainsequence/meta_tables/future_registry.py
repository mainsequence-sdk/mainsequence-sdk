# future_registry.py
import threading

# Use a thread‑safe set to store pending futures
_pending_futures = set()
_registry_lock = threading.Lock()


def add_future(future):
    """Add a future to the global registry."""
    with _registry_lock:
        _pending_futures.add(future)


def remove_future(future):
    """Remove a future from the global registry."""
    with _registry_lock:
        _pending_futures.discard(future)


def wait_for_all_futures():
    """Wait for all registered futures to complete."""
    with _registry_lock:
        # Take a snapshot of the current futures
        futures = list(_pending_futures)
    for future in futures:
        # This call blocks until the future completes
        future.result()
