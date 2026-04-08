# FastAPI Request User Context

If your FastAPI app wants the resolved Main Sequence user attached to `request.state`, add the Main Sequence request-context middleware at app startup.

This middleware is optional convenience. It does not authenticate requests and it does not reject unauthenticated callers.

Use:

```python
from fastapi import FastAPI, Request

from mainsequence.client.fastapi import LoggedUserContextMiddleware


app = FastAPI(
    title="My API",
    version="0.1.0",
)

app.add_middleware(LoggedUserContextMiddleware)


@app.get("/me")
def get_me(request: Request) -> dict[str, object]:
    user = request.state.user
    return {
        "id": request.state.user_id,
        "username": user.username if user else None,
        "email": user.email if user else None,
    }
```

What the middleware does:

- binds the current request headers into `mainsequence.client.models_user._CURRENT_AUTH_HEADERS`
- tries to resolve the current user through `User.get_logged_user()`
- stores the resolved user on `request.state.user` when available
- stores the resolved user id on `request.state.user_id` when available
- continues the request even if no user can be resolved

Use `request.state.user` inside route handlers when you already have the request object. That is the clearest path.

Use `User.get_logged_user()` when you are in shared helper code that does not receive the request object directly.

If you do not add the middleware, `request.state.user` is not populated automatically. That does not mean the SDK is unusable. It only means this convenience layer is absent.

Use this pattern when you want request-local state:

```python
app = FastAPI(...)
app.add_middleware(LoggedUserContextMiddleware)
```

That setup is useful when you want the resolved Main Sequence user available directly on `request.state`.
