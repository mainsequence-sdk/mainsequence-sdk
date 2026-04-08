# FastAPI Authenticated User Context

If your FastAPI app needs to know who is calling it, add the Main Sequence auth middleware at app startup.

Without it, SDK helpers such as `User.get_logged_user()` do not have request headers bound into the client context, so user resolution will fail outside Streamlit.

Use:

```python
from fastapi import FastAPI, Request

from mainsequence.client.fastapi import AuthenticatedUserMiddleware
from mainsequence.client.models_user import User


app = FastAPI(
    title="My API",
    version="0.1.0",
)

app.add_middleware(AuthenticatedUserMiddleware)


@app.get("/me")
def get_me(request: Request) -> dict[str, object]:
    user = request.state.user
    return {
        "id": request.state.user_id,
        "username": user.username,
        "email": user.email,
    }


@app.get("/me-sdk")
def get_me_via_sdk() -> dict[str, object]:
    user = User.get_logged_user()
    return {
        "id": user.id,
        "username": user.username,
    }
```

What the middleware does:

- binds the current request headers into `mainsequence.client.models_user._CURRENT_AUTH_HEADERS`
- resolves the authenticated user through `User.get_logged_user()`
- stores the resolved user on `request.state.user`
- stores the resolved user id on `request.state.user_id`
- returns `401` if the request is not authenticated

Use `request.state.user` inside route handlers when you already have the request object. That is the clearest path.

Use `User.get_logged_user()` only when you are in shared helper code that does not receive the request object directly.

If you do not add the middleware, this pattern is not available:

```python
app = FastAPI(...)
app.add_middleware(AuthenticatedUserMiddleware)
```

That setup is required whenever you want to resolve the currently authenticated Main Sequence user from inside a FastAPI request.
