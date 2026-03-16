# Constants and Secrets

Constants and secrets solve the same broad problem:

> how should a Main Sequence workflow receive configuration at runtime?

They are both small pieces of runtime configuration, but they are not interchangeable.

Use the split below:

- `Constant`: safe operational configuration
- `Secret`: sensitive configuration

If leaking the value would create an incident, it should not be a constant.

## Quick Decision Rule

Use a `Constant` for things like:

- feature flags
- model windows and thresholds
- dataset names
- vendor identifiers
- small JSON mappings
- mode switches such as `production`, `paper`, or `sandbox`

Use a `Secret` for things like:

- API keys
- bearer tokens
- passwords
- webhook signing secrets
- connection credentials
- private vendor access tokens

In practice:

- `MODEL__DEFAULT_WINDOW = 252` is a constant
- `FEATURE__ENABLE_VALIDATION = true` is a constant
- `POLYGON_API_KEY = "..."` is a secret
- `AWS_SECRET_ACCESS_KEY = "..."` is a secret

## Mental Model

Think of constants and secrets as runtime inputs, not workflow outputs.

Jobs, resources, dashboards, agents, and notebooks often need configuration that should not live directly inside code. Constants and secrets give you a place to store that configuration in the platform.

A useful mental split is:

- code defines behavior
- constants define non-sensitive runtime settings
- secrets define protected credentials

That keeps the repository cleaner and reduces the amount of environment-specific data hardcoded into scripts and jobs.

## Constants

The client model is:

```python
from mainsequence.client import Constant
```

A constant is a small, readable, organization-level configuration value.

The current SDK model exposes:

- `id`
- `name`
- `value`
- `category`

### What constants are good for

Good fits:

- default windows such as `252`
- tuning parameters such as confidence thresholds
- external dataset names
- reusable small lookup dictionaries
- environment-independent configuration values

Bad fits:

- passwords
- API keys
- secrets copied from other systems
- anything that should not be casually displayed in a UI, notebook, or log

### Naming and categories

Constants are expected to use `UPPER_SNAKE_CASE`.

You can also group constants with a double underscore:

```text
ASSETS__MASTER
MODEL__DEFAULT_WINDOW
FEATURE__ENABLE_VALIDATION
BROKER__DEFAULT_ACCOUNT
```

This does not create a nested object. It is just a naming convention.

Main Sequence treats the prefix before `__` as a human-readable category. For example:

- `ASSETS__MASTER` is displayed under category `ASSETS`
- `MODEL__DEFAULT_WINDOW` is displayed under category `MODEL`

This is useful when you want the constants table to stay readable without inventing a more complex hierarchy.

### Constant examples

Simple scalar value:

```python
Constant.create(
    name="MODEL__DEFAULT_WINDOW",
    value=252,
)
```

Boolean flag:

```python
Constant.create(
    name="FEATURE__ENABLE_VALIDATION",
    value=True,
)
```

Small JSON object:

```python
Constant.create(
    name="BROKER__DEFAULTS",
    value={
        "execution_venue": "paper",
        "account_type": "margin",
    },
)
```

### Reading constants

Read one constant:

```python
from mainsequence.client import Constant

default_window = Constant.get(name="MODEL__DEFAULT_WINDOW").value
```

Read just the value:

```python
default_window = Constant.get_value("MODEL__DEFAULT_WINDOW")
```

Read multiple constants by name:

```python
constants = Constant.filter(
    name__in=[
        "MODEL__DEFAULT_WINDOW",
        "FEATURE__ENABLE_VALIDATION",
    ]
)
```

Current client-side supported filters are:

- `name`
- `name__in`

### When to prefer a constant over code

Use a constant instead of hardcoding when:

- operators may need to change the value without editing source
- the value is reused across several jobs or scripts
- the value is environment or organization specific
- the value is business configuration, not program logic

Examples:

- a rebalance threshold
- a default benchmark name
- a vendor dataset alias
- a list of supported external identifiers

## Secrets

The client model is:

```python
from mainsequence.client import Secret
```

A secret is protected configuration. The platform should treat it differently from normal application settings.

The current SDK model exposes:

- `id`
- `name`
- `value`

In practice, some backend responses may only return the secret name rather than echoing the value back.

### What secrets are good for

Use a secret for:

- `POLYGON_API_KEY`
- `OPENAI_API_KEY`
- `AWS_SECRET_ACCESS_KEY`
- broker credentials
- database passwords
- signed webhook tokens

If you would be uncomfortable seeing the value in a screenshot, error email, or notebook output, it should be a secret.

### Secret examples

Create a secret:

```python
from mainsequence.client import Secret

Secret.create(
    name="POLYGON_API_KEY",
    value="***",
)
```

Read a secret:

```python
polygon_key = Secret.get(name="POLYGON_API_KEY").value
```

Filter secrets by name:

```python
secrets = Secret.filter(name__in=["POLYGON_API_KEY", "OPENAI_API_KEY"])
```

Current client-side supported filters are:

- `name`
- `name__in`

### Operational rules for secrets

Secrets need a different operational standard than constants:

- do not commit them to the repository
- do not copy them into constants
- do not print them in logs
- do not paste them into dashboards or notebooks casually
- rotate them when external systems require it

A good working rule is:

- constants may be read frequently and discussed openly
- secrets should be accessed only where they are required

## Human-Readable Examples

### Example 1: Market data integration

You are integrating a vendor feed.

Use constants for:

- the vendor dataset name
- a default universe identifier
- a feature flag enabling a fallback path

Use secrets for:

- the vendor API key
- the vendor bearer token

Example split:

- `VENDOR__DATASET = "us_equities_realtime"` as a constant
- `VENDOR__DEFAULT_EXCHANGE = "XNYS"` as a constant
- `VENDOR_API_KEY` as a secret

### Example 2: Model configuration

You have a job that computes signals using a rolling window and a threshold.

Use constants for:

- rolling window
- minimum signal score
- default region

Do not use secrets here unless an external protected service is involved.

Example:

- `MODEL__DEFAULT_WINDOW = 252`
- `MODEL__MIN_SCORE = 0.85`
- `MODEL__REGION = "US"`

### Example 3: External execution venue

You have a workflow that talks to a broker or exchange.

Use constants for:

- default account alias
- execution mode such as `paper` or `live`
- venue configuration labels

Use secrets for:

- API secret
- refresh token
- signing private key

## CLI Usage

The CLI exposes both resources directly.

Constants:

```bash
mainsequence constants list
mainsequence constants create MODEL__DEFAULT_WINDOW 252
mainsequence constants create BROKER__DEFAULTS '{"mode":"paper"}'
mainsequence constants delete 42
```

Secrets:

```bash
mainsequence secrets list
mainsequence secrets create POLYGON_API_KEY your-secret-value
mainsequence secrets delete 42
```

Important behavior:

- constants display the category derived from the prefix before `__`
- secrets are shown by metadata only in CLI tables and delete previews
- delete commands require typed verification

## Recommended Practice

A good configuration layout usually looks like this:

- constants contain readable runtime settings
- secrets contain credentials only
- application code reads both, but does not redefine them

A healthy split is:

- store business and runtime configuration in constants
- store credentials and protected tokens in secrets

That gives you:

- better readability
- safer operations
- fewer credentials hardcoded in project code

## Anti-Patterns

Avoid these:

- storing API keys in constants
- storing large application config blobs in secrets
- hardcoding credentials in source files
- using secrets for ordinary non-sensitive flags
- creating many nearly identical constants when one small JSON object would do

## Final Rule

If the value is safe to read, review, and discuss as part of ordinary project configuration, use a `Constant`.

If the value must be protected, use a `Secret`.
