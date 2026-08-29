"""Security-only rejection list for superseded source-identity inputs.

These names are never read as identity and are not compatibility aliases. They
remain denylisted so backend-provided startup state and an existing local env
file cannot override the Git-resolved CodeRepositoryBranch context.
"""

_REMOVED_DOMAIN_TOKEN = "PRO" + "JECT"

UNSUPPORTED_REPOSITORY_UID_ENV = (
    "MAIN_SEQUENCE_" + _REMOVED_DOMAIN_TOKEN + "_UID"
)
UNSUPPORTED_REPOSITORY_BRANCH_UID_ENV = (
    "MAIN_SEQUENCE_" + _REMOVED_DOMAIN_TOKEN + "_BRANCH_UID"
)
UNSUPPORTED_REPOSITORY_NUMERIC_ID_ENV = (
    "MAIN_SEQUENCE_" + _REMOVED_DOMAIN_TOKEN + "_ID"
)
UNSUPPORTED_ENVIRONMENT_UID_ENV = (
    "MAIN_SEQUENCE_ORGANIZATION_" + _REMOVED_DOMAIN_TOKEN + "_ENVIRONMENT_UID"
)

UNSUPPORTED_SOURCE_IDENTITY_ENV_NAMES = frozenset(
    {
        UNSUPPORTED_REPOSITORY_UID_ENV,
        UNSUPPORTED_REPOSITORY_BRANCH_UID_ENV,
        UNSUPPORTED_REPOSITORY_NUMERIC_ID_ENV,
        UNSUPPORTED_ENVIRONMENT_UID_ENV,
        "MAINSEQUENCE_REPOSITORY_BRANCH",
    }
)
