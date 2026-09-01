# Streamlit Dashboard Support Removal

Main Sequence-managed Streamlit dashboard support has been removed as a hard
cut. This is not a deprecation period and the SDK provides no compatibility
alias, fallback runtime, tombstone method, or automatic application conversion.

## Removed SDK Contract

The following public SDK surface no longer exists:

- `ResourceReleaseKind.STREAMLIT_DASHBOARD`
- `CodeRepositoryResource.create_dashboard()`
- the `dashboard` `CodeRepositoryResource.resource_type` discriminator
- `mainsequence code-repository resources create_dashboard`
- `mainsequence code-repository resources delete_dashboard`
- implicit request identity from Streamlit request headers

`ResourceRelease.release_kind` is required and accepts only supported release
kinds. Generic release and resource response models intentionally reject the
retired Streamlit discriminators.

## Consumer Action

Existing Streamlit source code is not rewritten by the SDK or platform. Choose
a supported application architecture and implement it deliberately. Depending
on the product requirement, that may be a FastAPI service, a static site, or a
Command Center application surface. These are architectural alternatives, not
automatic migration targets.

Remove retired workflow declarations, dashboard discovery assumptions, CLI
commands, and `User.get_logged_user()` calls that depended on implicit
Streamlit headers. FastAPI handlers receive the human request identity from the
Main Sequence platform through `request.state.user` and
`request.state.user_uid`.
