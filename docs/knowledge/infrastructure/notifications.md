# Notifications

Notifications are the platform-level messaging primitive for user, team, organization, and system delivery.

Use the client model:

- `mainsequence.client.models_user.Notification`

Notifications are not just UI banners. They are scoped objects with recipient rules, visibility rules, and explicit read/dismiss actions.

## Client Methods

The client supports three creation paths:

### `Notification.send(...)`

Creates organization-scoped notifications.

Allowed recipients:

- one `target_user_uid`
- one `target_team_uid`
- multiple `user_uids`
- multiple `team_uids`

The backend creates one notification per resolved recipient target.

Use this when the sender and recipients live inside the same organization scope.

### `Notification.send_to_self(...)`

Creates an organization-scoped notification addressed to the authenticated user only.

Use this for user-local reminders, confirmations, or self-service workflow messages.

### `Notification.send_system(...)`

Creates a system-scoped notification.

Only staff users or superusers are allowed to do this.

Use this for platform-wide operational or administrative messaging, not normal organization messaging.

## Who Can Send To Whom

### `LIGHT_USER`

- can only send notifications to themselves
- cannot send to other users
- cannot send to teams
- cannot send system notifications

### `DEV_USER`

- can send to themselves
- can send to members of teams they belong to
- can send to teams they can access through the existing team access model
- cannot send system notifications
- cannot target users outside the teams they belong to or can access

### `ORG_ADMIN`

- follows organization-scoped rules inside their organization
- can operate within organization scope
- cannot operate in system scope unless they are also staff or superuser

### `staff` / `superuser`

- can send system notifications
- can send system-global notifications
- can also send organization notifications

## Organization Rules

For organization-scoped notifications:

- sender must belong to the source organization
- targeted users must belong to the same organization
- targeted teams must belong to the same organization

This is the normal path used by `Notification.send(...)` and `Notification.send_to_self(...)`.

## System Rules

For system notifications:

- only staff or superuser may create them
- this is enforced by DRF and by hard model validation

This is the path used by `Notification.send_system(...)`.

## Visibility Rules

Visible notifications include:

- system-global notifications
- organization-global notifications for the user’s organization
- direct user notifications
- team-targeted notifications for teams the user is in

This is the effective read surface the user sees when listing notifications.

## Read And Bulk Actions

Available instance actions:

- `mark_read()`
- `dismiss()`

Available bulk actions:

- `mark_all_read()`
- `dismiss_all()`

Use bulk actions when the operation is about the current user’s visible notification set, not a single notification row.

## Basic Examples

Send to one user in the same organization:

```python
from mainsequence.client.models_user import Notification

Notification.send(
    type="IN",
    title="Dataset ready",
    description="The dataset finished loading.",
    target_user_uid="8f5d6b54-2f5e-4a8b-bb10-0b17f3f4c123",
)
```

Send to one team:

```python
from mainsequence.client.models_user import Notification

Notification.send(
    type="IN",
    title="Model review required",
    description="A new model version is ready for review.",
    target_team_uid="3f1cc452-43ec-49cb-b2ba-87dbac164d29",
)
```

List active teams and send one notification to each team:

```python
from mainsequence.client.models_user import Notification, Team

teams = Team.filter(is_active=True)

for team in teams:
    Notification.send(
        type="IN",
        title=f"Update for {team.name}",
        description=f"Hello {team.name}, there is a new platform update to review.",
        target_team_uid=team,
    )
```

List active teams and include the team name in both the title and metadata:

```python
from mainsequence.client.models_user import Notification, Team

teams = Team.filter(is_active=True)

for team in teams:
    Notification.send(
        type="IN",
        title=f"{team.name}: workflow update",
        description=f"The {team.name} workflow has been updated.",
        target_team_uid=team.uid,
        meta_data={
            "team_uid": team.uid,
            "team_name": team.name,
        },
    )
```

Send the same notification to several teams in one call:

```python
from mainsequence.client.models_user import Notification, Team

teams = Team.filter(is_active=True)
team_uids = [team.uid for team in teams[:3]]

Notification.send(
    type="IN",
    title="Quarterly review window",
    description="The quarterly review window is now open.",
    team_uids=team_uids,
)
```

Send the same notification to several users in one call:

```python
from mainsequence.client.models_user import Notification, Team

research = Team.filter(search="Research", is_active=True)[0]
members = research.list_members()
user_uids = [member.uid for member in members]

Notification.send(
    type="IN",
    title="Research sync",
    description="The research sync starts in 15 minutes.",
    user_uids=user_uids,
)
```

Send to yourself:

```python
from mainsequence.client.models_user import Notification

Notification.send_to_self(
    type="IN",
    title="Sync complete",
    description="Your local project sync completed successfully.",
)
```

Send a system notification:

```python
from mainsequence.client.models_user import Notification

Notification.send_system(
    type="IM",
    title="Planned maintenance",
    description="Platform maintenance starts at 22:00 UTC.",
    is_global=True,
)
```

Mark one notification as read:

```python
notification = Notification.filter()[0]
notification.mark_read()
```

Dismiss everything visible to the current user:

```python
Notification.dismiss_all()
```

## Practical Guidance

- use `send_to_self(...)` when the message is only for the current user
- use `send(...)` for normal organization delivery
- use `send_system(...)` only for true platform-wide administrative messaging
- do not treat team targeting as cross-organization messaging
- do not assume a user can message any other user in the organization without the role and team rules above

For broader organization, team, and sharing semantics, see [Users and Access](users_and_access.md).
