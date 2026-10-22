# Code Repositories, Branches and Projects

A **code repository** is a repository linked to GitHub, with all its branches;
the SDK model is `CodeRepository`. A **branch**, `CodeRepositoryBranch`, is one
Git branch of a repository in one Organization Environment, with the Jobs,
releases and Agent that run from it. A **Project**, `OrganizationProject`, is a
named group of branches in one Environment, whose policies pass down to them. A
Project groups branches; it is not a repository.

Who can use each of them is decided by its policies. This page says what each
policy allows; [Who Has Access to an Object](object_access.md) describes the
access methods and their models. Organization admins hold every action on all
three.

## Who can use a code repository

A code repository's access is decided by its policies:

- `reader`: see the repository, its GitHub link and issues, and every branch in
  it;
- `editor`: also rename or archive it, add deploy keys, create branches, edit
  every branch, and see who has access;
- `owner`: also decide who has access.

The repository's creator holds `owner`. Deleting a repository is in no policy:
only Organization admins delete one, so `explain_access("code-repositories.delete")`
names no policy that gives it. A deploy key lets a machine push to the GitHub
repository, which is part of what `editor` gives.

A repository's policies pass down to every branch: `reader` gives `reader` on
each branch, and `editor` and `owner` give `editor`. A branch lives in an
Environment, so a person reaches it only in an Environment they work in.

A policy on one of the repository's branches, held directly, through a Team or
through a Project, also reaches the repository without reaching its other
branches. Branch `reader` gives the computed policy `branch_reader`: the
repository appears in lists and search, with its GitHub link, branch names and
issues. Branch `editor` and `owner` give `branch_editor`, which also adds deploy
keys, so a person who edits a branch can develop locally. `get_access()` shows
them as `computed` rows with `source` `inherited` that name the branch. Neither
can be attached, and both apply only while the person works in the branch's
Environment.

The policies go only to people with the platform's `user` product role, to
workload identities and to Teams; a Light User does not have that role. Using a
repository needs the role too: a person without it gets
`PermissionDeniedError` with `code` `missing_product_role` from every
repository route, the access routes included, also when a Team they belong to
holds a policy. That refusal names no policy, because no policy can give it.

Read and change the policies with the access methods on `CodeRepository`:

```python
from mainsequence.client import CodeRepository

repository = CodeRepository.get_by_uid(code_repository_uid)

for binding in repository.get_access().bindings:
    print(binding.uid, binding.policy.id, binding.principal.type, binding.principal.uid)

binding = repository.attach_policy("reader", principal_type="team", principal_uid=team_uid)
repository.detach_policy(binding.uid)

answer = repository.explain_access("code-repositories.update", principal_uid=user_uid)
print(answer.allowed, answer.granting_policies)
```

The access methods address the repository by its UID alone and take no
Environment. They need a person's session: the platform refuses them to a
workload's runtime credentials. A refused change raises
`PermissionDeniedError`, whose `payload` names the `required_action` and the
`granting_policies`. `mainsequence access policies --type code-repositories`
lists the policies with their actions.

## Who can use a branch

A branch's access is decided by its policies:

- `reader`: see the branch, its files and issues, and its Jobs, runs, logs and
  releases, and call its releases, whose logs can contain request data; it also
  finds and shows the branch's repository;
- `editor`: also change and delete the branch, create issues and static sites,
  edit its Jobs and releases, manage what its workloads can reach, add deploy
  keys to its repository, and see who has access;
- `owner`: also decide who has access.

Creating a branch gives its creator nothing on it. A branch also takes the
policies held above it: `reader` on its repository, or on a Project it belongs
to, gives `reader` on the branch, and `editor` or `owner` there gives `editor`.
`get_access()` shows each of those as a `computed` row with `source`
`inherited`, which names the holder, the parent and the parent's policy. The
branch's policies pass down in turn to its releases and its standalone Jobs:
`reader` gives `viewer` there, and `editor` and `owner` give `editor`. Up to
the repository they give `branch_reader` or `branch_editor`
([Who can use a code repository](#who-can-use-a-code-repository)), never its
other branches. A release that backs the branch's Agent follows the Agent
instead; see
[Who can use a release](resource_releases.md#who-can-use-a-release) and
[Who can use a Job](scheduling_jobs.md#who-can-use-a-job).
Deleting a branch deletes its releases, its Agent, its Jobs and its build
contexts. Deleting a repository's last branch deletes the repository, so it
also needs the repository's `delete` action, which only Organization admins
hold.

The policies go to people, workload identities and Teams. A person without the
platform's `user` product role gets `PermissionDeniedError` with `code`
`missing_product_role` from every branch route, the access routes included,
also when a Team they belong to holds a policy. A repository workflow file's
`access.branch` and `access.branches` entries give the declaring workload the
policy they name on the branch (`branch: reader`, or `policy: reader` on a
`branches` entry).

```python
from mainsequence.client import CodeRepositoryBranch

branch = CodeRepositoryBranch.get_by_uid(branch_uid)

for row in branch.get_access().computed:
    print(row.source, row.policy, row.parent_policy)

branch.attach_policy("editor", principal_type="user", principal_uid=workload_user_uid)
print(branch.get_my_access().allowed_actions)
```

The access methods address the branch by its UID alone and take no
Environment, and the platform refuses them to runtime credentials.
`mainsequence access policies --type code-repository-branches` lists the
policies with their actions.

## Who can use a Project

A Project's access is decided by its policies:

- `reader`: see the Project, every branch in it, and those branches'
  repositories;
- `editor`: also rename it, add and remove branches, delete it, add deploy keys
  to its branches' repositories, and see who has access;
- `owner`: also decide who has access.

The Project's creator holds `owner`. A Project's policies pass down to its
branches: `reader` gives `reader` on each of them, and `editor` and `owner`
give `editor`. Through each branch they reach its repository as
`branch_reader` or `branch_editor`, so a Project editor finds the repositories
and adds deploy keys to develop locally, without reaching their other
branches. Adding a branch to a Project also needs `editor` or `owner` on
that branch, and deleting a Project keeps its branches. The policies go to
people, workload identities and Teams.

The SDK has no other Project methods: `OrganizationProject` carries only the
Project's UID, which is all its access methods use.

```python
from mainsequence.client import OrganizationProject

project = OrganizationProject(uid=project_uid)
binding = project.attach_policy("reader", principal_type="team", principal_uid=team_uid)
print([item.policy.id for item in project.get_access().bindings])
```

The access methods take no Environment, and the platform refuses them to
runtime credentials. `mainsequence access policies --type organization-projects`
lists the policies with their actions.

## From the command line

These commands read and change a code repository's policies:

```bash
mainsequence code-repository access list <CODE_REPOSITORY_UID>
mainsequence code-repository access grant <CODE_REPOSITORY_UID> reader --user <USER_UID>
mainsequence code-repository access grant <CODE_REPOSITORY_UID> editor --team <TEAM_UID>
mainsequence code-repository access revoke <CODE_REPOSITORY_UID> <BINDING_UID>
mainsequence code-repository access explain <CODE_REPOSITORY_UID> code-repositories.update
```

`<USER_UID>` is the public UUID of the person or workload identity receiving
the policy, `<TEAM_UID>` the team's, and `<BINDING_UID>` the UID of a binding,
which `access list` shows. The commands address the repository by its UID
alone and need no registered branch. A refused command prints the platform's
message and the policies that would allow it; a missing product role prints
the platform's message. The CLI has no commands for branches and Projects: use
their access methods. See
[Code repositories](../../cli/index.md#code-repositories) in the CLI reference.
