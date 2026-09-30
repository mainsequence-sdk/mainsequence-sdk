# ADR 0037: The session lives in the operating system credential store

Amended 2026-09-30: on macOS the CLI asks for the secret of a Keychain entry
only when the entry carries the CLI's own mark. The first implementation read
any entry under the name, and an entry that another program had written made
every process show a consent dialog. A session saved by another version is no
longer read; one login replaces it. See [macOS](#macos).

Amended 2026-09-30: `mainsequence code-repository refresh-token` is removed. Once
no checkout holds a credential, a token command that takes a checkout has no
purpose. `mainsequence refresh-token` renews the session for the machine.
Decision 2, the refresh command and the consequences below are edited
accordingly.

Date: 2026-09-30

Status: Accepted. Implemented for macOS and Linux. Windows keeps the behaviour
it had; see [Windows](#windows).

## Context

Released versions, up to 8.1.25, kept a developer's credentials in two places.

- `mainsequence code-repository set-up-locally` and
  `mainsequence code-repository refresh-token` wrote them into the `.env` of
  each CodeRepository checkout: the access token and the refresh token, or, in
  runtime-credential mode, the credential id, its secret and an exchanged
  access token.
- `mainsequence login` saved the session for the CLI. On macOS it used the
  login Keychain through Apple's `security` program, with the record on the
  command line and, since 4.0.1, through a login shell. On Linux and Windows,
  and on macOS when the Keychain entry could not be read back, it used a plain
  file, `auth.json`, in the CLI configuration directory.

The copies in `.env` had three costs.

- The tokens were in plain text in every checkout.
- Each checkout held its own copy. A copy went stale on its own, so the
  developer refreshed each CodeRepository separately.
- One machine had several places that claimed to hold the session.

The SDK did not need the copy. On import it reads only the endpoint from the
checkout `.env`, and takes the tokens from the process environment or from the
saved session. The copy served tools that load `.env` into a process
environment.

An earlier change on this unreleased line moved the saved session to the
`keyring` library on all three systems and removed the plain file as a
fallback. Keeping the session only in the credential store requires the store
to answer every interpreter on the machine the same way, because each
CodeRepository has its own virtual environment. These are the observations of
2026-09-30, with `keyring` 25.7.0:

- **macOS 26.4, Python 3.12.8 and 3.13.11.** The Keychain grants access to an
  entry per program. An entry written by the library, through the Security
  framework, made the other interpreter wait on a consent dialog, and
  `/usr/bin/security` did not read it without consent either. The library
  therefore tied the saved session to one interpreter, which the released
  `security` path had not done. An entry created through `security` was read
  and replaced through `security` from both interpreters with no dialog, in
  less than 0.1 seconds per call. `security` deleted an entry that another
  program owned with no dialog.
- **Linux, Secret Service provided by GNOME Keyring.** The library replaces its
  own item in place: the item kept its identifier across writes. It knows its
  own item by an `application` attribute. When another program had stored an
  item for the same service and account without that attribute, a write by the
  library added a second item.
- **`security -i`** reads one command per line into a 4,096-character buffer. A
  longer line was cut there: the first part ran with a truncated secret and the
  rest ran as another command.

## Decision

1. The session is one record per backend in the operating system credential
   store. A login made from any CodeRepository serves every other one on the
   machine. There is no plaintext fallback: without a credential store the
   session lasts for the current process only.
2. A CodeRepository `.env` holds the backend endpoint and no credential.
   `set-up-locally` writes the endpoint only, in every authentication mode.
   No command takes a checkout to refresh its tokens:
   `code-repository refresh-token` is removed, and `mainsequence refresh-token`
   renews the one session of the machine. See
   [The refresh command](#the-refresh-command).
3. A local tool that does not read the credential store obtains a short-lived
   access token with `mainsequence auth token`. The refresh token never leaves
   the store through that command. `mainsequence auth status` reports the
   session without any token value.
4. Credentials already in the process environment still win over the saved
   session, and a rejected pair from the environment is not replaced by the
   saved session, because that pair may belong to another user or backend. The
   error names the environment as the source and says whether a saved session
   exists.

## The record

The record is ASCII JSON:

```json
{"v": 1, "backend": "https://backend.example", "username": "...", "access": "...", "refresh": "..."}
```

- `backend` is the normalized backend URL. A reader refuses a record that names
  another backend. A record without the field was written by a released
  version and is accepted.
- A record with a refresh token and no access token is a complete session: the
  access token is renewed from it. A record with an access token only is a
  session in runtime-credential mode and nowhere else.
- `json.dumps` escapes non-ASCII characters, so every store holds the same
  bytes.
- Released versions read `username`, `access` and `refresh` and ignore the
  other fields.

The entry's service name is `MainSequenceCLI.auth` on every system. Its account
is `default.` followed by the first 16 hexadecimal digits of the SHA-256 of the
normalized backend URL. The account `default`, which versions before 4.0.1
used for every backend, is still read and is removed by a logout.

| System | Store | How the CLI reaches it |
| --- | --- | --- |
| macOS | Login Keychain | `/usr/bin/security`, with the record on standard input |
| Linux | Secret Service | The `keyring` library's Secret Service backend, named explicitly |
| Windows | Credential Manager | The `keyring` library's recommended backend |

On every system, a session that a released version left in `auth.json` is moved
into the store and removed from the file once the store holds it. Without a
store the file is left untouched and is not used. A store that could not be
read is not an empty store: nothing is moved then, so the entry that could not
be read is not replaced by an older session.

### macOS

macOS returns to `security`, which every released version used, under the same
entry name. `security` is one program for every interpreter, so an entry it
wrote is read from every CodeRepository without a dialog.

An entry that another program wrote is different: asking `security` for its
secret shows the consent dialog, in every process that starts. Such an entry
exists where the unreleased `keyring` build of this line saved a session, and
its attributes are the same as those of an entry `security` wrote, so the two
cannot be told apart. The CLI therefore marks every entry it writes with the
comment attribute `MainSequenceCLI.session.v1`, and asks for the secret only
after it has seen that mark. Attributes are read without consent, whoever
wrote the entry. An entry without the mark is never asked for: the CLI reports
that a session of another version is present and not read, and one login
replaces it. That holds for a session a released version saved as well, so an
upgrade on macOS needs one login.

A released version installed in another CodeRepository still shares the
session. It reads the entry this code wrote, and its update keeps the mark.

Three things differ from the released path.

- The record travels on standard input, as one `add-generic-password` line
  with the secret in hexadecimal. It used to be an argument of a command line,
  which other processes on the machine can list.
- `security` is started directly, not through a login shell.
- Every call has a 10-second limit.

A write updates in place an entry that the same process has read, so another
process never finds the session missing while a token is renewed. Any other
entry is removed and then added: removing an entry that another program owns
needs no consent, and the new entry belongs to `security`. A record whose line
would exceed 4,000 characters is refused before anything is removed or written,
so a long record cannot be stored as a truncated one. A session record of 800
characters makes a line of about 1,700; the limit leaves room for a record of
about 1,950.

A read can still wait for the user when the Keychain is locked. The call is cut
off at the limit and reported as a store error, and the process continues
without a saved session. An entry that could not be read, or that carries no
mark, is not looked at again for 60 seconds in that process. `mainsequence
doctor`, `mainsequence auth status`, `mainsequence auth token` and
`mainsequence refresh-token` say why the store could not be read, which
otherwise looks like a machine that is not logged in.

The cost of this arrangement is that any program running as the same user can
read the entry through `security` without a dialog. That was already so in the
released versions.

### Linux

The Secret Service backend is named explicitly instead of taking the library's
highest-priority backend, so the record is in the same store on every desktop.
A write first removes the items that other programs stored for the same service
and account, and then lets the library replace its own item in place. A delete
removes every match.

The library's highest-priority backend differs from Secret Service in one case:
a KDE desktop with `dbus-python` installed in the same environment, where it is
the KWallet backend. A session saved that way is not read, and one login
replaces it. A desktop with no Secret Service provider has no saved session:
the CLI reports that persistence is unavailable and the session lasts for the
process.

### Windows

Nothing about how the store is reached changes on Windows, and this decision
was not verified there. The record gains the `v` and `backend` fields; the
entry name, the backend and the persistence setting are the ones the `keyring`
library chooses.

## The token command

`mainsequence auth token` prints an access token for the session the process
would use: credentials set in the environment, otherwise the saved session. It
renews the token first when it would expire within 60 seconds. It asks nothing
and opens no browser.

With the global `--json` flag the output is one object with `endpoint`,
`access_token`, `token_type` (`Bearer`) and `expires_at` (epoch seconds, or
`null` when the token carries no expiry). Without it the access token alone is
printed.

| Exit code | Meaning |
| --- | --- |
| `0` | A token was printed |
| `1` | There is no session, the store could not be read, or the backend did not renew the session |
| `3` | The machine has no credential store and the environment carries no credentials |

`mainsequence auth status` exits `0` when a usable session exists and `1` when
it does not. Without `--check` it judges the session by the tokens' own expiry
and uses no network. Its `store_error` field is `null`, or the reason the
credential store could not be read.

## The refresh command

`mainsequence refresh-token` renews the saved session: it obtains a new access
token from the refresh token, or from the runtime credential of a platform
runtime, saves it, and reports the backend, the user and the session's expiry.
It takes no path and no CodeRepository. It prints no token value, asks nothing
and opens no browser. With `--json` it prints the report of
`mainsequence auth status` plus `removed_env_entries`. Its exit codes are those
of the token command.

Tokens that an earlier version or another tool left in a checkout would be
used, by any tool that loads that `.env`, instead of the saved session. When
the directory the command runs in has a `.env`, the command therefore removes
`MAINSEQUENCE_ACCESS_TOKEN`, `MAINSEQUENCE_REFRESH_TOKEN`,
`MAINSEQUENCE_RUNTIME_CREDENTIAL_ID`, `MAINSEQUENCE_RUNTIME_CREDENTIAL_SECRET`
and the unsupported `MAINSEQUENCE_TOKEN` from it, together with a
`MAINSEQUENCE_AUTH_MODE=runtime_credential` line. It names the removed entries
and never their values, changes no other line, and creates no file. It does
this before it renews the session, so a stale entry goes even on a machine
that is not logged in. An entry counts in the forms the tools that load a
`.env` accept: leading whitespace, an `export` prefix, and whitespace before
the equals sign.

## Consequences

- A tool that read the token pair from `.env` no longer finds it there. It
  receives the pair from the environment it is started in, for example after
  `eval "$(mainsequence login --export)"`, or it asks `mainsequence auth token`.
- A `.env` written by an earlier version keeps its tokens until
  `mainsequence refresh-token` runs in that checkout.
  `mainsequence doctor` names the credential entries it finds in the current
  checkout's `.env`.
- The CLI still passes the session to the child processes it starts through
  their environment. That is unchanged.

## Validation

Unit tests cover the per-system store selection, the record rules, the macOS
adapter against a stand-in for `security`, the Secret Service adapter against a
store that keeps the items of other programs, the removal of credential
entries from a `.env`, the refresh command, both `auth` commands and the
environment-source error.

The store functions were also run against real stores with test entry names and
dummy values:

- Secret Service in a container. Two saves in a row kept the item's
  identifier. With an item from another program present, a save left one item.
  The library alone left two.
- The login Keychain on macOS 26.4 from Python 3.12.8 and 3.13.11, together
  with the store code of the released 8.1.25. An entry the `keyring` library
  had written, and an entry the released code had written, were not asked for:
  both interpreters reported no session and the reason. A save replaced such an
  entry and both then read it. The released code read a session the new code
  saved and saved over it; the mark stayed and the new code read that session.
  A save after a read updated the entry in place. A too-long record was refused
  with the earlier session intact. No step waited for consent: each step, a
  whole process, took less than 0.4 seconds.
