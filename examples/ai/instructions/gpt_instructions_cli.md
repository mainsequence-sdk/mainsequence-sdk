# MainSequence CLI — Usage Guide

> Run every command via the module entrypoint:  
> `mainsequence …`

This CLI helps you sign in, view and set up projects locally (with SSH keys generated & added automatically), open mapped folders, and optionally build/run containers from a project.

---

## Quick start

```powershell
# 1) Sign in (prompts for password)
mainsequence login you@example.com

# 2) See your current settings
mainsequence settings

# 3) List projects you have access to
mainsequence project list

# 4) Clone & link a project locally (creates SSH keys, starts ssh-agent, writes .env)
mainsequence project set-up-locally 123

# 5) Open the mapped folder in File Explorer
mainsequence project open 123

# 6) Open a PowerShell terminal pre-wired with ssh-agent & the project's key
mainsequence project open-signed-terminal 123
```

---

## Prerequisites

- **Python** and **git** available on PATH.
- **OpenSSH** (included in Windows 10/11) and **ssh-agent** service available.
- For clipboard copy of the public key (optional): Windows clipboard is handled automatically. On macOS/Linux, requires `pbcopy` (macOS) or `wl-copy`/`xclip` (Linux).
- For `build_and_run`:  
  - **uv** (`pip install uv`)  
  - **Docker Desktop** (if you use the Docker part).

---

## Configuration & files

- **Config directory**
  - **Windows:** `%APPDATA%\MainSequenceCLI`  
    (typically: `C:\Users\YourName\AppData\Roaming\MainSequenceCLI`)
  - **macOS:** `~/Library/Application Support/MainSequenceCLI`
  - **Linux:** `~/.config/mainsequence`
- **Files**
  - `config.json` — stores `backend_url` and `mainsequence_path`
  - `token.json` — stores `{ username, access, refresh, ts }`
  - `project-links.json` — maps project IDs to absolute local paths

**Defaults**
- Backend: `https://main-sequence.app/` (override with `MAIN_SEQUENCE_BACKEND_URL`)
- Projects base: 
  - **Windows:** `C:\Users\YourName\mainsequence`
  - **macOS/Linux:** `~/mainsequence`

---

## Environment variables

- `MAIN_SEQUENCE_BACKEND_URL` – set before running the CLI to point at a different backend.
- `MAIN_SEQUENCE_USER_TOKEN` – access token; `login --export` prints a shell-ready export line.

---

## Top-level commands

### `login`
Obtain and store tokens, set `MAIN_SEQUENCE_USER_TOKEN` for the current process, and (by default) print a projects table.

```powershell
mainsequence login <email>
# options:
#   --export      Print: export MAIN_SEQUENCE_USER_TOKEN="…"
#   --no-status   Do not print the projects table after login
```

**Examples**

**Windows PowerShell:**
```powershell
# The CLI prints a POSIX export line, but you need to set it manually in PowerShell
mainsequence login you@example.com --export
# Copy the token from output, then:
$env:MAIN_SEQUENCE_USER_TOKEN = "<paste token here>"
```

**macOS/Linux (bash/zsh):**
```bash
# Set token env var for the current shell
eval "$(mainsequence login you@example.com --export)"
```

---

### `settings`
Show or change local settings.

```powershell
# `settings` with no subcommand == `settings show`
mainsequence settings
mainsequence settings show

# Change the projects base folder (created if missing)
# Windows:
mainsequence settings set-base "D:\Work\mainsequence"
mainsequence settings set-base "C:\Users\YourName\Projects\mainsequence"

# macOS/Linux:
# mainsequence settings set-base ~/work/mainsequence
```

Output of `show` is JSON, e.g.:

**Windows:**
```json
{
  "backend_url": "https://main-sequence.app/",
  "mainsequence_path": "C:\\Users\\YourName\\mainsequence"
}
```

**macOS/Linux:**
```json
{
  "backend_url": "https://main-sequence.app/",
  "mainsequence_path": "/home/alex/mainsequence"
}
```

---

### `project` (requires login)

#### `project list`
List projects with local mapping status and guessed path.

```powershell
mainsequence project list
```

Output columns:
- **ID, Project, Data Source, Class, Status, Local, Path**  
`Local` is **Local** if a mapping exists or a default folder is present; otherwise `—`.

#### `project set-up-locally <project_id>`
End-to-end local setup:
1. Figures out the repository SSH URL (from `git_ssh_url` or nested data source hints).
2. Creates a dedicated SSH key in the `.ssh` directory (if missing):
   - **Windows:** `C:\Users\YourName\.ssh\<repo-name>`
   - **macOS/Linux:** `~/.ssh/<repo-name>`
3. Tries to register the public key as a **deploy key** on the server.
4. Starts `ssh-agent` and adds that key.
5. Clones into:  
   - **Windows:** `<base>\<org-slug>\projects\<safe-project-name>`
   - **macOS/Linux:** `<base>/<org-slug>/projects/<safe-project-name>`
6. Fetches environment text from the backend and writes `.env` (ensuring `VFB_PROJECT_PATH=<local-path>`).

```powershell
mainsequence project set-up-locally 123

# Optional: override base dir for this one command
# Windows:
mainsequence project set-up-locally 123 --base-dir "D:\my-projects"
# macOS/Linux:
# mainsequence project set-up-locally 123 --base-dir ~/my-projects
```

**Exit codes**
- `1` – project not found/visible, or repo URL missing
- `2` – target folder already exists
- `3` – `git clone` failed

#### `project open <project_id>`
Opens the mapped/guessed folder in your OS file manager.

```powershell
mainsequence project open 123
```

If no mapping exists yet, the CLI tries the default path:
- **Windows:** `<base>\<org-slug>\projects\<safe-project-name>`
- **macOS/Linux:** `<base>/<org-slug>/projects/<safe-project-name>`

#### `project delete-local <project_id> [--permanent]`
Unlink the local mapping; optionally **delete** the folder.

```powershell
# just unlink the mapping (keeps folder)
mainsequence project delete-local 123

# unlink and remove the folder (dangerous)
mainsequence project delete-local 123 --permanent
```

#### `project open-signed-terminal <project_id>`
Opens a new terminal window **in the project directory** with `ssh-agent` started and the repo key added.  
- **Windows:** PowerShell window; tries to enable the `ssh-agent` service.  
- **macOS:** opens **Terminal.app** with a ready shell.  
- **Linux:** opens the first available emulator (e.g. `gnome-terminal`, `konsole`, etc.).

```powershell
mainsequence project open-signed-terminal 123
```

---

### `build_and_run [DOCKERFILE]`
Project packaging helper:

1) `uv lock`  
2) `uv export --format requirements --no-dev --hashes > requirements.txt`  
3) If `DOCKERFILE` provided: `docker build …` then `docker run -it` the image.

```powershell
# Only lock & export requirements.txt (no Docker)
mainsequence build_and_run

# Build & run with a specific Dockerfile
# Windows:
mainsequence build_and_run .\Dockerfile
# macOS/Linux:
# mainsequence build_and_run ./Dockerfile
```

**Requirements & behavior**
- Fails early if `uv` is missing or `pyproject.toml` isn't present.
- Image name defaults to `<cwd-name>-img` (override with `IMAGE_NAME`).
- Tag defaults to short `git` SHA, else a timestamp (override with `TAG`).

---

## Typical workflow

```powershell
# sign in
mainsequence login you@example.com

# pick a project to set up
mainsequence project list

# set it up locally (creates SSH key, clones, writes .env, links the folder)
mainsequence project set-up-locally 456

# open the folder to browse files
mainsequence project open 456

# open a ready-to-use terminal for git operations over SSH
mainsequence project open-signed-terminal 456
```

---

## Troubleshooting

- **"Not logged in."**  
  Run `mainsequence login <email>` again. Tokens live in `token.json`.

- **Token refresh / 401**  
  The CLI will auto-refresh once. If it still fails, re-login.

- **No project repo URL found**  
  The CLI looks at `git_ssh_url` then digs into `data_source.related_resource.extra_arguments`.  
  If your project truly lacks a repo, ask an admin to attach one.

- **`git clone` failed**  
  Likely SSH access. The CLI generates an SSH key and tries to add it as a deploy key.  
  - **Windows:** Key is at `C:\Users\YourName\.ssh\<repo-name>`
  - **macOS/Linux:** Key is at `~/.ssh/<repo-name>`
  
  You may need to add that **public key** to your Git host manually. Re-run after access is granted.

- **ssh-agent service not running (Windows)**  
  Open PowerShell as Administrator and run:
  ```powershell
  Set-Service ssh-agent -StartupType Automatic
  Start-Service ssh-agent
  ```

- **Clipboard didn't copy the public key**  
  - **Windows:** Clipboard should work automatically
  - **macOS:** Install `pbcopy` (usually pre-installed)
  - **Linux:** Install `wl-copy` or `xclip`
  
  Or manually open the `.pub` file and copy the contents.

- **Linux: "No terminal emulator found"** when opening a signed terminal  
  Install one of: `x-terminal-emulator`, `gnome-terminal`, `konsole`, `xfce4-terminal`, `tilix`, `mate-terminal`, `alacritty`, `kitty`, or `xterm`.

- **Change backend URL**  
  
  **Windows PowerShell:**
  ```powershell
  $env:MAIN_SEQUENCE_BACKEND_URL = "https://staging.main-sequence.app"
  mainsequence project list
  ```
  
  **macOS/Linux (bash/zsh):**
  ```bash
  export MAIN_SEQUENCE_BACKEND_URL="https://staging.main-sequence.app"
  mainsequence project list
  ```
  
  Or edit `config.json` directly in your config directory.

---

## Help

```powershell
mainsequence --help
mainsequence login --help
mainsequence settings --help
mainsequence project --help
mainsequence build_and_run --help
```