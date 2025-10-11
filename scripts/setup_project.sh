#!/usr/bin/env bash
set +e  # Don't abort on error

# Source our new utils
source "$(dirname "$0")/utils.sh"

echo "======================================================"
echo "PostStart script invoked at: $(date)"
echo "Running as user: $(whoami)"

mkdir -p /tmp

if [ -z "$HOME_DIR" ]; then
  HOME_DIR="/tmp/repo"
fi



# >>> Resolve PROJECT_NAME early and recompute paths (fixes empty PROJECT_NAME) <<<
PNAME="${PROJECT_NAME:-}"
if [ -z "$PNAME" ] && [ -n "${GIT_REPO_URL:-}" ]; then
  PNAME="$(basename -s .git "$GIT_REPO_URL")"
fi
PNAME="${PNAME:-project}"
PROJECT_NAME="$PNAME"
ROOT_PROJECT_PATH="$HOME_DIR/$PROJECT_NAME"
PKG_NAME="${PROJECT_NAME//-/_}"

if [ -z "$VFB_PROJECT_PATH" ]; then
  VFB_PROJECT_PATH=$ROOT_PROJECT_PATH/$PROJECT_LIBRARY_NAME
fi

echo ">> HOME_DIR: $HOME_DIR"
echo ">> PROJECT_NAME: $PROJECT_NAME"
echo ">> VFB_PROJECT_PATH: $VFB_PROJECT_PATH"
echo ">> AUTHENTICATION_METHOD: $AUTHENTICATION_METHOD"

# Template locations (keep as-is)
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PYPROJECT_TEMPLATE="${SCRIPT_DIR}/pyproject.template.toml"
README_TEMPLATE="${SCRIPT_DIR}/README.template.md"

# Nodes template + target
NODE_TEMPLATE="${SCRIPT_DIR}/template_node_file.py"
NODES_TARGET="$ROOT_PROJECT_PATH/src/data_nodes/nodes.py"

if [ ! -f "${ROOT_PROJECT_PATH}/requirements.txt" ]; then
  echo "File ${ROOT_PROJECT_PATH}/requirements.txt does not exist. Cloning repo..."

  # Basic SSH config if we’re using SSH-based Git
  mkdir -p "$HOME_DIR/.ssh" || true
  chmod 700 "$HOME_DIR/.ssh" || true

  echo "Host *" > "$HOME_DIR/.ssh/config" 2>/dev/null || true
  echo "  StrictHostKeyChecking no" >> "$HOME_DIR/.ssh/config" 2>/dev/null || true
  chmod 600 "$HOME_DIR/.ssh/config" || true

  # Write private key (only relevant if using SSH)
  if [ -n "$GIT_PRIVATE_KEY" ]; then
    echo "$GIT_PRIVATE_KEY" > "$HOME_DIR/.ssh/id_rsa"
    chmod 600 "$HOME_DIR/.ssh/id_rsa"
    unset GIT_PRIVATE_KEY
  fi

  # GitHub known hosts
  if ! grep -q "github.com" "$HOME_DIR/.ssh/known_hosts" 2>/dev/null; then
    ssh-keyscan -t ed25519 github.com >> "$HOME_DIR/.ssh/known_hosts" 2>/dev/null || true
    chmod 600 "$HOME_DIR/.ssh/known_hosts" || true
  fi

  # Start SSH agent
  eval "$(ssh-agent -s)" 2>/dev/null || true
  ssh-add "$HOME_DIR/.ssh/id_rsa" 2>/dev/null || true

  # Configure Git identity
  git config --global user.email "ms_pod@example.com" || true
  git config --global user.name "ms_pod" || true
  git config --global --add safe.directory "$ROOT_PROJECT_PATH" || true

  # ---------------------------------------------------------------------
  # CLONE the repo using either API or SSH
  # ---------------------------------------------------------------------
  if [ "$AUTHENTICATION_METHOD" = "api" ] && [ -n "$GIT_API_TOKEN" ] && [ -n "$GIT_REPO_URL" ]; then
    echo "Using API token for Git clone..."
    REMOTE_URL=$(echo "$GIT_REPO_URL" | sed "s|https://|https://$GIT_API_TOKEN@|")
    git clone "$REMOTE_URL" "$ROOT_PROJECT_PATH" || true
  else
    echo "Using SSH-based approach..."
    if [ -n "$GIT_SSH_URL" ]; then
      git clone "$GIT_SSH_URL" "$ROOT_PROJECT_PATH" || true
    else
      echo "WARNING: GIT_SSH_URL is not set or empty. Skipping clone."
    fi
  fi

  # create default folders if not exist
  mkdir -p "$ROOT_PROJECT_PATH/dashboards"
  mkdir -p "$ROOT_PROJECT_PATH/src"
  mkdir -p "$ROOT_PROJECT_PATH/scripts"


  touch "$ROOT_PROJECT_PATH/dashboards/__init__.py"
  touch "$ROOT_PROJECT_PATH/src/__init__.py"
  touch "$ROOT_PROJECT_PATH/scripts/__init__.py"
  printf 'import mainsequence\nprint("Main Sequence running")\n' > "$ROOT_PROJECT_PATH/scripts/test_script.py"


  # ensure src/<package>/__init__.py exists for packaging
  mkdir -p "$ROOT_PROJECT_PATH/src/data_nodes"
  [ -f "$ROOT_PROJECT_PATH/src/data_nodes/__init__.py" ] || echo '__all__ = []' > "$ROOT_PROJECT_PATH/src/data_nodes/__init__.py"

  # create nodes.py from template IF MISSING (create-only)
  mkdir -p "$(dirname "$NODES_TARGET")"
  if [ ! -f "$NODES_TARGET" ]; then
    if [ -f "$NODE_TEMPLATE" ]; then
      cp -a "$NODE_TEMPLATE" "$NODES_TARGET" || echo "WARNING: copying nodes.py failed!"
    else
      echo "WARNING: node template not found: $NODE_TEMPLATE"
    fi
  fi


  mkdir -p "$ROOT_PROJECT_PATH/src/scripts"

  echo "Copying Files from mainsequence-sdk"
  cp -a "/opt/code/mainsequence-sdk/requirements.txt" "${ROOT_PROJECT_PATH}/requirements.txt" || echo "WARNING: Copy requirements step failed!"

  echo "Adding/Updating .gitignore..."
  echo ".ipynb_checkpoints" > "$ROOT_PROJECT_PATH/.gitignore"
  echo ".env" >> "$ROOT_PROJECT_PATH/.gitignore"
  echo ".venv" >> "$ROOT_PROJECT_PATH/.gitignore"

  # Render root files from templates (create-only by default)
  PROJECT_NAME="$PROJECT_NAME" ensure_file_from_template "$PYPROJECT_TEMPLATE" "$ROOT_PROJECT_PATH/pyproject.toml" "${OVERWRITE_TEMPLATES:-false}"
  PROJECT_NAME="$PROJECT_NAME" ensure_file_from_template "$README_TEMPLATE"    "$ROOT_PROJECT_PATH/README.md"      "${OVERWRITE_TEMPLATES:-false}"

  chown -R 1000:100 "$HOME_DIR" 2>/dev/null || true

  echo "Create initial commit New version"
  cd "$ROOT_PROJECT_PATH"
  git add -A
  git commit -am "initial commit for $TDAG_ENDPOINT"
  git push
else
  echo "File ${ROOT_PROJECT_PATH}/requirements.txt already exists. Updating repo..."

  # Fix SSH key perms if re-using SSH
  chmod 600 "$HOME_DIR/.ssh/id_rsa" 2>/dev/null || true
  mkdir -p $ROOT_PROJECT_PATH
  cd "$ROOT_PROJECT_PATH" || true
  pull_changes

  # Ensure templated files exist after pulling and commit if anything changed
  mkdir -p "$ROOT_PROJECT_PATH/src/data_nodes"
  [ -f "$ROOT_PROJECT_PATH/src/data_nodes/__init__.py" ] || echo '__all__ = []' > "$ROOT_PROJECT_PATH/src/data_nodes/__init__.py"
  PROJECT_NAME="$PROJECT_NAME" ensure_file_from_template "$PYPROJECT_TEMPLATE" "$ROOT_PROJECT_PATH/pyproject.toml" "${OVERWRITE_TEMPLATES:-false}"
  PROJECT_NAME="$PROJECT_NAME" ensure_file_from_template "$README_TEMPLATE"    "$ROOT_PROJECT_PATH/README.md"      "${OVERWRITE_TEMPLATES:-false}"
  git add -A
  if ! git diff --cached --quiet; then
    git commit -m "chore: ensure README.md and pyproject.toml from templates"
    git push || true
  fi
fi

# Install requirements if they exist
install_requirements_if_present "${ROOT_PROJECT_PATH}/requirements.txt"

# Update backend job status
update_backend_job_status "SUCCEEDED"

echo ">> PostStart script completed."
echo "======================================================"
