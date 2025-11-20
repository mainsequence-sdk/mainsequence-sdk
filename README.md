<p align="center">
<img src="https://main-sequence.app/static/media/logos/MS_logo_long_black.png" alt="Main Sequence Logo" width="500"/>
</p>

# Main Sequence Python SDK



The Main Sequence Python SDK is a high-performance client library that enables seamless integration with the Main Sequence platform. 
It provides a unified and intuitive interface for interacting with data, compute, and intelligence services across the platform.

Main Sequence functions as a centralized engine for data intelligence—integrating information from diverse data sources and systems
while abstracting away the complexity of underlying storage layers. This allows quants, researchers, analysts,
and engineers to focus on the data-generating process itself, while Main Sequence optimizes all CRUD operations and manages 
the mapping between logical data structures and physical storage.

The Main Sequence SDK is also a foundational component of all Main Sequence Platform projects. 
It acts as the backbone for automation, process orchestration, and the rapid development of dashboards,
data nodes, and agentic tools built on top of the platform.

---

## Developing with the Main Sequence SDK & Platform
To make it easy to work on Main Sequence projects from your local environment, you have two options:

1. **Use the MainSequence CLI directly in your terminal**, or  
2. **Use the Main Sequence VS Code extension** (recommended if you already work in VS Code).

The VS Code extension provides a more visual, editor-integrated workflow on top of what the CLI offers.


### Visual Studio Code Extension

1. **Open the Extensions view in VS Code**

   - **macOS:** Press `Cmd` + `Shift` + `X`  
   - **Windows/Linux:** Press `Ctrl` + `Shift` + `X`  
   - Or click the **Extensions** icon in the Activity Bar on the left side of the window.

2. **Search for the extension**

   In the Extensions search box, type Main Sequence and press Enter.:


![img.png](/docs/img/vs_code_extension.png)

### MainSequence CLI

MainSequence CLI is a small helper tool to:

- Authenticate against the Main Sequence backend
- Manage your local project checkouts (clone, open, delete)
- Set up SSH deploy keys for project repos
- Generate and maintain a `.env` file with project-specific tokens and endpoints
- Build & run your project in Docker (via `uv` + `docker`)
- Bundle and copy AI/LLM instruction markdowns to the clipboard

The CLI is implemented with [Typer](https://typer.tiangolo.com/) and exposes a `mainsequence` command.

---

## Installation & Invocation

How you install the CLI depends on how this repository is packaged, but assuming it’s installed in your environment and provides the `mainsequence` entry point:

```bash
# General form
mainsequence --help
