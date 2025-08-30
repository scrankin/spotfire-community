# Spotfire Community

Spotfire Community is a Python package for working with the Spotfire Library REST API (v2) and DXP files.

It also includes an Automation Services client for starting and monitoring jobs.

## Installation

Install from PyPI (recommended):

Using pip:

```sh
pip install spotfire-community
```

Using uv:

```sh
uv add spotfire-community
```

Python 3.10+ is required (see `pyproject.toml`).

## Usage

### Library Client

Upload, create, and delete items in the Spotfire Library:

```python
from spotfire_community import LibraryClient
from spotfire_community.library.models import ItemType

client = LibraryClient(
	spotfire_url="https://your-spotfire-host",  # e.g., https://dev.spotfire.com
	client_id="YOUR_CLIENT_ID",
	client_secret="YOUR_CLIENT_SECRET",
)

file_id = client.upload_file(
	data=b"...bytes...",            # upload raw content
	path="/Samples/Doc1",           # library path
	item_type=ItemType.DXP,
	description="Uploaded via LibraryClient",
	overwrite=False,
)
print("uploaded:", file_id)
```

### Automation Services Client

Start and monitor Automation Services jobs:

```python
from spotfire_community.automation_services import (
	AutomationServicesClient,
	JobDefinition,
	OpenAnalysisTask,
)

client = AutomationServicesClient(
	spotfire_url="https://your-spotfire-host",
	client_id="YOUR_CLIENT_ID",
	client_secret="YOUR_CLIENT_SECRET",
)

# Build a minimal job definition
job_def = JobDefinition()
job_def.add_task(OpenAnalysisTask(path="/Samples/Analysis.dxp"))

# Start and wait for completion
status = client.start_job_definition_and_wait(job_def, poll_interval=1, timeout=120)
print("status:", status)
```

### DXP Utilities

Inspect and repackage DXP files:

```python
from spotfire_community import Dxp


print(dxp.get_all_files())
zip_bytes = dxp.get_zip_folder_in_memory()  # BytesIO
```

---

## Development

For development and testing (requires [uv](https://github.com/astral-sh/uv)):

```sh
uv sync --dev
```

### Mock APIs and Tests

The repo includes a FastAPI mock server for deterministic tests (not included in the PyPI package):
- Router and handlers: `src/mock_spotfire/library_v2/paths.py`
- Models: `src/mock_spotfire/library_v2/models.py`
- Errors: `src/mock_spotfire/library_v2/errors.py`
- In-memory state: `src/mock_spotfire/library_v2/state.py`

Automation Services (v1) mock:
- Router and handlers: `src/mock_spotfire/automation_services_v1/paths.py`
- Models: `src/mock_spotfire/automation_services_v1/models.py`
- Errors: `src/mock_spotfire/automation_services_v1/errors.py`
- In-memory state: `src/mock_spotfire/automation_services_v1/state.py`

Endpoints:
- Core: `POST /spotfire/oauth2/token`
- Library v2: `GET/POST /spotfire/api/rest/library/v2/items`
- Library v2: `DELETE /spotfire/api/rest/library/v2/items/{id}`
- Library v2: `POST /spotfire/api/rest/library/v2/upload`
- Library v2: `POST /spotfire/api/rest/library/v2/upload/{jobId}`
- Automation Services v1: `GET /spotfire/api/rest/as/job/status/{job_id}`
- Automation Services v1: `POST /spotfire/api/rest/as/job/abort/{job_id}`
- Automation Services v1: `POST /spotfire/api/rest/as/job/start-content`
- Automation Services v1: `POST /spotfire/api/rest/as/job/start-library`

Tests use `fastapi.testclient.TestClient` and monkeypatch `requests.Session` so the client talks to the mock app in‑memory. See `tests/library/upload/*.py`.

Run tests:

```sh
uv run -m pytest -q
```

### Dev Container (VS Code)

This repo ships a devcontainer for a consistent environment (Debian 12 + Python 3.13 + uv).

What you get:
- Python 3.13 base image
- uv installed by post-create script
- `.venv` virtual environment managed by uv
- Extensions: Python, TOML

Open it:
1. Install “Dev Containers” in VS Code.
2. Open the repo folder and choose “Reopen in Container”.
3. Wait for the post-create to finish; then run:

```sh
uv sync --dev
uvx pyright
uvx ruff format --check .
uv run -m pytest -q
```

Notes:
- The container uses the `vscode` user and exposes `WORKSPACE_PATH`.
- After changing dependencies, run `uv sync` again.
- If `uv` is not found, ensure it’s on PATH or re-run the post-create.

### CI

GitHub Actions runs lint, type‑check, and tests. See `.github/workflows/ci.yaml`.

## License

Open source. See `LICENSE`.
