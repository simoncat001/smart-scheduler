# Smart Scheduler development

- Backend: `backend/app/` (FastAPI/services/workflows), `backend/mat_model/`, `backend/tests/`.
- Frontend: `ui/src/`; build with `cd ui && npm run build`.
- Start with the files relevant to the request. Use `rg` for text/file searches and Serena symbol tools for Python definitions and references; read only the needed symbols.
- Use Context7, when available, to verify library APIs or version-specific behavior; match the installed version in the dependency manifests or lockfile.
- The root `.ignore` excludes dependencies, caches, generated output and XRD data even without a Git repository. Do not use `rg --no-ignore` or follow directory symlinks for routine searches. Read data files explicitly when a task needs them.
- External local storage: `/Users/liye/.local/share/smart_scheduler`. `ui/node_modules`, `ui/dist`, `runtime`, model outputs and cache directories are symlinks there; keep these links intact. Archived files and migration hashes are recorded in `archives/` and `latest-cleanup.json`.
- The old `backend/.venv` was a broken Linux environment and is archived. On this Mac use `/opt/anaconda3/envs/dev/bin/python`; if an isolated environment is needed, create it outside the project under the external storage directory.
- Run Python with `PYTHONPYCACHEPREFIX=/Users/liye/.local/share/smart_scheduler/cache/python` and `MPLCONFIGDIR=/Users/liye/.local/share/smart_scheduler/cache/matplotlib`.
- Backend tests: `cd backend && PYTHONPYCACHEPREFIX=/Users/liye/.local/share/smart_scheduler/cache/python MPLCONFIGDIR=/Users/liye/.local/share/smart_scheduler/cache/matplotlib /opt/anaconda3/envs/dev/bin/python -m pytest -o cache_dir=/Users/liye/.local/share/smart_scheduler/cache/pytest`.
- Run the tests relevant to the change. Use the full suite when changes cross backend boundaries; UI-only changes need a frontend build.
- Keep `.env`, database files, model/data inputs and credentials private. Tests must use isolated databases and mocked external services; do not start equipment or real experiments as a development check.
- Consult `docs/workflow-foundation.md`, `docs/kafka-workflows.md`, `docs/workflow-scheduling.md`, or `docs/workflow-api.md` only when the task concerns that area.
