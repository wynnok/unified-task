# AGENTS.md

This file provides guidance to Codex (Codex.ai/code) when working with code in this repository.

## Project Overview

Unified Task Scheduler is a personal Flask application that schedules reminder tasks through email or webhook channels. It stores tasks, execution history, sessions, settings, and task groups in a local SQLite database, and uses APScheduler to run enabled tasks from 5-part or 6-part cron expressions.

## Development Commands

Python dependencies are specified with `requirements.txt` for runtime and `requirements-dev.txt` for tests/dev. No `pyproject.toml`, lockfile, dedicated lint, type-check, or formatter command is configured.

Set up a local development environment:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements-dev.txt
```

If using `uv`, keep the environment local to the project:

```bash
uv venv .venv
source .venv/bin/activate
uv pip install -r requirements-dev.txt
```

Run the app locally:

```bash
python3 app.py
```

The app listens on `http://localhost:8000` by default. Useful environment variables include `DATA_DIR`, `TASKS_DB`, `SETTINGS_FILE`, `LOG_DIR`, `APP_TIMEZONE`, `SECRET_KEY`, `INITIAL_ADMIN_USERNAME`, `INITIAL_ADMIN_PASSWORD`, `HOST`, `PORT`, and `SESSION_TIMEOUT`.

Run all tests:

```bash
python3 -m pytest tests -q
```

Run one test file:

```bash
python3 -m pytest tests/test_email_rendering.py -q
```

Run one test:

```bash
python3 -m pytest tests/test_email_rendering.py::test_render_task_message_replaces_monthly_count_with_current_send_number -q
```

Build and run with Docker:

```bash
docker build -t unified-task-app .
docker run -d --name unified-task-app -p 8000:8000 \
  -e SECRET_KEY=replace-this \
  -e INITIAL_ADMIN_PASSWORD=replace-this \
  -e APP_TIMEZONE=Asia/Shanghai \
  -v $(pwd)/data:/app/data \
  -v $(pwd)/logs:/app/logs \
  unified-task-app
```

Run with Docker Compose:

```bash
docker compose up -d
```

`docker-compose.yml` expects an external Docker network named `common_bridge_net`.

## Architecture

- `app.py` is the application factory and runtime composition root. It configures logging, data paths, session/CSRF checks, SQLite access, APScheduler, task dispatch, email/webhook senders, cron parsing, and route registration.
- `create_app()` wires dependencies into route registration functions. The route modules are not Flask Blueprints; `task_routes.py`, `settings_routes.py`, and `group_routes.py` attach routes directly to the passed app and should keep receiving app/database/scheduler helpers from `create_app()`.
- `database.py` owns SQLite schema initialization, lightweight in-place migrations, thread-local connections, transactions, and all persistence methods for groups, tasks, execution history, sessions, and settings. Startup ensures a default group named `默认`, reconciles legacy `默认分组`, and backfills invalid task group references.
- `task_routes.py` owns dashboard/task management routes, manual runs, batch enable/disable/delete, JSON import/export, monitoring, and `/api/statistics`. Task changes must keep APScheduler jobs in sync through the callbacks passed from `create_app()`.
- `settings_routes.py` owns auth/SMTP/webhook settings plus email/webhook test actions. `group_routes.py` owns group CRUD and refuses to delete groups that still have tasks.
- `templates/` contains server-rendered Jinja pages for login, dashboard, tasks, groups, task history, monitoring, and settings. `static/style.css` holds the shared UI styling; UI tests assert some template/CSS structure, so keep markup changes minimal and verified.
- `tests/conftest.py` isolates tests by setting temp `DATA_DIR`, `TASKS_DB`, `SETTINGS_FILE`, and `LOG_DIR`, and monkeypatches APScheduler start/shutdown so importing `app` does not start real background scheduling.
- `migrate_to_sqlite.py` is a legacy JSON-to-SQLite migration helper. It is interactive and may delete the target database or rename source JSON files to `.backup`, so do not run it as part of normal development or tests.

## Runtime Data and Persistence

- Default local runtime paths are `data/tasks.db`, `data/settings.json`, and `logs/task_scheduler.log`; Docker uses `/app/data` and `/app/logs` volumes.
- First startup initializes settings in the database. If `INITIAL_ADMIN_PASSWORD` is absent, a random admin password is generated and written to the application log.
- Settings are stored in the `settings` table as flattened keys such as `auth.username`, `smtp.server`, and `webhook.base_url`; `data/settings.json` is only a legacy bootstrap file created if missing.
- Enabled tasks are synced into APScheduler jobs via `sync_all_jobs()` at startup and `sync_task_job()` after task changes. Each task execution writes a row to `execution_history`.

## Testing Notes

- Prefer pytest fixtures from `tests/conftest.py` (`app_module`, `client`, `db`, `login`, `post_form`) for route and database tests.
- Route tests that post forms must include CSRF; use the `post_form` fixture unless intentionally testing CSRF behavior. Non-API POST requests without a valid session token abort with 400.
- Tests should use temporary databases through fixtures or `tmp_path`; do not depend on or mutate `data/tasks.db`, `tasks-prod.db`, or `tasks-prod-copy.db`.
- For code that imports `app`, be aware that `app = create_app()` is executed at import time; tests avoid side effects with environment monkeypatching before import.
- Database methods normalize missing or invalid `group_id` values to the default group, but HTTP task forms require an existing group ID and reject invalid submissions.
- Task import uses `Database.import_tasks()` and then `sync_all_jobs()`. Imported task group IDs are normalized to the default group when missing or invalid.
- Email rendering replaces `{var_monthly_count}` only during real task dispatch when a database and task ID are available; test email sends pass `db=None` and preserve placeholders.
