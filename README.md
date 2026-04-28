# Unified Task Scheduler

This app merges email and webhook scheduled reminders into one service.
It is designed for personal use and stores tasks, execution history, sessions, and settings in a local SQLite database.

## Features

- Single scheduler for both `email` and `webhook` channels
- SQLite-based task storage with execution history
- Centralized settings management at `/settings`
- Dashboard, task CRUD, manual run, history, and monitoring pages
- Built-in email/webhook test send buttons
- Docker-ready deployment

## Data Storage

Runtime data is stored under `data/` by default:

- `data/tasks.db`: SQLite database containing:
  - `tasks`
  - `execution_history`
  - `sessions`
  - `settings`
- `data/settings.json`: legacy bootstrap file created on first startup if missing

## Pages

- `/`: dashboard
- `/tasks`: task management (create, update, delete, run once)
- `/tasks/<id>/history`: execution history
- `/monitoring`: execution monitoring overview
- `/settings`: auth / SMTP / webhook settings and test actions

## Task Fields

- `title`: notification title
- `message`: notification body
- `url`: optional link
- `cron_expression`: 5-part or 6-part cron expression
- `channel`: `email` or `webhook`
- `enabled`: task enabled status
- `tags`: optional tag list

## Local Run

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements-dev.txt
python3 app.py
```

Open: `http://localhost:8000`

## Run Tests

```bash
python3 -m pytest tests -q
```

## First Login

On first startup, the app initializes an admin account:

- Username defaults to `admin`
- Password is taken from `INITIAL_ADMIN_PASSWORD` if provided
- Otherwise, a random password is generated and written to the application log

Recommended environment variables for first boot:

- `INITIAL_ADMIN_USERNAME`
- `INITIAL_ADMIN_PASSWORD`
- `SECRET_KEY`

## Docker Build and Run

The production image installs runtime dependencies from `requirements.txt` only.
Development/test-only dependencies live in `requirements-dev.txt`.

```bash
docker build -t unified-task-app .
docker run -d \
  --name unified-task-app \
  -p 8000:8000 \
  -e SECRET_KEY=replace-this \
  -e INITIAL_ADMIN_PASSWORD=replace-this \
  -e APP_TIMEZONE=Asia/Shanghai \
  -v $(pwd)/data:/app/data \
  -v $(pwd)/logs:/app/logs \
  unified-task-app
```

After startup, configure SMTP/Webhook from `/settings`.
Do not commit real secrets or runtime data under `data/`.

## Optional Environment Variables

- `APP_TIMEZONE` (default: `Asia/Shanghai`)
- `SECRET_KEY` (recommended to set explicitly; app generates a random value if omitted)
- `INITIAL_ADMIN_USERNAME` (default: `admin`)
- `INITIAL_ADMIN_PASSWORD` (recommended for predictable first login)
- `DATA_DIR` (default: `/app/data`)
- `TASKS_DB` (default: `$DATA_DIR/tasks.db`)
- `SETTINGS_FILE` (default: `$DATA_DIR/settings.json`)
- `LOG_DIR` (default: `/app/logs`)
- `SESSION_TIMEOUT` (default: `30` minutes)
- `HOST` (default: `0.0.0.0`)
- `PORT` (default: `8000`)

## Notes

- Enabled tasks with valid cron expressions are loaded into APScheduler automatically.
- Each execution writes a success/failure record to `execution_history`.
- Session timeout is checked on authenticated requests.
