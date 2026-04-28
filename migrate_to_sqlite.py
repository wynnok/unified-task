#!/usr/bin/env python3
"""Migration script to convert tasks.json and settings.json to SQLite database"""

import json
import os
import sys
from database import Database


def migrate_to_sqlite(tasks_json: str, settings_json: str, db_path: str):
    """Migrate tasks and settings from JSON files to SQLite database"""

    # Check if tasks.json exists
    if not os.path.exists(tasks_json):
        print(f"Error: {tasks_json} not found")
        sys.exit(1)

    # Check if database already exists
    if os.path.exists(db_path):
        response = input(f"{db_path} already exists. Overwrite? (yes/no): ")
        if response.lower() != "yes":
            print("Migration cancelled")
            sys.exit(0)
        os.remove(db_path)

    # Initialize database
    db = Database(db_path)
    print(f"✓ Database initialized at: {db_path}")

    # Migrate tasks
    with open(tasks_json, "r", encoding="utf-8") as f:
        tasks = json.load(f)

    print(f"\nMigrating {len(tasks)} tasks...")
    migrated_tasks = 0
    for task in tasks:
        try:
            task_data = {
                "title": task["title"],
                "message": task.get("message", ""),
                "url": task.get("url", ""),
                "cron_expression": task["cron_expression"],
                "channel": task["channel"],
                "enabled": task.get("enabled", True),
                "tags": task.get("tags", [])
            }

            task_id = db.create_task(task_data)

            # Migrate execution history if exists
            if task.get("last_run_at") and task.get("last_status"):
                db.add_execution_record(
                    task_id,
                    task["last_status"],
                    task.get("last_error")
                )

            migrated_tasks += 1
            print(f"  ✓ Task #{task['id']}: {task['title']}")

        except Exception as e:
            print(f"  ✗ Failed to migrate task #{task.get('id', '?')}: {e}")

    print(f"✓ Tasks migration complete: {migrated_tasks}/{len(tasks)}")

    # Migrate settings
    if os.path.exists(settings_json):
        print(f"\nMigrating settings from {settings_json}...")
        with open(settings_json, "r", encoding="utf-8") as f:
            settings = json.load(f)

        db.init_default_settings(settings)
        print(f"✓ Settings migrated successfully")

        # Backup settings.json
        settings_backup = f"{settings_json}.backup"
        os.rename(settings_json, settings_backup)
        print(f"✓ Original settings backed up to: {settings_backup}")
    else:
        print(f"\n⚠ {settings_json} not found, using default settings")

    # Backup tasks.json
    tasks_backup = f"{tasks_json}.backup"
    os.rename(tasks_json, tasks_backup)
    print(f"✓ Original tasks backed up to: {tasks_backup}")

    print(f"\n{'='*60}")
    print(f"✅ Migration completed successfully!")
    print(f"{'='*60}")
    print(f"Database: {db_path}")
    print(f"Tasks migrated: {migrated_tasks}")
    print(f"Backups created:")
    print(f"  - {tasks_backup}")
    if os.path.exists(settings_backup):
        print(f"  - {settings_backup}")


if __name__ == "__main__":
    APP_DIR = os.path.dirname(os.path.abspath(__file__))
    DATA_DIR = os.path.join(APP_DIR, "data")
    TASKS_JSON = os.path.join(DATA_DIR, "tasks.json")
    SETTINGS_JSON = os.path.join(DATA_DIR, "settings.json")
    TASKS_DB = os.path.join(DATA_DIR, "tasks.db")

    migrate_to_sqlite(TASKS_JSON, SETTINGS_JSON, TASKS_DB)
