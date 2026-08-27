import io
import json
import calendar
from datetime import datetime, timedelta
from typing import Any, Callable, Dict, List, Optional
from zoneinfo import ZoneInfo

from flask import flash, jsonify, redirect, render_template, request, send_file, url_for




def register_task_routes(
    app,
    db,
    scheduler,
    timezone: str,
    get_next_run_time: Callable[[str], Optional[str]],
    expand_cron_occurrences: Callable[..., List[datetime]],
    apply_task_filters: Callable[[List[Dict[str, Any]], str, str, str, str, str], List[Dict[str, Any]]],
    parse_task_form: Callable[[], Dict[str, Any]],
    find_task: Callable[[int], Optional[Dict[str, Any]]],
    sync_task_job: Callable[[Dict[str, Any]], None],
    remove_task_job: Callable[[int], None],
    sync_all_jobs: Callable[[], None],
    dispatch_task: Callable[[int], None],
    stats_data: Callable[[List[Dict[str, Any]]], Dict[str, Any]],
):
    @app.route("/")
    def dashboard():
        tasks = db.get_all_tasks()
        return render_template(
            "dashboard.html",
            stats=stats_data(tasks),
            recent=tasks[:8],
            scheduler=scheduler,
        )

    @app.route("/tasks", methods=["GET"])
    def tasks_page():
        q = request.args.get("q", "").strip()
        channel = request.args.get("channel", "").strip()
        enabled = request.args.get("enabled", "").strip()
        last_status = request.args.get("last_status", "").strip()
        group_id = request.args.get("group_id", "").strip()

        tasks = sorted(db.get_all_tasks(), key=lambda item: item.get("id", 0), reverse=True)
        groups = db.get_all_groups()

        for task in tasks:
            if task.get("tags"):
                task["tags"] = json.loads(task["tags"]) if isinstance(task["tags"], str) else task["tags"]

        filtered_tasks = apply_task_filters(tasks, q, channel, enabled, last_status, group_id)

        tag = request.args.get("tag", "").strip()
        if tag:
            filtered_tasks = [
                task for task in filtered_tasks
                if isinstance(task.get("tags"), list) and tag in task["tags"]
            ]

        for task in filtered_tasks:
            task["next_run_time"] = get_next_run_time(task["cron_expression"])

        return render_template(
            "tasks.html",
            tasks=filtered_tasks,
            groups=groups,
            filter_q=q,
            filter_channel=channel,
            filter_enabled=enabled,
            filter_last_status=last_status,
            filter_group_id=group_id,
            filter_tag=tag,
            timezone=timezone,
        )

    @app.route("/tasks", methods=["POST"])
    def create_task_route():
        try:
            new_task = parse_task_form()
        except ValueError as exc:
            flash(str(exc), "error")
            return redirect(url_for("tasks_page"))

        task_id = db.create_task(new_task)
        created_task = find_task(task_id)
        if created_task:
            sync_task_job(created_task)

        flash("任务已创建", "success")
        return redirect(url_for("tasks_page"))

    @app.route("/tasks/<int:task_id>/update", methods=["POST"])
    def update_task(task_id: int):
        try:
            updated_task = parse_task_form()
        except ValueError as exc:
            flash(str(exc), "error")
            return redirect(url_for("tasks_page"))

        if not db.update_task(task_id, updated_task):
            flash("任务不存在", "error")
            return redirect(url_for("tasks_page"))

        sync_task_job({"id": task_id, **updated_task})
        flash("任务已更新", "success")
        return redirect(url_for("tasks_page"))

    @app.route("/tasks/<int:task_id>/delete", methods=["POST"])
    def delete_task(task_id: int):
        if not db.delete_task(task_id):
            flash("任务不存在", "error")
            return redirect(url_for("tasks_page"))

        remove_task_job(task_id)
        flash("任务已删除", "success")
        return redirect(url_for("tasks_page"))

    @app.route("/tasks/<int:task_id>/run", methods=["POST"])
    def run_task_now(task_id: int):
        if find_task(task_id) is None:
            flash("任务不存在", "error")
            return redirect(url_for("tasks_page"))

        dispatch_task(task_id)
        flash("任务已执行一次", "success")
        return redirect(url_for("tasks_page"))

    @app.route("/tasks/<int:task_id>/history", methods=["GET"])
    def task_history(task_id: int):
        task = find_task(task_id)
        if not task:
            flash("任务不存在", "error")
            return redirect(url_for("tasks_page"))

        history = db.get_execution_history(task_id, 100)
        return render_template("task_history.html", task=task, history=history)

    @app.route("/calendar")
    def calendar_page():
        zone = ZoneInfo(timezone)
        now = datetime.now(zone)

        def _month_int(name: str, default: int) -> int:
            try:
                return int(request.args.get(name, ""))
            except (TypeError, ValueError):
                return default

        year = _month_int("year", now.year)
        month = _month_int("month", now.month)
        if not 2000 <= year <= 2100:
            year = now.year
        if not 1 <= month <= 12:
            month = now.month

        window_start = datetime(year, month, 1, tzinfo=zone)
        next_year, next_month_num = (year + 1, 1) if month == 12 else (year, month + 1)
        window_end = datetime(next_year, next_month_num, 1, tzinfo=zone) - timedelta(seconds=1)

        entries = []
        overflow = False
        for task in db.get_all_tasks():
            if not task.get("enabled", True):
                continue
            for occurrence in expand_cron_occurrences(task["cron_expression"], window_start, window_end, now):
                entries.append({
                    "task_id": task["id"],
                    "title": task["title"],
                    "when": occurrence,
                    "time_text": occurrence.strftime("%H:%M"),
                })

        entries.sort(key=lambda e: e["when"])
        if len(entries) > 600:
            entries = entries[:600]
            overflow = True

        day_entries: Dict[int, List[Dict[str, Any]]] = {}
        for entry in entries:
            day_entries.setdefault(entry["when"].day, []).append(entry)

        _, last_day = calendar.monthrange(year, month)
        weeks: List[List[Optional[int]]] = []
        current_week: List[Optional[int]] = [None] * window_start.weekday()
        for day in range(1, last_day + 1):
            current_week.append(day)
            if len(current_week) == 7:
                weeks.append(current_week)
                current_week = []
        if current_week:
            weeks.append(current_week + [None] * (7 - len(current_week)))

        prev_year, prev_month = (year - 1, 12) if month == 1 else (year, month - 1)
        has_prev = prev_year >= 2000
        has_next = next_year <= 2100

        return render_template(
            "calendar.html",
            title="日历",
            year=year,
            month=month,
            weeks=weeks,
            day_entries=day_entries,
            upcoming=entries,
            overflow=overflow,
            prev_year=prev_year,
            prev_month=prev_month,
            next_year=next_year,
            next_month_num=next_month_num,
            has_prev=has_prev,
            has_next=has_next,
        )

    @app.route("/tasks/batch", methods=["POST"])
    def batch_operations():
        action = request.form.get("action", "").strip()
        task_ids_str = request.form.get("task_ids", "").strip()

        if not task_ids_str:
            flash("未选择任务", "error")
            return redirect(url_for("tasks_page"))

        try:
            task_ids = [int(tid) for tid in task_ids_str.split(",")]
        except ValueError:
            flash("任务ID格式错误", "error")
            return redirect(url_for("tasks_page"))

        if action == "enable":
            count = db.batch_update_enabled(task_ids, True)
            for task_id in task_ids:
                task = find_task(task_id)
                if task:
                    sync_task_job(task)
            flash(f"已启用 {count} 个任务", "success")
        elif action == "disable":
            count = db.batch_update_enabled(task_ids, False)
            for task_id in task_ids:
                remove_task_job(task_id)
            flash(f"已禁用 {count} 个任务", "success")
        elif action == "delete":
            count = db.batch_delete_tasks(task_ids)
            for task_id in task_ids:
                remove_task_job(task_id)
            flash(f"已删除 {count} 个任务", "success")
        else:
            flash("无效的操作", "error")

        return redirect(url_for("tasks_page"))

    @app.route("/tasks/export", methods=["GET"])
    def export_tasks():
        tasks = db.export_tasks()
        export_data = json.dumps(tasks, ensure_ascii=False, indent=2)

        buffer = io.BytesIO(export_data.encode("utf-8"))
        buffer.seek(0)

        return send_file(
            buffer,
            mimetype="application/json",
            as_attachment=True,
            download_name=f"tasks_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
        )

    @app.route("/tasks/import", methods=["POST"])
    def import_tasks():
        if "file" not in request.files:
            flash("未选择文件", "error")
            return redirect(url_for("tasks_page"))

        file = request.files["file"]
        if file.filename == "":
            flash("未选择文件", "error")
            return redirect(url_for("tasks_page"))

        try:
            content = file.read().decode("utf-8")
            tasks = json.loads(content)

            if not isinstance(tasks, list):
                flash("文件格式错误：必须是任务数组", "error")
                return redirect(url_for("tasks_page"))

            count = db.import_tasks(tasks)
            sync_all_jobs()
            flash(f"成功导入 {count} 个任务", "success")
        except json.JSONDecodeError:
            flash("文件格式错误：无效的JSON", "error")
        except Exception as e:
            flash(f"导入失败: {e}", "error")

        return redirect(url_for("tasks_page"))

    @app.route("/monitoring")
    def monitoring():
        stats = db.get_statistics(30, timezone_name=timezone)
        tasks = db.get_all_tasks()

        return render_template(
            "monitoring.html",
            stats=stats,
            task_count=len(tasks),
            enabled_count=len([t for t in tasks if t.get("enabled")]),
            never_run_count=len([t for t in tasks if not t.get("last_run_at")]),
        )

    @app.route("/api/statistics")
    def api_statistics():
        try:
            days = int(request.args.get("days", 7))
        except (TypeError, ValueError):
            days = 7
        days = max(1, min(365, days))
        stats = db.get_statistics(days, timezone_name=timezone)
        return jsonify(stats)
