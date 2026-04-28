import io
import json
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional

from flask import flash, jsonify, redirect, render_template, request, send_file, url_for


def register_task_routes(
    app,
    db,
    scheduler,
    timezone: str,
    get_next_run_time: Callable[[str], Optional[str]],
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
        tasks_sorted = sorted(tasks, key=lambda item: item.get("updated_at", ""), reverse=True)
        return render_template(
            "dashboard.html",
            stats=stats_data(tasks),
            recent=tasks_sorted[:8],
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
        stats = db.get_statistics(30)
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
        days = request.args.get("days", 7, type=int)
        stats = db.get_statistics(days)
        return jsonify(stats)
