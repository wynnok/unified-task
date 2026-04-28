import logging
import sqlite3
from typing import Any, Dict

from flask import flash, redirect, render_template, request, url_for


ICON_OPTIONS = [
    {"value": "folder", "label": "文件夹"},
    {"value": "briefcase", "label": "公文包"},
    {"value": "calendar", "label": "日历"},
    {"value": "bell", "label": "提醒铃"},
    {"value": "check-square", "label": "清单"},
    {"value": "chart-bar", "label": "图表"},
    {"value": "gear", "label": "齿轮"},
    {"value": "envelope", "label": "邮件"},
    {"value": "globe", "label": "地球"},
    {"value": "tag", "label": "标签"},
]
ALLOWED_GROUP_ICONS = {item["value"] for item in ICON_OPTIONS}


def is_group_name_unique_constraint_error(exc: sqlite3.IntegrityError) -> bool:
    message = str(exc).lower()
    return "unique constraint failed: groups.name" in message


def register_group_routes(app, db):
    def parse_group_form() -> Dict[str, Any]:
        sort_order_text = request.form.get("sort_order", "").strip()
        name = request.form.get("name", "").strip()
        icon = request.form.get("icon", "").strip()

        if not sort_order_text.lstrip("-").isdigit():
            raise ValueError("分组序号必须是整数")
        if not name:
            raise ValueError("分组名称不能为空")
        if icon not in ALLOWED_GROUP_ICONS:
            raise ValueError("请选择有效图标")

        return {
            "sort_order": int(sort_order_text),
            "name": name,
            "icon": icon,
        }

    @app.route("/groups", methods=["GET"])
    def groups_page():
        groups = db.get_all_groups()
        task_counts = {
            row["group_id"]: row["task_count"]
            for row in db._get_conn().execute(
                "SELECT group_id, COUNT(*) AS task_count FROM tasks GROUP BY group_id"
            ).fetchall()
        }
        for group in groups:
            group["task_count"] = task_counts.get(group["id"], 0)
        return render_template("groups.html", groups=groups, icon_options=ICON_OPTIONS)

    @app.route("/groups", methods=["POST"])
    def create_group_route():
        try:
            group = parse_group_form()
            db.create_group(group)
            flash("分组已创建", "success")
        except ValueError as exc:
            flash(str(exc), "error")
        except sqlite3.IntegrityError as exc:
            if is_group_name_unique_constraint_error(exc):
                flash("分组名称已存在", "error")
            else:
                logging.exception("创建分组失败")
                flash("分组创建失败，请稍后重试", "error")
        return redirect(url_for("groups_page"))

    @app.route("/groups/<int:group_id>/update", methods=["POST"])
    def update_group_route(group_id: int):
        try:
            group = parse_group_form()
            if not db.update_group(group_id, group):
                flash("分组不存在", "error")
            else:
                flash("分组已更新", "success")
        except ValueError as exc:
            flash(str(exc), "error")
        except sqlite3.IntegrityError as exc:
            if is_group_name_unique_constraint_error(exc):
                flash("分组名称已存在", "error")
            else:
                logging.exception("更新分组失败")
                flash("分组更新失败，请稍后重试", "error")
        return redirect(url_for("groups_page"))

    @app.route("/groups/<int:group_id>/delete", methods=["POST"])
    def delete_group_route(group_id: int):
        task_count = db.count_tasks_by_group(group_id)
        if task_count > 0:
            flash(f"该分组下仍有 {task_count} 个任务，无法删除", "error")
            return redirect(url_for("groups_page"))

        if not db.delete_group(group_id):
            flash("分组不存在", "error")
            return redirect(url_for("groups_page"))

        flash("分组已删除", "success")
        return redirect(url_for("groups_page"))
