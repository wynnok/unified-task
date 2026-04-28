import atexit
import html
import json
import logging
import os
import re
import secrets
import smtplib
import threading
from datetime import datetime, timedelta
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from logging.handlers import RotatingFileHandler
from typing import Any, Dict, List, Optional
from urllib.parse import quote

import requests
from apscheduler.jobstores.base import JobLookupError
from apscheduler.triggers.cron import CronTrigger
from apscheduler.schedulers.background import BackgroundScheduler
from flask import (
    Flask,
    abort,
    flash,
    redirect,
    render_template,
    request,
    session,
    url_for,
    jsonify,
    send_file,
)

from database import Database
from group_routes import register_group_routes
from settings_routes import register_settings_routes
from task_routes import register_task_routes


APP_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.environ.get("DATA_DIR", os.path.join(APP_DIR, "data"))
TASKS_DB = os.environ.get("TASKS_DB", os.path.join(DATA_DIR, "tasks.db"))
SETTINGS_FILE = os.environ.get("SETTINGS_FILE", os.path.join(DATA_DIR, "settings.json"))
LOG_DIR = os.environ.get("LOG_DIR", os.path.join(APP_DIR, "logs"))
TIMEZONE = os.environ.get("APP_TIMEZONE", "Asia/Shanghai")
SESSION_TIMEOUT = int(os.environ.get("SESSION_TIMEOUT", "30"))


def now_text() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def setup_logging() -> None:
    os.makedirs(LOG_DIR, exist_ok=True)
    log_file = os.path.join(LOG_DIR, "task_scheduler.log")
    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    root = logging.getLogger()
    root.setLevel(logging.INFO)

    if root.hasHandlers():
        root.handlers.clear()

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    root.addHandler(console_handler)

    file_handler = RotatingFileHandler(
        log_file,
        maxBytes=10 * 1024 * 1024,
        backupCount=5,
        encoding="utf-8",
    )
    file_handler.setFormatter(formatter)
    root.addHandler(file_handler)


def default_settings() -> Dict[str, Any]:
    return {
        "auth": {
            "username": os.environ.get("INITIAL_ADMIN_USERNAME", "admin"),
            "password": "",
            "note": "登录账号配置",
        },
        "smtp": {
            "server": "",
            "port": 465,
            "user": "",
            "password": "",
            "sender": "",
            "receiver": "",
            "note": "",
        },
        "webhook": {
            "base_url": "",
            "default_params": "",
            "note": "",
        },
    }


def write_json_atomic(path: str, data: Any) -> None:
    temp_path = f"{path}.tmp"
    with open(temp_path, "w", encoding="utf-8") as file:
        json.dump(data, file, ensure_ascii=False, indent=2)
    os.replace(temp_path, path)


def ensure_data_files() -> None:
    os.makedirs(DATA_DIR, exist_ok=True)
    if not os.path.exists(SETTINGS_FILE):
        write_json_atomic(SETTINGS_FILE, default_settings())


def load_settings_from_db(db: Database) -> Dict[str, Any]:
    """Load settings from database"""
    all_settings = db.get_all_settings()

    if not all_settings:
        defaults = default_settings()
        bootstrap_password = os.environ.get("INITIAL_ADMIN_PASSWORD", "").strip()
        if not bootstrap_password:
            bootstrap_password = secrets.token_urlsafe(12)
        defaults["auth"]["password"] = bootstrap_password
        db.init_default_settings(defaults)
        logging.warning(
            "首次初始化管理员账号: username=%s password=%s，请登录后立即修改。",
            defaults["auth"]["username"],
            bootstrap_password,
        )
        return defaults

    # Reconstruct nested structure
    result = default_settings()
    for key, value in all_settings.items():
        if "." in key:
            section, field = key.split(".", 1)
            if section in result and isinstance(result[section], dict):
                try:
                    result[section][field] = json.loads(value)
                except json.JSONDecodeError:
                    result[section][field] = value

    return result


def save_settings_to_db(db: Database, settings: Dict[str, Any]) -> None:
    """Save settings to database"""
    for section, values in settings.items():
        if isinstance(values, dict):
            for key, value in values.items():
                full_key = f"{section}.{key}"
                db.set_setting(full_key, json.dumps(value))


def parse_cron_expression(cron_expression: str) -> Dict[str, str]:
    parts = cron_expression.split()
    if len(parts) == 5:
        return {
            "minute": parts[0],
            "hour": parts[1],
            "day": parts[2],
            "month": parts[3],
            "day_of_week": parts[4],
        }
    if len(parts) == 6:
        return {
            "second": parts[0],
            "minute": parts[1],
            "hour": parts[2],
            "day": parts[3],
            "month": parts[4],
            "day_of_week": parts[5],
        }
    raise ValueError("Cron 表达式必须是 5 段或 6 段")


def validate_cron_expression(cron_expression: str) -> Dict[str, str]:
    trigger_args = parse_cron_expression(cron_expression)
    try:
        CronTrigger(timezone=TIMEZONE, **trigger_args)
    except ValueError as exc:
        raise ValueError(f"Cron 表达式无效: {exc}")
    return trigger_args


VAR_PLACEHOLDER_PATTERN = re.compile(r"\{(var[a-zA-Z0-9_]*)\}")


def render_task_message(
    task: Dict[str, Any],
    db: Optional[Database] = None,
) -> str:
    message = task.get("message", "") or ""

    def replace_placeholder(match: re.Match) -> str:
        placeholder_name = match.group(1)
        if placeholder_name != "var_monthly_count":
            logging.warning("Unknown task message placeholder preserved: %s", placeholder_name)
            return match.group(0)

        task_id = task.get("id")
        if db is None or task_id is None:
            return match.group(0)

        count = db.get_month_execution_count(int(task_id), timezone_name=TIMEZONE)
        return str(count)

    return VAR_PLACEHOLDER_PATTERN.sub(replace_placeholder, message)


def build_email_html(task: Dict[str, Any], rendered_message: str) -> str:
    title = html.escape(task.get("title", ""))
    body_html = html.escape(rendered_message).replace("\n", "<br>")
    task_url = (task.get("url") or "").strip()
    link_block = ""
    if task_url:
        escaped_url = html.escape(task_url, quote=True)
        link_block = f"""
            <div style=\"margin-top:24px;padding-top:20px;border-top:1px solid #e5e7eb;\">
              <div style=\"margin-bottom:10px;font-size:13px;color:#6b7280;word-break:break-all;\">{escaped_url}</div>
              <a href=\"{escaped_url}\" style=\"display:inline-block;padding:10px 18px;background:#2563eb;color:#ffffff;text-decoration:none;border-radius:8px;font-size:14px;\">查看详情</a>
            </div>
        """

    return f"""
<!DOCTYPE html>
<html lang=\"zh-CN\">
  <body style=\"margin:0;padding:24px;background:#f3f4f6;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;color:#111827;\">
    <div style=\"max-width:640px;margin:0 auto;background:#ffffff;border:1px solid #e5e7eb;border-radius:14px;overflow:hidden;\">
      <div style=\"padding:24px 24px 12px;font-size:22px;font-weight:600;color:#111827;\">{title}</div>
      <div style=\"padding:0 24px 24px;font-size:15px;line-height:1.8;color:#374151;\">{body_html}{link_block}</div>
    </div>
  </body>
</html>
""".strip()


def sanitize_email_subject(subject: str) -> str:
    return re.sub(r"\s+", " ", re.sub(r"[\r\n]+", " ", str(subject or ""))).strip()


def build_email_message(
    task: Dict[str, Any],
    settings: Dict[str, Any],
    db: Optional[Database] = None,
) -> MIMEMultipart:
    smtp = settings["smtp"]
    message = MIMEMultipart("alternative")
    message["From"] = sanitize_email_subject(smtp["sender"])
    message["To"] = sanitize_email_subject(smtp["receiver"])
    message["Subject"] = sanitize_email_subject(task["title"])

    rendered_message = render_task_message(task, db=db)
    body = rendered_message
    task_url = (task.get("url") or "").strip()
    if task_url:
        body += f"\n\n相关链接: {task_url}"

    message.attach(MIMEText(body, "plain", "utf-8"))
    message.attach(MIMEText(build_email_html(task, rendered_message), "html", "utf-8"))
    return message


def send_email(
    task: Dict[str, Any],
    settings: Dict[str, Any],
    db: Optional[Database] = None,
) -> None:
    smtp = settings["smtp"]
    required = [
        smtp.get("server"),
        smtp.get("port"),
        smtp.get("user"),
        smtp.get("password"),
        smtp.get("sender"),
        smtp.get("receiver"),
    ]
    if not all(required):
        raise RuntimeError("SMTP 配置不完整")

    message = build_email_message(
        task,
        settings,
        db=db,
    )

    with smtplib.SMTP_SSL(smtp["server"], int(smtp["port"])) as server:
        server.login(smtp["user"], smtp["password"])
        server.sendmail(smtp["sender"], smtp["receiver"], message.as_string())


def send_webhook(task: Dict[str, Any], settings: Dict[str, Any]) -> None:
    webhook = settings["webhook"]
    base_url = (webhook.get("base_url") or "").strip()
    if not base_url:
        raise RuntimeError("Webhook 基础地址未配置")

    encoded_title = quote(task["title"])
    encoded_message = quote(task.get("message", ""))
    final_url = f"{base_url.rstrip('/')}/{encoded_title}/{encoded_message}"

    query_parts = []
    task_url = (task.get("url") or "").strip()
    default_params = (webhook.get("default_params") or "").strip()
    if task_url:
        query_parts.append(f"url={quote(task_url)}")
    if default_params:
        query_parts.append(default_params.lstrip("?"))
    if query_parts:
        final_url = f"{final_url}?{'&'.join(query_parts)}"

    response = requests.get(final_url, timeout=10)
    if response.status_code != 200:
        raise RuntimeError(f"Webhook failed with status={response.status_code}")


def create_app() -> Flask:
    app = Flask(__name__)
    secret_key = os.environ.get("SECRET_KEY", "").strip()
    if not secret_key:
        secret_key = secrets.token_urlsafe(32)
        logging.warning("未设置 SECRET_KEY，已使用临时随机值。")
    app.config["SECRET_KEY"] = secret_key

    setup_logging()
    ensure_data_files()

    db = Database(TASKS_DB)
    scheduler = BackgroundScheduler(timezone=TIMEZONE)
    scheduler_lock = threading.Lock()

    def check_session_timeout() -> Optional[Any]:
        if not session.get("authenticated"):
            return None
        session_id = session.get("session_id")
        if not session_id:
            session.clear()
            return redirect(url_for("login"))

        db_session = db.get_session(session_id)
        if not db_session:
            session.clear()
            flash("登录状态已失效，请重新登录", "warning")
            return redirect(url_for("login"))

        last_activity = datetime.strptime(db_session["last_activity"], "%Y-%m-%d %H:%M:%S")
        if datetime.now() - last_activity > timedelta(minutes=SESSION_TIMEOUT):
            session.clear()
            flash("会话已超时，请重新登录", "warning")
            return redirect(url_for("login"))

        db.update_session_activity(session_id)
        return None

    @app.context_processor
    def inject_csrf_token() -> Dict[str, Any]:
        if "csrf_token" not in session:
            session["csrf_token"] = secrets.token_urlsafe(24)
        return {"csrf_token": session["csrf_token"]}

    @app.before_request
    def require_login() -> Optional[Any]:
        allowed_endpoints = {"login", "static"}
        endpoint = request.endpoint
        if endpoint is None or endpoint in allowed_endpoints:
            return None

        if session.get("authenticated"):
            timeout_response = check_session_timeout()
            if timeout_response is not None:
                return timeout_response
            return None

        next_path = request.path if request.path.startswith("/") else "/"
        return redirect(url_for("login", next=next_path))

    @app.before_request
    def check_csrf() -> Optional[Any]:
        if request.method != "POST":
            return None
        if request.path.startswith("/api/"):
            return None
        form_token = request.form.get("csrf_token", "")
        session_token = session.get("csrf_token", "")
        if not form_token or not session_token or form_token != session_token:
            abort(400, "CSRF token 校验失败")
        return None

    def task_job_id(task_id: int) -> str:
        return f"task_{task_id}"

    def find_task(task_id: int) -> Optional[Dict[str, Any]]:
        return db.get_task_by_id(task_id)

    def set_task_runtime(task_id: int, status: str, error: Optional[str]) -> None:
        db.add_execution_record(task_id, status, error)

    def dispatch_task(task_id: int) -> None:
        task = find_task(task_id)
        if task is None:
            logging.warning("Task %s not found, skipping", task_id)
            return
        if not task.get("enabled", True):
            logging.info("Task %s is disabled, skipping", task_id)
            return

        settings = load_settings_from_db(db)
        logging.info(
            "Running task id=%s title=%s channel=%s",
            task_id,
            task.get("title"),
            task.get("channel"),
        )
        try:
            channel = task.get("channel")
            if channel == "email":
                send_email(task, settings, db=db)
            elif channel == "webhook":
                send_webhook(task, settings)
            else:
                raise RuntimeError(f"Unsupported channel: {channel}")
            set_task_runtime(task_id, "success", None)
            logging.info("Task id=%s completed", task_id)
        except Exception as exc:
            set_task_runtime(task_id, "failed", str(exc))
            logging.exception("Task id=%s failed: %s", task_id, exc)

    def get_next_run_time(cron_expression: str) -> Optional[str]:
        try:
            trigger_args = validate_cron_expression(cron_expression)
            trigger = CronTrigger(timezone=TIMEZONE, **trigger_args)
            next_fire = trigger.get_next_fire_time(None, datetime.now())
            return next_fire.strftime("%Y-%m-%d %H:%M:%S") if next_fire else None
        except Exception:
            return None

    def sync_task_job(task: Dict[str, Any]) -> None:
        job_id = task_job_id(task["id"])
        with scheduler_lock:
            if not task.get("enabled", True):
                try:
                    scheduler.remove_job(job_id)
                except JobLookupError:
                    pass
                return
            try:
                trigger_args = validate_cron_expression(task["cron_expression"])
                scheduler.add_job(
                    dispatch_task,
                    trigger="cron",
                    id=job_id,
                    args=[task["id"]],
                    misfire_grace_time=300,
                    replace_existing=True,
                    **trigger_args,
                )
            except Exception as exc:
                try:
                    scheduler.remove_job(job_id)
                except JobLookupError:
                    pass
                logging.error(
                    "Skip task id=%s because config invalid: %s",
                    task.get("id"),
                    exc,
                )

    def remove_task_job(task_id: int) -> None:
        with scheduler_lock:
            try:
                scheduler.remove_job(task_job_id(task_id))
            except JobLookupError:
                pass

    def sync_all_jobs() -> None:
        with scheduler_lock:
            scheduler.remove_all_jobs()
        for task in db.get_all_tasks():
            sync_task_job(task)

    def stats_data(tasks: List[Dict[str, Any]]) -> Dict[str, Any]:
        exec_stats = db.get_statistics(7)
        return {
            "total": len(tasks),
            "enabled": len([task for task in tasks if task.get("enabled", True)]),
            "email_count": len([task for task in tasks if task.get("channel") == "email"]),
            "webhook_count": len([task for task in tasks if task.get("channel") == "webhook"]),
            "success_count": exec_stats.get("success_count", 0),
            "failed_count": exec_stats.get("failed_count", 0),
            "total_executions": exec_stats.get("total_executions", 0),
        }

    def apply_task_filters(
        tasks: List[Dict[str, Any]],
        q: str,
        channel: str,
        enabled: str,
        last_status: str,
        group_id: str,
    ) -> List[Dict[str, Any]]:
        result = tasks

        if q:
            keyword = q.lower()

            def matches(task: Dict[str, Any]) -> bool:
                haystack = " ".join(
                    [
                        str(task.get("title", "")),
                        str(task.get("message", "")),
                        str(task.get("url", "")),
                        str(task.get("cron_expression", "")),
                        str(task.get("group_name", "")),
                    ]
                ).lower()
                return keyword in haystack

            result = [task for task in result if matches(task)]

        if channel in {"email", "webhook"}:
            result = [task for task in result if task.get("channel") == channel]

        if enabled == "enabled":
            result = [task for task in result if task.get("enabled", True)]
        elif enabled == "disabled":
            result = [task for task in result if not task.get("enabled", True)]

        if last_status == "failed":
            result = [task for task in result if task.get("last_status") == "failed"]

        if group_id:
            result = [task for task in result if str(task.get("group_id")) == group_id]

        return result

    def parse_task_form() -> Dict[str, Any]:
        title = request.form.get("title", "").strip()
        message = request.form.get("message", "").strip()
        url = request.form.get("url", "").strip()
        cron_expression = request.form.get("cron_expression", "").strip()
        channel = request.form.get("channel", "").strip()
        group_id_text = request.form.get("group_id", "").strip()
        enabled = request.form.get("enabled") in {"1", "on", "true"}
        tags = request.form.get("tags", "").strip()

        if not title or channel not in {"email", "webhook"}:
            raise ValueError("任务输入不合法")
        if not group_id_text.isdigit():
            raise ValueError("请选择任务分组")

        group_id = int(group_id_text)
        if db.get_group_by_id(group_id) is None:
            raise ValueError("所选分组不存在")

        validate_cron_expression(cron_expression)

        return {
            "title": title,
            "message": message,
            "url": url,
            "cron_expression": cron_expression,
            "channel": channel,
            "enabled": enabled,
            "tags": [t.strip() for t in tags.split(",") if t.strip()],
            "group_id": group_id,
        }

    @app.route("/login", methods=["GET", "POST"])
    def login():
        if request.method == "GET":
            if session.get("authenticated"):
                return redirect(url_for("dashboard"))
            return render_template("login.html")

        username = request.form.get("username", "").strip()
        password = request.form.get("password", "").strip()

        auth_settings = load_settings_from_db(db)["auth"]
        config_username = str(auth_settings.get("username", "admin"))
        config_password = str(auth_settings.get("password", "")).strip()

        if not config_password:
            flash("管理员密码未初始化，请检查服务启动日志中的初始化密码", "error")
            return render_template("login.html")

        if username == config_username and password == config_password:
            session_id = secrets.token_urlsafe(32)
            session["authenticated"] = True
            session["auth_user"] = username
            session["session_id"] = session_id
            db.create_session(session_id, username)
            next_path = request.args.get("next", "").strip()
            if not next_path.startswith("/"):
                next_path = url_for("dashboard")
            return redirect(next_path)

        flash("账号或密码错误", "error")
        return render_template("login.html")

    @app.route("/logout", methods=["POST"])
    def logout():
        session.pop("authenticated", None)
        session.pop("auth_user", None)
        flash("已退出登录", "success")
        return redirect(url_for("login"))

    register_task_routes(
        app=app,
        db=db,
        scheduler=scheduler,
        timezone=TIMEZONE,
        get_next_run_time=get_next_run_time,
        apply_task_filters=apply_task_filters,
        parse_task_form=parse_task_form,
        find_task=find_task,
        sync_task_job=sync_task_job,
        remove_task_job=remove_task_job,
        sync_all_jobs=sync_all_jobs,
        dispatch_task=dispatch_task,
        stats_data=stats_data,
    )
    register_settings_routes(
        app=app,
        db=db,
        load_settings_from_db=load_settings_from_db,
        save_settings_to_db=save_settings_to_db,
        now_text=now_text,
        send_email=send_email,
        send_webhook=send_webhook,
    )
    register_group_routes(app=app, db=db)

    sync_all_jobs()
    scheduler.start()
    atexit.register(
        lambda: scheduler.shutdown(wait=False) if scheduler.running else None
    )

    return app


app = create_app()


if __name__ == "__main__":
    host = os.environ.get("HOST", "0.0.0.0")
    port = int(os.environ.get("PORT", "8000"))
    app.run(host=host, port=port)
