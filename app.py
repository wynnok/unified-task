import atexit
import hmac
import html
import json
import logging
import os
import re
import secrets
import smtplib
import threading
import time
from datetime import datetime, timedelta
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from logging.handlers import RotatingFileHandler
from typing import Any, Dict, List, Optional
from urllib.parse import quote, urlsplit
from zoneinfo import ZoneInfo

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
from werkzeug.security import check_password_hash, generate_password_hash


APP_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.environ.get("DATA_DIR", os.path.join(APP_DIR, "data"))
TASKS_DB = os.environ.get("TASKS_DB", os.path.join(DATA_DIR, "tasks.db"))
SETTINGS_FILE = os.environ.get("SETTINGS_FILE", os.path.join(DATA_DIR, "settings.json"))
LOG_DIR = os.environ.get("LOG_DIR", os.path.join(APP_DIR, "logs"))
TIMEZONE = os.environ.get("APP_TIMEZONE", "Asia/Shanghai")
SESSION_TIMEOUT = int(os.environ.get("SESSION_TIMEOUT", "30"))
SESSION_ACTIVITY_REFRESH_SECONDS = int(os.environ.get("SESSION_ACTIVITY_REFRESH_SECONDS", "60"))

SECURITY_HEADERS = {
    "X-Content-Type-Options": "nosniff",
    "X-Frame-Options": "DENY",
    "Referrer-Policy": "strict-origin-when-cross-origin",
    "Content-Security-Policy": (
        "default-src 'self'; "
        "script-src 'self' 'unsafe-inline'; "
        "style-src 'self' 'unsafe-inline'; "
        "img-src 'self' data:; "
        "font-src 'self' data:; "
        "connect-src 'self'; "
        "object-src 'none'; "
        "base-uri 'self'; "
        "form-action 'self'; "
        "frame-ancestors 'none'"
    ),
}


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
            "targets": [],
        },
    }


def write_json_atomic(path: str, data: Any) -> None:
    """Deprecated: settings are now stored in SQLite. Only used for initial settings.json bootstrap."""
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
            logging.warning(
                "未设置 INITIAL_ADMIN_PASSWORD，已生成随机管理员密码（请登录后立即修改）: %s",
                bootstrap_password,
            )
        defaults["auth"]["password"] = generate_password_hash(bootstrap_password)
        db.init_default_settings(defaults)
        logging.warning(
            "管理员账号已初始化: username=%s，请登录后立即修改密码。",
            defaults["auth"]["username"],
        )
        return defaults

    result = default_settings()
    for key, value in all_settings.items():
        if "." in key:
            section, field = key.split(".", 1)
            if section in result and isinstance(result[section], dict):
                try:
                    result[section][field] = json.loads(value)
                except json.JSONDecodeError:
                    result[section][field] = value

    if not result["auth"]["password"]:
        logging.error("管理员密码未设置，请通过环境变量 INITIAL_ADMIN_PASSWORD 初始化")

    _migrate_legacy_webhook_settings(result, all_settings)

    return result


def _migrate_legacy_webhook_settings(settings: Dict[str, Any], stored_keys: Dict[str, str]) -> None:
    """旧版只有单一 base_url（GET 路径风格）；首次加载时合成一个 Webhook 通道。"""
    raw_targets = stored_keys.get("webhook.targets")
    if raw_targets:
        try:
            if json.loads(raw_targets):
                return  # 已有通道配置，无需迁移
        except json.JSONDecodeError:
            pass

    legacy_base_url = str(settings["webhook"].get("base_url") or "").strip()
    if not legacy_base_url:
        settings["webhook"]["targets"] = []
        return

    default_params = str(settings["webhook"].get("default_params") or "").strip().lstrip("?")
    query_template = default_params
    query_template += ("&" if query_template else "") + "url={{url}}"

    settings["webhook"]["targets"] = [{
        "id": 1,
        "name": "默认通道",
        "method": "get",
        "url": f"{legacy_base_url.rstrip('/')}/{{{{title}}}}/{{{{content}}}}",
        "template": query_template,
        "note": "由旧版 Webhook 配置自动迁移",
    }]
    logging.info("已将旧版 Webhook 基础地址迁移为默认 Webhook 通道，可在设置页调整")


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


def expand_cron_occurrences(
    cron_expression: str,
    window_start: datetime,
    window_end: datetime,
    now: Optional[datetime] = None,
    max_occurrences: int = 400,
) -> List[datetime]:
    """展开 cron 表达式在 [window_start, window_end] 内的未来触发时刻。

    仅返回晚于 now 的触发点（日历视图只看未来）。
    """
    try:
        trigger_args = validate_cron_expression(cron_expression)
        trigger = CronTrigger(timezone=TIMEZONE, **trigger_args)
    except ValueError:
        return []

    effective_start = max(window_start, now) if now else window_start
    if effective_start > window_end:
        return []

    occurrence = trigger.get_next_fire_time(None, effective_start)
    results: List[datetime] = []
    while (
        occurrence is not None
        and occurrence <= window_end
        and len(results) < max_occurrences
    ):
        results.append(occurrence)
        occurrence = trigger.get_next_fire_time(occurrence, occurrence)
    return results


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
        # 执行记录在发送后写入，占位符展示本次发送序号。
        return str(count + 1)

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


EMAIL_RE = re.compile(r"^[^\s@]+@[^\s@]+\.[^\s@]+$")


def is_valid_email(email: str) -> bool:
    return bool(EMAIL_RE.match(email))


def is_safe_url(url: str) -> bool:
    if not url:
        return True
    parsed = urlsplit(url)
    return parsed.scheme in {"http", "https"} and bool(parsed.netloc)


def safe_next_path(next_value: str) -> Optional[str]:
    next_path = (next_value or "").strip()
    if not next_path:
        return None
    if "\\" in next_path:
        return None
    parsed = urlsplit(next_path)
    if parsed.scheme or parsed.netloc or not parsed.path.startswith("/"):
        return None
    return next_path


def _tokens_match(a: str, b: str) -> bool:
    return hmac.compare_digest(a.encode("utf-8"), b.encode("utf-8"))


def _looks_hashed(value: str) -> bool:
    return value.startswith(("pbkdf2:", "scrypt:", "werkzeug:"))


def verify_password(stored: str, provided: str) -> bool:
    if not stored:
        return False
    if _looks_hashed(stored):
        return check_password_hash(stored, provided)
    return _tokens_match(stored, provided)


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

    server = smtplib.SMTP_SSL(smtp["server"], int(smtp["port"]), timeout=10)
    try:
        server.login(smtp["user"], smtp["password"])
        server.sendmail(smtp["sender"], smtp["receiver"], message.as_string())
    finally:
        # sendmail 正常返回即代表服务器已接收邮件（250）；126 投出后会直接断开
        # 连接，会话收尾(QUIT)失败只影响连接释放，不能推翻一次成功的投递。
        try:
            server.quit()
        except Exception:
            server.close()


WEBHOOK_METHODS = ("get", "post_json", "post_form")

WEBHOOK_PLACEHOLDER_PATTERN = re.compile(r"\{\{(\w+)\}\}")

_WEBHOOK_SAMPLE_VARS = {
    "title": "样例标题",
    "content": "样例内容",
    "url": "https://example.com/detail",
    "time": "2026-01-01 09:00:00",
}


def build_webhook_vars(task: Dict[str, Any]) -> Dict[str, str]:
    return {
        "title": str(task.get("title", "") or ""),
        "content": str(task.get("message", "") or ""),
        "url": str(task.get("url", "") or ""),
        "time": now_text(),
    }


def _render_webhook_text(template: str, vars: Dict[str, str], encode_value) -> str:
    """替换模板中的 {{key}} 占位符；未识别的占位符原样保留，便于排查拼写问题。"""
    def replace_placeholder(match: re.Match) -> str:
        key = match.group(1)
        if key not in vars:
            return match.group(0)
        return encode_value(vars[key])

    return WEBHOOK_PLACEHOLDER_PATTERN.sub(replace_placeholder, template)


def render_webhook_url(url: str, vars: Dict[str, str]) -> str:
    """渲染 GET 请求地址，占位符值按 URL 编码（支持路径风格模板）。"""
    return _render_webhook_text(url, vars, lambda value: quote(value, safe=""))


def render_webhook_query(template: str, vars: Dict[str, str]) -> str:
    """渲染 GET 查询参数模板，占位符值按 URL 编码。"""
    return _render_webhook_text(template, vars, lambda value: quote(value, safe=""))


def render_webhook_form_body(template: str, vars: Dict[str, str]) -> str:
    """渲染表单编码请求体，占位符值按 URL 编码。"""
    return _render_webhook_text(template, vars, lambda value: quote(value, safe=""))


def render_webhook_json_body(template: str, vars: Dict[str, str]) -> str:
    """渲染 JSON 请求体模板：占位符值先做 JSON 字符串转义再嵌入，渲染结果必须可被解析。"""
    if not template.strip():
        raise RuntimeError("JSON 请求体模板不能为空")
    body = _render_webhook_text(
        template,
        vars,
        lambda value: json.dumps(value, ensure_ascii=False)[1:-1],
    )
    try:
        json.loads(body)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"渲染后的请求体不是合法 JSON（字符串占位符需写在引号内，如 \"{{{{title}}}}\"）: {exc}")
    return body


def validate_webhook_targets(raw_targets: Any) -> tuple:
    """归一化并校验 Webhook 通道列表，返回 (targets, 错误信息或 None)。"""
    if not isinstance(raw_targets, list):
        return [], "Webhook 通道配置必须是数组"

    result = []
    for index, raw in enumerate(raw_targets):
        if not isinstance(raw, dict):
            return [], f"第 {index + 1} 个 Webhook 通道配置不合法"
        name = str(raw.get("name", "") or "").strip() or f"通道 {index + 1}"
        method = str(raw.get("method", "") or "get").strip()
        if method not in WEBHOOK_METHODS:
            return [], f"通道「{name}」的请求方式不合法"
        url = str(raw.get("url", "") or "").strip()
        if not url:
            return [], f"通道「{name}」的地址不能为空"
        if not is_safe_url(url):
            return [], f"通道「{name}」的地址必须为 http 或 https"
        template = str(raw.get("template", "") or "")
        if method == "post_json":
            try:
                render_webhook_json_body(template, _WEBHOOK_SAMPLE_VARS)
            except RuntimeError as exc:
                return [], f"通道「{name}」的 JSON 模板无效: {exc}"
        note = str(raw.get("note", "") or "").strip()
        raw_id = raw.get("id")
        target_id = raw_id if isinstance(raw_id, int) and raw_id > 0 else None
        result.append({
            "id": target_id,
            "name": name,
            "method": method,
            "url": url,
            "template": template,
            "note": note,
        })

    existing_ids = [target["id"] for target in result if target["id"] is not None]
    next_id = max(existing_ids, default=0) + 1
    for target in result:
        if target["id"] is None:
            target["id"] = next_id
            next_id += 1
    return result, None


def _resolve_webhook_target(task: Dict[str, Any], targets: List[Dict[str, Any]]) -> Dict[str, Any]:
    """按任务的 webhook_id 找通道；未指定或已失效时回退到第一个通道。"""
    webhook_id = task.get("webhook_id")
    for target in targets:
        if webhook_id is not None and target.get("id") == webhook_id:
            return target
    logging.info(
        "Webhook target %s not found, falling back to the first channel",
        webhook_id,
    )
    return targets[0]


def send_webhook(task: Dict[str, Any], settings: Dict[str, Any]) -> None:
    targets = (settings.get("webhook") or {}).get("targets") or []
    if not targets:
        raise RuntimeError("Webhook 通道未配置，请先在设置页添加")

    target = _resolve_webhook_target(task, targets)
    url = str(target.get("url", "") or "").strip()
    if not url:
        raise RuntimeError(f"Webhook 通道「{target.get('name')}」的地址未配置")
    if not is_safe_url(url):
        raise RuntimeError("Webhook 地址必须为 http 或 https")

    method = target.get("method") or "get"
    template = str(target.get("template", "") or "")
    vars = build_webhook_vars(task)

    if method == "get":
        final_url = render_webhook_url(url, vars)
        query = render_webhook_query(template, vars)
        if query:
            final_url += ("&" if "?" in final_url else "?") + query
        response = requests.get(final_url, timeout=10)
    elif method == "post_json":
        body = render_webhook_json_body(template, vars)
        response = requests.post(
            url,
            data=body.encode("utf-8"),
            headers={"Content-Type": "application/json; charset=utf-8"},
            timeout=10,
        )
    elif method == "post_form":
        body = render_webhook_form_body(template, vars)
        response = requests.post(
            url,
            data=body.encode("utf-8"),
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            timeout=10,
        )
    else:
        raise RuntimeError(f"不支持的 Webhook 请求方式: {method}")

    if not 200 <= response.status_code < 300:
        raise RuntimeError(f"Webhook failed with status={response.status_code}")


# ---- 投递韧性（并发闸门 + 瞬时超时重试）----
# 所有 email 任务常在同一整点同时触发；一秒内向同一 SMTP 服务器发起 10+ 个
# 并发 TLS 连接容易触发其反垃圾限流，表现为握手/读取停滞直至超时
# （"The handshake operation timed out"、"Connection unexpectedly closed:
# The read operation timed out"）。用信号量把实际发送并发压到上限以内。
_MAX_CONCURRENT_CHANNEL_SENDS = 2
_channel_send_gate = threading.Semaphore(_MAX_CONCURRENT_CHANNEL_SENDS)

# 派发发送的总尝试次数与瞬时异常重试间隔。重试仅针对超时/连接类瞬时异常；
# 若首次发送实际已投出但对端响应超时，重试可能造成重复提醒——对个人提醒
# 场景，这比整次提醒丢失更可接受。
SEND_ATTEMPTS = 2
_SEND_RETRY_DELAY_SECONDS = 3.0

_TRANSIENT_SEND_ERRORS = (
    smtplib.SMTPServerDisconnected,
    smtplib.SMTPConnectError,
    # OSError 覆盖 TimeoutError / ssl.SSLError / ConnectionError / gaierror
    # 以及 errno 101 ENETUNREACH 等全部网络层瞬时错误（线上实测 101 是裸
    # OSError，不在 ConnectionError 覆盖范围内；requests 的
    # Timeout/ConnectionError 也继承自 OSError）。
    OSError,
)


def backfill_webhook_task_targets(db: Database) -> None:
    """仅有一个 Webhook 通道时，把未指定或指向失效通道的 webhook 任务回填到该通道。

    旧版任务没有 webhook_id，导入的任务可能引用已删除的通道；回填保证
    列表页的启停开关和定时发送继续可用。
    """
    targets = load_settings_from_db(db)["webhook"].get("targets") or []
    if len(targets) != 1:
        return
    updated = db.backfill_webhook_task_targets(
        targets[0]["id"],
        [target["id"] for target in targets],
    )
    if updated:
        logging.info("已回填 %d 个 Webhook 任务到通道「%s」", updated, targets[0]["name"])


def create_app() -> Flask:
    app = Flask(__name__)
    secret_key = os.environ.get("SECRET_KEY", "").strip()
    if not secret_key:
        secret_key = secrets.token_urlsafe(32)
        logging.warning("未设置 SECRET_KEY，已使用临时随机值。")
    app.config.update(
        SECRET_KEY=secret_key,
        SESSION_COOKIE_HTTPONLY=True,
        SESSION_COOKIE_SAMESITE="Lax",
        SESSION_COOKIE_SECURE=os.environ.get("SESSION_COOKIE_SECURE", "").lower()
        in {"1", "true", "yes"},
    )

    setup_logging()
    ensure_data_files()

    db = Database(TASKS_DB)
    scheduler = BackgroundScheduler(timezone=TIMEZONE)

    @app.after_request
    def add_security_headers(response):
        for header, value in SECURITY_HEADERS.items():
            response.headers.setdefault(header, value)
        return response

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

        app_timezone = ZoneInfo(TIMEZONE)
        last_activity = datetime.strptime(
            db_session["last_activity"], "%Y-%m-%d %H:%M:%S"
        ).replace(tzinfo=app_timezone)
        now = datetime.now(app_timezone)
        if now - last_activity > timedelta(minutes=SESSION_TIMEOUT):
            session.clear()
            flash("会话已超时，请重新登录", "warning")
            return redirect(url_for("login"))

        if now - last_activity > timedelta(seconds=SESSION_ACTIVITY_REFRESH_SECONDS):
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
        if not form_token or not session_token or not _tokens_match(form_token, session_token):
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
            last_error: Optional[BaseException] = None
            for attempt in range(1, SEND_ATTEMPTS + 1):
                try:
                    with _channel_send_gate:
                        if channel == "email":
                            send_email(task, settings, db=db)
                        elif channel == "webhook":
                            send_webhook(task, settings)
                        else:
                            raise RuntimeError(f"Unsupported channel: {channel}")
                    last_error = None
                    break
                except _TRANSIENT_SEND_ERRORS as exc:
                    last_error = exc
                    if attempt < SEND_ATTEMPTS:
                        logging.warning(
                            "Task id=%s transient send failure (attempt %d/%d), retrying: %s",
                            task_id, attempt, SEND_ATTEMPTS, exc,
                        )
                        time.sleep(_SEND_RETRY_DELAY_SECONDS)
            if last_error is None:
                set_task_runtime(task_id, "success", None)
                logging.info("Task id=%s completed", task_id)
            else:
                set_task_runtime(task_id, "failed", str(last_error))
                logging.error(
                    "Task id=%s failed after %d attempts: %s",
                    task_id, SEND_ATTEMPTS, last_error, exc_info=last_error,
                )
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
        try:
            scheduler.remove_job(task_job_id(task_id))
        except JobLookupError:
            pass

    def sync_all_jobs() -> None:
        scheduler.remove_all_jobs()
        for task in db.get_all_tasks():
            sync_task_job(task)

    def stats_data(tasks: List[Dict[str, Any]]) -> Dict[str, Any]:
        exec_stats = db.get_statistics(7, timezone_name=TIMEZONE)
        return {
            "total": len(tasks),
            "enabled": len([task for task in tasks if task.get("enabled", True)]),
            "email_count": len([task for task in tasks if task.get("channel") == "email"]),
            "webhook_count": len([task for task in tasks if task.get("channel") == "webhook"]),
            "success_count": exec_stats.get("success_count", 0),
            "failed_count": exec_stats.get("failed_count", 0),
            "total_executions": exec_stats.get("total_executions", 0),
        }

    def resolve_form_webhook_id(webhook_id_text: str) -> int:
        """校验表单提交的 Webhook 通道；未选或失效时，仅有一个通道则自动采用。"""
        targets = load_settings_from_db(db)["webhook"].get("targets") or []
        if not targets:
            raise ValueError("请先在设置页配置 Webhook 通道")

        if webhook_id_text.isdigit():
            webhook_id = int(webhook_id_text)
            if any(target.get("id") == webhook_id for target in targets):
                return webhook_id

        if len(targets) == 1:
            return targets[0]["id"]
        raise ValueError("请选择有效的 Webhook 通道")

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
        if url and not is_safe_url(url):
            raise ValueError("URL 不合法")
        if not group_id_text.isdigit():
            raise ValueError("请选择任务分组")

        group_id = int(group_id_text)
        if db.get_group_by_id(group_id) is None:
            raise ValueError("所选分组不存在")

        validate_cron_expression(cron_expression)

        webhook_id = None
        if channel == "webhook":
            webhook_id = resolve_form_webhook_id(request.form.get("webhook_id", "").strip())

        return {
            "title": title,
            "message": message,
            "url": url,
            "cron_expression": cron_expression,
            "channel": channel,
            "enabled": enabled,
            "tags": [t.strip() for t in tags.split(",") if t.strip()],
            "group_id": group_id,
            "webhook_id": webhook_id,
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

        if username == config_username and verify_password(config_password, password):
            if not _looks_hashed(config_password):
                db.set_setting("auth.password", json.dumps(generate_password_hash(password)))
            session_id = secrets.token_urlsafe(32)
            session["authenticated"] = True
            session["auth_user"] = username
            session["session_id"] = session_id
            db.create_session(session_id, username)
            db.delete_expired_sessions(SESSION_TIMEOUT)
            next_path = safe_next_path(request.args.get("next", ""))
            if not next_path:
                next_path = url_for("dashboard")
            return redirect(next_path)

        flash("账号或密码错误", "error")
        return render_template("login.html")

    @app.route("/logout", methods=["POST"])
    def logout():
        session_id = session.get("session_id")
        session.pop("authenticated", None)
        session.pop("auth_user", None)
        if session_id:
            db.delete_session(session_id)
        flash("已退出登录", "success")
        return redirect(url_for("login"))

    register_task_routes(
        app=app,
        db=db,
        scheduler=scheduler,
        timezone=TIMEZONE,
        get_next_run_time=get_next_run_time,
        expand_cron_occurrences=expand_cron_occurrences,
        parse_task_form=parse_task_form,
        find_task=find_task,
        sync_task_job=sync_task_job,
        remove_task_job=remove_task_job,
        sync_all_jobs=sync_all_jobs,
        dispatch_task=dispatch_task,
        stats_data=stats_data,
        get_webhook_targets=lambda: (load_settings_from_db(db).get("webhook") or {}).get("targets") or [],
    )
    register_settings_routes(
        app=app,
        db=db,
        load_settings_from_db=load_settings_from_db,
        save_settings_to_db=save_settings_to_db,
        send_email=send_email,
        send_webhook=send_webhook,
        is_valid_email=is_valid_email,
        validate_webhook_targets=validate_webhook_targets,
    )
    register_group_routes(app=app, db=db)

    backfill_webhook_task_targets(db)
    sync_all_jobs()
    scheduler.start()
    atexit.register(
        lambda: scheduler.shutdown(wait=False) if scheduler.running else None
    )

    return app


application = create_app()


if __name__ == "__main__":
    host = os.environ.get("HOST", "0.0.0.0")
    port = int(os.environ.get("PORT", "8000"))
    application.run(host=host, port=port)
