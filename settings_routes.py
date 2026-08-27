from datetime import datetime

from flask import flash, redirect, render_template, request, url_for
from werkzeug.security import generate_password_hash


def register_settings_routes(app, db, load_settings_from_db, save_settings_to_db, send_email, send_webhook, is_valid_email):
    @app.route("/settings", methods=["GET"])
    def settings_page():
        return render_template("settings.html", settings=load_settings_from_db(db))

    @app.route("/settings", methods=["POST"])
    def save_settings_route():
        settings = load_settings_from_db(db)

        auth_username = request.form.get("auth_username", "").strip()
        if auth_username:
            settings["auth"]["username"] = auth_username

        auth_password_input = request.form.get("auth_password", "").strip()
        if auth_password_input:
            settings["auth"]["password"] = generate_password_hash(auth_password_input)

        settings["auth"]["note"] = request.form.get("auth_note", "").strip()

        settings["smtp"]["server"] = request.form.get("smtp_server", "").strip()
        smtp_port_text = request.form.get("smtp_port", "465").strip() or "465"
        try:
            smtp_port = int(smtp_port_text)
            if smtp_port <= 0 or smtp_port > 65535:
                raise ValueError("端口范围错误")
        except ValueError:
            flash("SMTP 端口不合法，请输入 1-65535 的整数", "error")
            return redirect(url_for("settings_page"))

        settings["smtp"]["port"] = smtp_port
        settings["smtp"]["user"] = request.form.get("smtp_user", "").strip()
        smtp_password_input = request.form.get("smtp_password", "").strip()
        if smtp_password_input:
            settings["smtp"]["password"] = smtp_password_input

        sender = request.form.get("smtp_sender", "").strip()
        receiver = request.form.get("smtp_receiver", "").strip()
        if sender and not is_valid_email(sender):
            flash("SMTP 发件人邮箱格式不正确", "error")
            return redirect(url_for("settings_page"))
        if receiver and not is_valid_email(receiver):
            flash("SMTP 收件人邮箱格式不正确", "error")
            return redirect(url_for("settings_page"))
        settings["smtp"]["sender"] = sender
        settings["smtp"]["receiver"] = receiver
        settings["smtp"]["note"] = request.form.get("smtp_note", "").strip()

        settings["webhook"]["base_url"] = request.form.get("webhook_base_url", "").strip()
        settings["webhook"]["default_params"] = request.form.get("webhook_default_params", "").strip()
        settings["webhook"]["note"] = request.form.get("webhook_note", "").strip()

        save_settings_to_db(db, settings)
        flash("设置已保存", "success")
        return redirect(url_for("settings_page"))

    @app.route("/settings/test/email", methods=["POST"])
    def test_email_route():
        settings = load_settings_from_db(db)
        test_task = {
            "title": "邮件测试",
            "message": f"测试消息发送时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            "url": "",
        }
        try:
            send_email(test_task, settings, db=None)
            flash("邮件测试发送成功", "success")
        except Exception as exc:
            flash(f"邮件测试失败: {exc}", "error")
        return redirect(url_for("settings_page"))

    @app.route("/settings/test/webhook", methods=["POST"])
    def test_webhook_route():
        settings = load_settings_from_db(db)
        test_task = {
            "title": "Webhook 测试",
            "message": f"测试消息发送时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            "url": "",
        }
        try:
            send_webhook(test_task, settings)
            flash("Webhook 测试发送成功", "success")
        except Exception as exc:
            flash(f"Webhook 测试失败: {exc}", "error")
        return redirect(url_for("settings_page"))
