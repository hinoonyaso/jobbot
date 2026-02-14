import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Any, Dict


def send_email(cfg: Dict[str, Any], html: str, subject: str, logger) -> bool:
    enable_send = bool(cfg.get("enable_send", False))
    sender = cfg.get("sender", "")
    recipients = cfg.get("recipients", [])

    if not enable_send:
        logger.info("dry-run mode: email skipped")
        return False

    if not sender or not recipients:
        logger.error("email sender/recipients missing")
        return False

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = sender
    msg["To"] = ", ".join(recipients)
    msg.attach(MIMEText(html, "html", "utf-8"))

    host = cfg.get("smtp_host", "smtp.gmail.com")
    port = int(cfg.get("smtp_port", 587))
    password = cfg.get("app_password", "")

    if not password:
        logger.error("email app_password missing")
        return False

    with smtplib.SMTP(host, port, timeout=20) as server:
        server.starttls()
        server.login(sender, password)
        server.sendmail(sender, recipients, msg.as_string())
    logger.info("email sent recipients=%d", len(recipients))
    return True
