from __future__ import annotations
from typing import Iterable, Sequence, Union, Any, Dict
from airflow.sdk import BaseNotifier
# from airflow.providers.smtp.hooks.smtp import SmtpHook
import ssl, smtplib
from email.mime.text import MIMEText
from email.utils import formataddr
from airflow.sdk import BaseNotifier
from airflow.hooks.base import BaseHook


TEMPLATE_INCLUDE = "{% include 'airflow_task_notification.html' %}"

def get_subject(line: str) -> str:
    return "[Airflow] {{ dag.dag_id }} " + line + " | {{ dag_run.run_id if dag_run is defined else 'run' }}"

# class CustomEmailBaseNotifier(BaseNotifier):
#     template_fields: Sequence[str] = ("subject", "html_content", "to")

#     def __init__(self, to: Union[str, Iterable[str]], subject_line: str):
#         super().__init__()
#         self.to = to
#         self.subject = get_subject(subject_line)
#         self.html_content = TEMPLATE_INCLUDE

#     def notify(self, context: Dict[str, Any]):
#         recipients = [self.to] if isinstance(self.to, str) else list(self.to)
#         with SmtpHook() as hook:
#             hook.send_email_smtp(
#                 to=recipients,
#                 subject=self.subject,
#                 html_content=self.html_content,
#                 mime_subtype="html",
#             )

class CustomEmailBaseNotifier(BaseNotifier):
    template_fields: Sequence[str] = ("subject", "html_content", "to")

    def __init__(self, to: Union[str, Iterable[str]], subject_line: str, smtp_conn_id: str = "smtp_default"):
        super().__init__()
        self.to = to
        self.subject = get_subject(subject_line)
        self.html_content = TEMPLATE_INCLUDE
        self.smtp_conn_id = smtp_conn_id

    def notify(self, context: Dict[str, Any]):
        recipients = [self.to] if isinstance(self.to, str) else list(self.to)

        subject = self.subject
        body_html = self.html_content

        conn = BaseHook.get_connection(self.smtp_conn_id)
        host = conn.host
        port = int(conn.port or 587)
        user = conn.login or ""
        password = conn.password or ""
        from_email = conn.extra_dejson.get("from_email") or conn.login or "no-reply@example.com"

        msg = MIMEText(body_html, "html", "utf-8")
        msg["Subject"] = subject
        msg["From"] = formataddr(("Airflow", from_email))
        msg["To"] = ", ".join(recipients)

        context_tls = ssl.create_default_context()
        with smtplib.SMTP(host, port, timeout=30) as server:
            server.ehlo()
            if port == 587:
                server.starttls(context=context_tls)
                server.ehlo()
            if user or password:
                server.login(user, password)
            server.sendmail(from_email, recipients, msg.as_string())

class CustomSuccessNotifier(CustomEmailBaseNotifier):
    def __init__(self, to: Union[str, Iterable[str]]):
        super().__init__(to=to, subject_line="Succeeded")


class CustomFailureNotifier(CustomEmailBaseNotifier):
    def __init__(self, to: Union[str, Iterable[str]]):
        super().__init__(to=to, subject_line="Failed")


class CustomRetryNotifier(CustomEmailBaseNotifier):
    def __init__(self, to: Union[str, Iterable[str]]):
        super().__init__(to=to, subject_line="Retrying")
