from __future__ import annotations

from airflow.sdk import dag, task
from airflow.providers.amazon.aws.hooks.secrets_manager import SecretsManagerHook
from airflow.providers.smtp.notifications.smtp import SmtpNotifier

from include.custom_email_notifier.custom_email_notifier import CustomFailureNotifier

failure_email = CustomFailureNotifier(to="astro-alerts@tradingtechnologies.com")
DEFAULT_ARGS = {
    "on_failure_callback": failure_email,
}

SECRET_ID  = "ses-user-astro"
AWS_CONN_ID = "dna_int_aws_conn"

@dag(
     default_args=DEFAULT_ARGS,
     template_searchpath=["/usr/local/airflow/include/custom_email_notifier"],
     tags=["DNA", "TEST DAG"],
)
def print_aws_secret_dag():
    @task
    def fetch_and_print_secret() -> None:
        sm_hook = SecretsManagerHook(aws_conn_id=AWS_CONN_ID)
        secret = sm_hook.get_secret(SECRET_ID)
        print(f"Fetched secret: {secret}")

    fetch_and_print_secret()

print_aws_secret_dag()
