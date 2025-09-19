from __future__ import annotations

import asyncio
from pathlib import Path
from datetime import timedelta
from typing import Any, AsyncIterator, Iterable, Mapping, Sequence

from asgiref.sync import sync_to_async

from airflow.exceptions import AirflowException
from airflow.providers.common.sql.version_compat import BaseHook, BaseOperator
from airflow.providers.common.sql.hooks.sql import DbApiHook
from airflow.triggers.base import BaseTrigger, TriggerEvent
from airflow.utils.context import Context


class SqlConditionTrigger(BaseTrigger):
    """
    Poll a DB (provider hook resolved from conn_id) until the first cell of the first row is True-ish.

    Required:
        conn_id: Airflow connection id.

    Optional:
        sql / sql_path: Provide exactly one. If sql_path is used, the file is read each poll.
        parameters: Bind params (sequence or mapping) matching the driver paramstyle.
        poll_interval: Seconds between polls.
        true_values: Values considered True.
        hook_init_kwargs: Extra kwargs for the provider hook constructor (e.g., {"schema": "public"}).

    On success, this trigger serializes itself (classpath + kwargs), including "raw_value".
    """

    def __init__(
        self,
        *,
        conn_id: str,
        sql: str | None = None,
        sql_path: str | None = None,
        parameters: Mapping[str, Any] | Sequence[Any] | None = None,
        poll_interval: int = 60,
        true_values: Iterable[Any] = (True, 1, "1", "t", "true", "TRUE", "yes", "YES"),
        hook_init_kwargs: Mapping[str, Any] | None = None,
        raw_value: Any | None = None,
    ):
        super().__init__()
        if not (sql or sql_path) or (sql and sql_path):
            raise AirflowException("Provide exactly one of sql or sql_path.")
        self.conn_id = conn_id
        self.sql = sql
        self.sql_path = sql_path
        self.parameters = parameters
        self.poll_interval = int(poll_interval)
        self.true_values = set(true_values)
        self.hook_init_kwargs = dict(hook_init_kwargs or {})
        self.raw_value = raw_value

    def serialize(self) -> tuple[str, dict[str, Any]]:
        return (
            "include.custom_operators.custom_sql_deferrable_operator.SqlConditionTrigger",
            {
                "conn_id": self.conn_id,
                "sql": self.sql,
                "sql_path": self.sql_path,
                "parameters": self.parameters,
                "poll_interval": self.poll_interval,
                "true_values": list(self.true_values),
                "hook_init_kwargs": self.hook_init_kwargs,
                "raw_value": self.raw_value,
            },
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:
        while True:
            is_true, raw = await self._check_once()
            if is_true:
                self.raw_value = raw
                yield TriggerEvent(self.serialize())
                return
            self.log.info("Condition not met (raw=%r). Check after %s sec…", raw, self.poll_interval)
            await asyncio.sleep(self.poll_interval)

    @sync_to_async
    def _check_once(self) -> tuple[bool, Any]:
        sql_text = self.sql or Path(self.sql_path).read_text(encoding="utf-8")

        conn = BaseHook.get_connection(self.conn_id)
        provider_hook = conn.get_hook(hook_params=self.hook_init_kwargs or {})
        if not isinstance(provider_hook, DbApiHook):
            raise AirflowException(
                f"Resolved hook {provider_hook.__class__.__name__} is not a DbApiHook. "
                "Upgrade the provider or use a DB-API-based connection."
            )

        row = provider_hook.get_first(sql_text, parameters=self.parameters)
        if row is None:
            return False, None

        first = row[0]
        is_true = (first in self.true_values) or (isinstance(first, bool) and first is True)
        return bool(is_true), first


class CustomSqlSensor(BaseOperator):
    """
    Deferrable operator (via defer()) that waits for a SQL condition to become True.

    Airflow renders `sql` (including file templates and params) before `execute()` runs,
    so the trigger receives final SQL text.
    """

    template_fields: Sequence[str] = ("sql", "sql_path", "parameters")
    template_ext: Sequence[str] = (".sql",)

    ui_color = "#73deff"

    def __init__(
        self,
        *,
        conn_id: str,
        sql: str | None = None,
        sql_path: str | None = None,
        parameters: Mapping[str, Any] | Sequence[Any] | None = None,
        poll_interval: int = 60,
        timeout: int | None = None,
        true_values: Iterable[Any] = (True, 1, "1", "t", "true", "TRUE", "yes", "YES"),
        hook_init_kwargs: Mapping[str, Any] | None = None,
        **kwargs,
    ):
        super().__init__(**kwargs)

        if not (sql or sql_path) or (sql and sql_path):
            raise AirflowException("Provide exactly one of sql or sql_path.")

        self.conn_id = conn_id
        self.sql = sql
        self.sql_path = sql_path
        self.parameters = parameters
        self.poll_interval = int(poll_interval)
        self.true_values = list(true_values)
        self.hook_init_kwargs = dict(hook_init_kwargs or {})
        self._timeout = timeout

    def execute(self, context: Context):
        final_sql = self.sql
        final_sql_path = None
        if final_sql is None and self.sql_path:
            final_sql_path = self.sql_path

        self.defer(
            trigger=SqlConditionTrigger(
                conn_id=self.conn_id,
                sql=final_sql,
                sql_path=final_sql_path,
                parameters=self.parameters,
                poll_interval=self.poll_interval,
                true_values=self.true_values,
                hook_init_kwargs=self.hook_init_kwargs,
            ),
            method_name="execute_complete",
            timeout=timedelta(seconds=self._timeout) if self._timeout is not None else None,
        )

    def execute_complete(
        self,
        context: Context,
        event: tuple[str, dict[str, Any]] | dict[str, Any],
    ) -> Any:
        if isinstance(event, dict):
            payload = event
        elif isinstance(event, (list, tuple)) and len(event) == 2 and isinstance(event[1], dict):
            payload = event[1]
        else:
            raise AirflowException(f"Unexpected trigger event payload: {event!r}")

        raw_value = payload.get("raw_value")
        context["ti"].xcom_push(key="raw_value", value=raw_value)
        self.log.info("SQL condition met. raw_value=%r", raw_value)
        return payload
