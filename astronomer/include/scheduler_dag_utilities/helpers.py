from __future__ import annotations

import json
from typing import Any, Dict, Iterable, List, Optional
from urllib.parse import unquote_plus

from airflow.exceptions import AirflowSkipException

def iter_s3_records_from_payload(payload: Any) -> Iterable[Dict[str, str]]:
    if isinstance(payload, dict) and payload.get("Type") == "Notification" and "Message" in payload:
        try:
            payload = json.loads(payload["Message"])
        except Exception:
            return

    if not (isinstance(payload, dict) and isinstance(payload.get("Records"), list)):
        return

    for rec in payload["Records"]:
        s3 = rec.get("s3") or {}
        bucket = (s3.get("bucket") or {}).get("name")
        obj = s3.get("object") or {}
        key = obj.get("key")
        if not bucket or not key:
            continue

        decoded_key = unquote_plus(key).lstrip("/")
        if not decoded_key or decoded_key.endswith("/"):
            continue  # skip folder markers

        prefix_dir, sep, _ = decoded_key.rpartition("/")  # e.g., "data/incremental/op_xprice", "/", "file.csv"
        prefix = f"/{prefix_dir}" if sep else "/"

        yield {"file_path": f"s3://{bucket}/{decoded_key}", "s3_uri": prefix}

def build_trigger_kwargs(cursor):
    cols = [d[0].lower() for d in cursor.description] if cursor.description else []
    rows = cursor.fetchall() if cursor.description else []
    if not rows:
        raise AirflowSkipException("No manifest match for this prefix.")
    data = dict(zip(cols, rows[0]))

    dag_id = data.get("dag_id")
    if not dag_id:
        raise AirflowSkipException("Manifest row missing dag_id.")

    return {
        "trigger_dag_id": dag_id,
        "conf": {
            "FILE_PATH": data.get("file_path"),
            "TARGET_TABLE": data.get("target_table"),
            "MODULE_NAME": data.get("module_name"),
        },
    }

def parse_sqs_message_body(body: str) -> Optional[Any]:
    try:
        return json.loads(body)
    except json.JSONDecodeError:
        return None


def metas_from_payloads(payloads: Iterable[Any]) -> List[Dict[str, str]]:
    metas: List[Dict[str, str]] = []
    for payload in payloads:
        if payload is None:
            continue
        metas.extend(iter_s3_records_from_payload(payload) or [])
    return metas


def collapse_by_prefix(metas: Iterable[Dict[str, str]]) -> Dict[str, str]:
    prefix_map: Dict[str, str] = {}
    for m in metas:
        s3_uri = m.get("s3_uri")
        file_path = m.get("file_path")
        if not s3_uri or not file_path:
            continue
        prefix_map.setdefault(s3_uri, file_path)
    return prefix_map


def items_for_manifest(prefix_map: Dict[str, str], db: str) -> List[Dict[str, str]]:
    items = [{"db": db, "s3_uri": k, "file_path": v} for k, v in prefix_map.items()]
    if not items:
        raise AirflowSkipException("Parsed batch but found no S3 URIs.")
    return items
