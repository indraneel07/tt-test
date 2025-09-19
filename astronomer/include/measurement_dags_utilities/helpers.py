from __future__ import annotations

import re
from collections import OrderedDict
from typing import Any, Dict, List


def looks_like_number(s: str) -> bool:
    try:
        float(s)
        return True
    except Exception:
        return False


def sql_literal(v: Any) -> str:
    if v is None:
        return "NULL"
    s = str(v)
    su = s.upper()
    if su == "NULL":
        return "NULL"
    if su in ("TRUE", "FALSE"):
        return su
    if looks_like_number(s):
        return s
    if s.startswith("'") and s.endswith("'"):
        return s
    return "'" + s.replace("'", "''") + "'"


def qualify_proc(proc_name: str, default_schema: str | None) -> str:
    if not proc_name:
        return proc_name
    return proc_name if "." in proc_name else (f"{default_schema}.{proc_name}" if default_schema else proc_name)


def fetch_as_dicts(cursor):
    cols = [d[0] for d in cursor.description]
    return [dict(zip(cols, row)) for row in cursor.fetchall()]

def parse_file_path_info(file_path: str) -> Dict[str, Any]:
    m = re.search(r"/(\d+)/[^/]*?LoadID-(\d+)[^/]*?ClientID-(\d+)\.", file_path)
    if not m:
        raise ValueError("File path does not match expected pattern for job/load/client IDs")
    return {
        "job_id": int(m.group(1)),
        "load_id": int(m.group(2)),
        "client_id": int(m.group(3)),
        "file_path": file_path,
    }

def validate_inputs(info: Dict[str, Any]) -> Dict[str, Any]:
    required = [("file_path", str), ("target_table", str), ("module_name", str), ("job_id", int), ("load_id", int)]
    for k, t in required:
        v = info.get(k)
        if v is None or (isinstance(v, str) and v.strip() == ""):
            raise ValueError(f"{k} is required")
        if t is int and int(v) <= 0:
            raise ValueError(f"{k} must be > 0")
    return info


def build_calls(
    rows: List[Dict[str, Any]],
    default_schema: str | None,
    steporder: int,
    load_id: int,
    target_id: str,
) -> List[str]:
    if not rows:
        return []

    grouped_params: Dict[Any, OrderedDict[str, Any]] = {}
    procs_by_step: Dict[Any, List[str]] = {}

    for r in rows:
        step_id = r.get("STEPID")
        pname = (r.get("PARAMETERNAME") or "").strip()
        pval = r.get("PARAMETERVALUE")
        if step_id not in grouped_params:
            grouped_params[step_id] = OrderedDict()
            procs_by_step[step_id] = []
        if "spname" in pname.lower():
            if pval is not None and str(pval).strip() != "":
                procs_by_step[step_id].append(str(pval).strip())
        elif pname:
            grouped_params[step_id][pname] = pval

    set_tag = f"CALL UTILS.p_SetQueryTag(TargetID=>{sql_literal(target_id)},LoadID=>{load_id});"
    calls: List[str] = []

    for step_id, base_params in grouped_params.items():
        procs = procs_by_step.get(step_id, [])
        if not procs:
            continue

        if "LoadID" not in base_params:
            base_params = OrderedDict([("LoadID", load_id), *base_params.items()])

        if steporder == 10:
            spname = procs[0]
            proc_name = qualify_proc(spname, default_schema)
            price_tables = ["dbp", "fbp", "gbp", "kbp", "mbp", "obp", "xbp"]
            for pt in price_tables:
                pmap = OrderedDict(base_params.items())
                pmap["PriceTableID"] = pt
                named_args = ",\n        ".join(f"{k} => {sql_literal(v)}" for k, v in pmap.items())
                calls.append(set_tag + "\n" + f"CALL {proc_name}(\n        {named_args}\n);")
        else:
            named_args = ",\n        ".join(f"{k} => {sql_literal(v)}" for k, v in base_params.items())
            for spname in procs:
                proc_name = qualify_proc(spname, default_schema)
                calls.append(set_tag + "\n" + f"CALL {proc_name}(\n        {named_args}\n);")

    return calls
