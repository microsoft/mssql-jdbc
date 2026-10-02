#!/usr/bin/env python3
import base64
import json
import os
import re
import time
from collections import Counter, defaultdict
from datetime import datetime, timezone

import requests
from azure.identity import AzureCliCredential
from azure.kusto.data import KustoClient, KustoConnectionStringBuilder
from deltalake import DeltaTable


ALLOWED_EVENT_NAMES = frozenset((
    "mssql.driver.error",
    "mssql.driver.connection.retry_decision",
))
FORBIDDEN_ATTRIBUTE = re.compile(
    r"authorization|password|bearer|connection[._]string|db\.(statement|query)|"
    r"exception\.(message|stacktrace)|http\.request\.header",
    re.IGNORECASE,
)


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def normalize_id(value, byte_length, root=False):
    if isinstance(value, bytes):
        raw = value
    else:
        text = str(value or "")
        if len(text) == byte_length * 2 and re.fullmatch(r"[0-9a-fA-F]+", text):
            raw = bytes.fromhex(text)
        elif text:
            raw = base64.b64decode(text, validate=True)
        else:
            raw = b""
    if root and (not raw or raw == bytes(byte_length)):
        return ""
    if len(raw) != byte_length:
        raise RuntimeError("Cloud trace identifier has an invalid length")
    return raw.hex()


def expected_rows():
    with open("/artifacts/expected.json", encoding="utf-8") as handle:
        return json.load(handle)


def normalize(rows):
    keys = ("application", "trace_id", "span_id", "parent_span_id", "name",
            "status_code", "attributes", "start_us", "duration_us")
    result = []
    for row in rows:
        normalized = {key: row[key] for key in keys}
        normalized["trace_id"] = normalize_id(normalized["trace_id"], 16)
        normalized["span_id"] = normalize_id(normalized["span_id"], 8)
        normalized["parent_span_id"] = normalize_id(normalized["parent_span_id"], 8, root=True)
        normalized["status_code"] = int(normalized["status_code"])
        normalized["attributes"] = canonical(json.loads(normalized["attributes"]))
        for key in keys:
            if key not in ("status_code", "trace_id", "span_id", "parent_span_id"):
                normalized[key] = str(normalized[key])
        result.append(normalized)
    return sorted(result, key=lambda row: (row["trace_id"], row["start_us"], row["span_id"]))


def assert_rows(label, expected, actual):
    if normalize(expected) != normalize(actual):
        raise RuntimeError(f"{label} trace rows do not match local OTLP evidence")


def kusto_client():
    cluster = os.environ["KUSTO_CLUSTER_URI"].rstrip("/")
    credential = AzureCliCredential(tenant_id=os.environ["AZURE_TENANT_ID"], process_timeout=30)
    builder = KustoConnectionStringBuilder.with_token_provider(
        cluster, lambda: credential.get_token(cluster + "/.default").token)
    return KustoClient(builder)


KUSTO_ROWS = r"""
let App = '__SERVICE__';
let A = span_attrs
| where application == App
| extend value = case(type == 1, str, type == 2, tostring(tolong(['int'])), type == 3, iif(['double'] == todouble(tolong(['double'])), tostring(tolong(['double'])), tostring(['double'])), type == 4, tolower(tostring(['bool'])), type == 5, bytes, ser)
| summarize attributes = make_bag(pack(key, value)) by parent_id, export_time_unix_nano;
spans
| where application == App
| join kind=leftouter A on $left.id == $right.parent_id, $left.export_time_unix_nano == $right.export_time_unix_nano
| project application, trace_id, span_id, parent_span_id, name, status_code,
          attributes=tostring(coalesce(attributes, dynamic({}))),
          start_us=tostring(datetime_diff('microsecond', start_time_unix_nano, datetime(1970-01-01))),
          duration_us=tostring(duration_time_unix_nano)
"""


def read_kusto(service):
    client = kusto_client()
    table_result = client.execute_mgmt(
        os.environ["KUSTO_DATABASE"], ".show tables | project TableName"
    ).primary_results[0]
    existing_tables = {str(row[0]) for row in table_result}
    query = KUSTO_ROWS.replace("__SERVICE__", service.replace("'", "''"))
    result = client.execute(os.environ["KUSTO_DATABASE"], query).primary_results[0]
    columns = [column.column_name for column in result.columns]
    rows = [dict(zip(columns, list(row))) for row in result]
    prohibited_tables = existing_tables.intersection({
        "logs", "univariate_metrics", "number_data_points",
        "histogram_data_points", "exp_histogram_data_points",
        "summary_data_points",
    })
    if prohibited_tables:
        unexpected_query = f"""
union withsource=TableName {', '.join(sorted(prohibited_tables))}
| where application == '{service.replace("'", "''")}'
| summarize rows=count() by TableName
"""
        unexpected = client.execute(os.environ["KUSTO_DATABASE"], unexpected_query).primary_results[0]
        if any(int(row[1]) for row in unexpected):
            raise RuntimeError("Kusto contains unexpected JDBC metrics or logs")
    events = client.execute(os.environ["KUSTO_DATABASE"], f"""
span_events
| where application == '{service.replace("'", "''")}'
| summarize rows=count() by name
""").primary_results[0]
    event_counts = Counter({str(row[0]): int(row[1]) for row in events})
    if not event_counts or set(event_counts) - ALLOWED_EVENT_NAMES:
        raise RuntimeError("Kusto contains missing or unknown JDBC span events")
    event_keys = client.execute(os.environ["KUSTO_DATABASE"], f"""
span_event_attrs
| where application == '{service.replace("'", "''")}'
| distinct key
""").primary_results[0]
    keys = {str(row[0]) for row in event_keys}
    if any(FORBIDDEN_ATTRIBUTE.search(key) for key in keys):
        raise RuntimeError("Kusto contains unsafe JDBC span-event attributes")
    return rows, event_counts, keys


def micros(value):
    if isinstance(value, datetime):
        return str(int(value.replace(tzinfo=value.tzinfo or timezone.utc).timestamp() * 1_000_000))
    return str(int(value))


def delta_uri(table):
    return (f"az://{os.environ['STORAGE_CONTAINER']}/"
            f"{os.environ['ROOT_PATH'].strip('/')}/{table}")


def storage_options():
    credential = AzureCliCredential(tenant_id=os.environ["AZURE_TENANT_ID"], process_timeout=30)
    return {
        "azure_storage_account_name": os.environ["STORAGE_ACCOUNT"],
        "bearer_token": credential.get_token("https://storage.azure.com/.default").token,
        "use_fabric_endpoint": "false",
    }


def delta_attribute_value(row):
    kind = int(row.get("type") or 0)
    if kind == 1:
        return str(row.get("str") or "")
    if kind == 2:
        return str(int(row.get("int") or 0))
    if kind == 3:
        value = float(row.get("double") or 0)
        return str(int(value)) if value.is_integer() else str(value)
    if kind == 4:
        return str(bool(row.get("bool"))).lower()
    if kind == 5:
        value = row.get("bytes") or ""
        return base64.b64encode(value).decode("ascii") if isinstance(value, bytes) else str(value)
    return str(row.get("ser") or "")


def read_delta(service):
    options = storage_options()
    spans = DeltaTable(delta_uri("spans"), storage_options=options).to_pyarrow_table().to_pylist()
    attrs = DeltaTable(delta_uri("span_attrs"), storage_options=options).to_pyarrow_table().to_pylist()
    grouped = defaultdict(dict)
    for row in attrs:
        if row.get("application") != service:
            continue
        grouped[(row["parent_id"], row["export_time_unix_nano"])][row["key"]] = delta_attribute_value(row)
    result = []
    for row in spans:
        if row.get("application") != service:
            continue
        result.append({
            "application": service,
            "trace_id": row["trace_id"],
            "span_id": row["span_id"],
            "parent_span_id": row.get("parent_span_id") or "",
            "name": row["name"],
            "status_code": row.get("status_code") or 0,
            "attributes": canonical(grouped.get((row["id"], row["export_time_unix_nano"]), {})),
            "start_us": micros(row["start_time_unix_nano"]),
            "duration_us": str(row["duration_time_unix_nano"]),
        })
    events = DeltaTable(delta_uri("span_events"), storage_options=options).to_pyarrow_table().to_pylist()
    event_attrs = DeltaTable(delta_uri("span_event_attrs"), storage_options=options).to_pyarrow_table().to_pylist()
    event_counts = Counter(str(row["name"]) for row in events if row.get("application") == service)
    if not event_counts or set(event_counts) - ALLOWED_EVENT_NAMES:
        raise RuntimeError("Delta contains missing or unknown JDBC span events")
    keys = {str(row["key"]) for row in event_attrs if row.get("application") == service}
    if any(FORBIDDEN_ATTRIBUTE.search(key) for key in keys):
        raise RuntimeError("Delta contains unsafe JDBC span-event attributes")
    return result, event_counts, keys


def verify_grafana():
    base = os.environ["GRAFANA_URL"].rstrip("/")
    auth = ("admin", os.environ["GRAFANA_ADMIN_PASSWORD"])
    health = requests.get(base + "/api/health", timeout=10)
    health.raise_for_status()
    dashboard = requests.get(base + "/api/dashboards/uid/jdbc-connection-errors", auth=auth, timeout=15)
    dashboard.raise_for_status()
    dashboard_model = dashboard.json()["dashboard"]
    with open("/artifacts/queries.json", encoding="utf-8") as handle:
        queries = json.load(handle)
    roots = [row for row in expected_rows() if row["name"] == "mssql.driver.connection.open"]
    if not roots:
        raise RuntimeError("No failed connection is available for Grafana drill-down verification")
    selected = next((row for row in roots if json.loads(row["attributes"]).get("mssql.connection.failure_phase") == "login"), roots[0])
    selected_attributes = json.loads(selected["attributes"])
    replacements = {
        "$failure_phase": selected_attributes["mssql.connection.failure_phase"],
        "$error_category": selected_attributes["mssql.error.category"],
        "$error_type": selected_attributes["error.type"],
        "$connection_guid": selected_attributes["mssql.connection.guid"],
        "${failure_phase:raw}": selected_attributes["mssql.connection.failure_phase"],
        "${error_category:raw}": selected_attributes["mssql.error.category"],
        "${error_type:raw}": selected_attributes["error.type"],
        "${connection_guid:raw}": selected_attributes["mssql.connection.guid"],
    }

    def resolve(query):
        for variable, value in replacements.items():
            query = query.replace(variable, str(value).replace("'", "''"))
        return query

    def execute(query, index, require_rows=True):
        query = resolve(query)
        payload = {
            "from": str(now - 3_600_000), "to": str(now),
            "queries": [{
                "refId": f"Q{index}", "datasource": {"type": "grafana-azure-data-explorer-datasource", "uid": "adx-jdbc-errors"},
                "queryType": "KQL", "querySource": "raw", "rawMode": True,
                "resultFormat": "table", "query": query, "intervalMs": 60000,
                "maxDataPoints": 2000, "pluginVersion": "7.2.8", "clusterUri": "", "database": ""
            }]
        }
        response = requests.post(base + "/api/ds/query", auth=auth, json=payload, timeout=150)
        response.raise_for_status()
        body = response.json()
        results = body.get("results", {})
        if any(value.get("error") for value in results.values()):
            raise RuntimeError(f"Grafana query {index} returned an error")
        rows = 0
        for result in results.values():
            for frame in result.get("frames", []):
                values = frame.get("data", {}).get("values", [])
                rows = max(rows, max((len(column) for column in values), default=0))
        if require_rows and rows < 1:
            raise RuntimeError(f"Grafana drill-down query {index} returned no rows")

    now = int(time.time() * 1000)
    for index, query in enumerate(queries.values(), 1):
        execute(query, index)
    query_index = len(queries) + 1
    for variable in dashboard_model.get("templating", {}).get("list", []):
        execute(variable["query"], query_index)
        query_index += 1
    for panel in dashboard_model.get("panels", []):
        for panel_target in panel.get("targets", []):
            execute(panel_target["query"], query_index)
            query_index += 1


def main():
    expected = expected_rows()
    service = expected[0]["application"]
    deadline = time.monotonic() + 900
    last_error = None
    while time.monotonic() < deadline:
        try:
            kusto, kusto_events, kusto_event_keys = read_kusto(service)
            if len(kusto) < len(expected):
                raise RuntimeError("Kusto ingestion is incomplete")
            assert_rows("Kusto", expected, kusto)
            delta, delta_events, delta_event_keys = read_delta(service)
            if len(delta) < len(expected):
                raise RuntimeError("Delta commit is incomplete")
            assert_rows("Delta", expected, delta)
            if kusto_events != delta_events or kusto_event_keys != delta_event_keys:
                raise RuntimeError("Kusto and Delta span-event metadata do not match")
            verify_grafana()
            with open("/artifacts/cloud-result.json", "w", encoding="utf-8") as handle:
                json.dump({"status": "passed", "service": service, "rows": len(expected)}, handle, indent=2)
            print(f"Cloud E2E verified: {len(expected)} connection-error spans in Kusto and Delta; Grafana queries passed.")
            return
        except Exception as error:
            last_error = type(error).__name__
            time.sleep(10)
    raise RuntimeError(f"Cloud E2E verification timed out ({last_error})")


if __name__ == "__main__":
    main()
