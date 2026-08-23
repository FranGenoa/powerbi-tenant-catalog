# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "7d01b385-49fb-4874-9fca-ba9b47dfd240",
# META       "default_lakehouse_name": "PBICatalogLakehouse",
# META       "default_lakehouse_workspace_id": "1b1d98e1-4bab-4441-bd10-c262cc2d84f2",
# META       "known_lakehouses": [
# META         {
# META           "id": "7d01b385-49fb-4874-9fca-ba9b47dfd240"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Power BI Tenant Documentation Scanner
# 
# Scans Power BI / Fabric content and produces a **machine-readable data dictionary**:
# workspaces, reports, semantic models, tables, columns, **DAX measures (with expressions)**,
# calculated columns, relationships, RLS roles, M/Power Query expressions, data sources and lineage.
# 
# ## Pipeline shape
# 
# ```
# Power BI APIs  ->  Files/pbi_documentation/raw/<run_id>/*.json   (bronze, verbatim payload)
#                         |
#                         v
#                    pbi_* Delta tables                            (silver, flattened)
#                         |
#                         v
#                    Files/pbi_documentation/data_dictionary_<run_id>.md
# ```
# 
# The raw JSON is written **first and untouched**, so a schema change in the API never loses
# data - you can always re-run the flatten step against previously landed files.
# 
# ## Modes
# 
# | Mode | API used | Requirement | Coverage |
# |---|---|---|---|
# | `admin` | **Power BI Scanner API** (`/admin/workspaces/getInfo`) | Caller is a Fabric/Power BI Administrator | Whole tenant, no per-model queries |
# | `user` | Workspace REST + **DAX `INFO.VIEW.*`** via `executeQueries` | Workspace Member/Contributor + XMLA read | Only workspaces you can access |
# 
# `MODE = "auto"` probes for admin rights and falls back to user mode automatically.
# 
# ## Prerequisites
# 
# **Admin mode** (the fast path for a tenant-wide scan):
# 1. Caller is a member of the **Fabric Administrator** / Power BI Administrator role.
# 2. Admin portal -> Tenant settings -> **Admin API settings**:
#    - *Enhanced admin APIs responses with detailed metadata* -> **Enabled**
#    - *Enhanced admin APIs responses with DAX and mashup expressions* -> **Enabled** (this is what returns DAX)
# 
# **User mode**: workspaces on Fabric/Premium capacity, **XMLA endpoint = Read**, and the
# *Dataset Execute Queries REST API* tenant setting enabled.

# PARAMETERS CELL ********************

# ============================================================================
# 1. Configuration  (this cell is parameterised - a pipeline can override it)
# ============================================================================

MODE = "auto"                     # "auto" | "admin" | "user"

# --- Scope -----------------------------------------------------------------
WORKSPACE_NAME_FILTER: list = []  # e.g. ["Finance", "PBI_Copilot"]; empty = all
WORKSPACE_ID_FILTER: list = []    # explicit workspace ids; empty = all
EXCLUDE_PERSONAL_WORKSPACES = True
EXCLUDE_INACTIVE_WORKSPACES = True

# --- Scanner API behaviour -------------------------------------------------
BATCH_SIZE = 100                  # max allowed by the Scanner API
SCAN_POLL_SECONDS = 5
SCAN_TIMEOUT_SECONDS = 3600

# --- User-mode behaviour ---------------------------------------------------
USER_MODE_MAX_MODELS = 0          # 0 = no limit; set e.g. 25 for a quick smoke test
USER_MODE_SLEEP_SECONDS = 0.5     # executeQueries is throttled at ~120 req/min

# --- Output ----------------------------------------------------------------
FILES_SUBFOLDER = "pbi_documentation"
TABLE_PREFIX = "pbi_"
WRITE_MODE = "overwrite"          # "overwrite" | "append"
RUN_ID = ""                       # blank = derive from UTC timestamp
FLATTEN_RUN_IDS: list = []        # re-flatten historical raw folders; empty = current run
GENERATE_MARKDOWN_DICTIONARY = True

print("Configuration loaded.")

# CELL ********************

# ============================================================================
# 2. Imports, authentication, HTTP helper and Lakehouse paths
# ============================================================================
import glob
import json
import os
import re
import time
import datetime as dt
from typing import Any

import pandas as pd
import requests

PBI_API = "https://api.powerbi.com/v1.0/myorg"

RUN_ID = RUN_ID or dt.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")

LAKEHOUSE_MOUNT = "/lakehouse/default"
if not os.path.isdir(LAKEHOUSE_MOUNT):
    raise RuntimeError(
        "No default Lakehouse is attached. Attach one from the Explorer pane "
        "(Add data items -> Lakehouse) and re-run."
    )

FILES_ROOT = os.path.join(LAKEHOUSE_MOUNT, "Files", FILES_SUBFOLDER)
RAW_ROOT = os.path.join(FILES_ROOT, "raw")
RAW_DIR = os.path.join(RAW_ROOT, RUN_ID)
os.makedirs(RAW_DIR, exist_ok=True)

_token_cache: dict = {}


def get_token(audience: str = "pbi") -> str:
    """Token for the identity running the notebook. Cached for 45 minutes."""
    cached = _token_cache.get(audience)
    if cached and cached[1] > time.time():
        return cached[0]
    import notebookutils
    token = notebookutils.credentials.getToken(audience)
    _token_cache[audience] = (token, time.time() + 45 * 60)
    return token


_session = requests.Session()
RETRYABLE = {429, 500, 502, 503, 504}


def api(method: str, url: str, *, json_body: Any = None, max_retries: int = 6,
        audience: str = "pbi", raise_on_error: bool = True) -> Any:
    """Call a Power BI / Fabric REST endpoint with 429-aware exponential backoff."""
    if url.startswith("/"):
        url = PBI_API + url
    delay = 2.0
    last = None
    for _ in range(max_retries):
        headers = {"Authorization": "Bearer " + get_token(audience),
                   "Content-Type": "application/json"}
        resp = _session.request(method, url, headers=headers, json=json_body, timeout=300)
        last = resp
        if 200 <= resp.status_code < 300:
            if not resp.content:
                return {}
            try:
                return resp.json()
            except ValueError:
                return {"raw": resp.text}
        if resp.status_code in RETRYABLE:
            wait = float(resp.headers.get("Retry-After", delay))
            print("  [retry] HTTP {} on {} - waiting {:.0f}s".format(resp.status_code, url, wait))
            time.sleep(wait)
            delay = min(delay * 2, 120)
            continue
        break
    msg = "{} {} -> HTTP {}: {}".format(method, url, last.status_code, last.text[:600])
    if raise_on_error:
        raise RuntimeError(msg)
    print("  [warn] " + msg)
    return None


def as_json(value: Any):
    """Serialise nested structures so they survive a flat table column."""
    if value is None or isinstance(value, str):
        return value
    return json.dumps(value, default=str, ensure_ascii=False)


def land_raw(payload: dict, name: str) -> str:
    """Write a verbatim API payload into the Files section (bronze layer)."""
    path = os.path.join(RAW_DIR, name + ".json")
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(payload, fh, ensure_ascii=False, default=str)
    print("  landed {}  ({:,} bytes)".format(os.path.basename(path), os.path.getsize(path)))
    return path


print("Run id     :", RUN_ID)
print("Raw folder : Files/{}/raw/{}".format(FILES_SUBFOLDER, RUN_ID))

# CELL ********************

# ============================================================================
# 3. Decide which mode to run in
# ============================================================================

def has_admin_access() -> bool:
    probe = api("GET", "/admin/workspaces/modified?excludePersonalWorkspaces=True",
                max_retries=1, raise_on_error=False)
    return probe is not None


if MODE == "auto":
    EFFECTIVE_MODE = "admin" if has_admin_access() else "user"
elif MODE == "admin":
    if not has_admin_access():
        raise PermissionError(
            "Admin mode requested but /admin/workspaces/modified was rejected. Confirm the "
            "caller is a Fabric Administrator and that 'Enhanced admin APIs responses with "
            "detailed metadata' is enabled in tenant settings."
        )
    EFFECTIVE_MODE = "admin"
else:
    EFFECTIVE_MODE = "user"

print("Effective mode:", EFFECTIVE_MODE)

# CELL ********************

# ============================================================================
# 4. BRONZE - admin mode: Power BI Scanner API -> raw JSON in Files
# ============================================================================

def list_modified_workspaces() -> list:
    params = []
    if EXCLUDE_PERSONAL_WORKSPACES:
        params.append("excludePersonalWorkspaces=True")
    if EXCLUDE_INACTIVE_WORKSPACES:
        params.append("excludeInActiveWorkspaces=True")
    query = ("?" + "&".join(params)) if params else ""
    data = api("GET", "/admin/workspaces/modified" + query)
    return [w["id"] for w in (data or [])]


def start_scan(workspace_ids: list) -> str:
    query = ("?lineage=True&datasourceDetails=True&datasetSchema=True"
             "&datasetExpressions=True&getArtifactUsers=True")
    return api("POST", "/admin/workspaces/getInfo" + query,
               json_body={"workspaces": workspace_ids})["id"]


def wait_for_scan(scan_id: str) -> None:
    deadline = time.time() + SCAN_TIMEOUT_SECONDS
    while time.time() < deadline:
        status = api("GET", "/admin/workspaces/scanStatus/" + scan_id).get("status")
        if status == "Succeeded":
            return
        if status in ("Failed", "Cancelled"):
            raise RuntimeError("Scan {} ended with status {}".format(scan_id, status))
        time.sleep(SCAN_POLL_SECONDS)
    raise TimeoutError("Scan {} did not finish within {}s".format(scan_id, SCAN_TIMEOUT_SECONDS))


def run_admin_scan() -> None:
    ws_ids = WORKSPACE_ID_FILTER or list_modified_workspaces()
    print("Workspaces discovered: {}".format(len(ws_ids)))

    batches = [ws_ids[i:i + BATCH_SIZE] for i in range(0, len(ws_ids), BATCH_SIZE)]
    for n, batch in enumerate(batches, start=1):
        print("Batch {}/{} - scanning {} workspaces...".format(n, len(batches), len(batch)))
        scan_id = start_scan(batch)
        wait_for_scan(scan_id)
        result = api("GET", "/admin/workspaces/scanResult/" + scan_id)
        result["_meta"] = {"mode": "admin", "runId": RUN_ID, "scanId": scan_id,
                           "batch": n, "requestedWorkspaces": batch,
                           "landedUtc": dt.datetime.utcnow().isoformat() + "Z"}
        land_raw(result, "scan_{:04d}".format(n))


if EFFECTIVE_MODE == "admin":
    run_admin_scan()
else:
    print("Skipped - running in user mode.")

# CELL ********************

# ============================================================================
# 5. BRONZE - user mode fallback: REST + DAX INFO.VIEW -> raw JSON in Files
#    Emits the same shape as the Scanner API so one flattener handles both.
# ============================================================================

DAX_TABLES = "EVALUATE INFO.VIEW.TABLES()"
DAX_COLUMNS = "EVALUATE INFO.VIEW.COLUMNS()"
DAX_MEASURES = "EVALUATE INFO.VIEW.MEASURES()"
DAX_RELATIONSHIPS = "EVALUATE INFO.VIEW.RELATIONSHIPS()"


def execute_dax(group_id: str, dataset_id: str, dax: str):
    body = {"queries": [{"query": dax}], "serializerSettings": {"includeNulls": True}}
    res = api("POST", "/groups/{}/datasets/{}/executeQueries".format(group_id, dataset_id),
              json_body=body, raise_on_error=False, max_retries=3)
    if not res:
        return []
    try:
        rows = res["results"][0]["tables"][0]["rows"]
    except (KeyError, IndexError):
        return []
    return [{re.sub(r"^.*\[|\]$", "", k): v for k, v in row.items()} for row in rows]


def run_user_scan() -> None:
    groups = api("GET", "/groups?$top=5000").get("value", [])
    if WORKSPACE_ID_FILTER:
        groups = [g for g in groups if g["id"] in WORKSPACE_ID_FILTER]
    if WORKSPACE_NAME_FILTER:
        groups = [g for g in groups if g.get("name") in WORKSPACE_NAME_FILTER]
    print("Accessible workspaces: {}".format(len(groups)))

    workspaces, models_done = [], 0
    for g in groups:
        ws_id, ws_name = g["id"], g.get("name")
        reports = (api("GET", "/groups/{}/reports".format(ws_id), raise_on_error=False) or {}).get("value", [])
        datasets = (api("GET", "/groups/{}/datasets".format(ws_id), raise_on_error=False) or {}).get("value", [])

        ws_payload = {"id": ws_id, "name": ws_name, "type": g.get("type"),
                      "state": g.get("state"), "capacityId": g.get("capacityId"),
                      "reports": reports, "datasets": []}

        for ds in datasets:
            if USER_MODE_MAX_MODELS and models_done >= USER_MODE_MAX_MODELS:
                break
            ds_id = ds["id"]
            tables = {}
            for t in execute_dax(ws_id, ds_id, DAX_TABLES):
                tables[t.get("Name")] = {
                    "name": t.get("Name"), "description": t.get("Description"),
                    "isHidden": t.get("IsHidden"), "storageMode": t.get("StorageMode"),
                    "source": [{"expression": t.get("Expression")}] if t.get("Expression") else [],
                    "columns": [], "measures": [],
                }
            for c in execute_dax(ws_id, ds_id, DAX_COLUMNS):
                tables.setdefault(c.get("Table"), {"name": c.get("Table"), "columns": [], "measures": []})
                tables[c.get("Table")]["columns"].append({
                    "name": c.get("Name"), "dataType": c.get("DataType"),
                    "columnType": c.get("Type"), "isHidden": c.get("IsHidden"),
                    "description": c.get("Description"), "expression": c.get("Expression"),
                    "displayFolder": c.get("DisplayFolder"), "formatString": c.get("FormatString"),
                })
            for m in execute_dax(ws_id, ds_id, DAX_MEASURES):
                tables.setdefault(m.get("Table"), {"name": m.get("Table"), "columns": [], "measures": []})
                tables[m.get("Table")]["measures"].append({
                    "name": m.get("Name"), "expression": m.get("Expression"),
                    "description": m.get("Description"), "isHidden": m.get("IsHidden"),
                    "displayFolder": m.get("DisplayFolder"), "formatString": m.get("FormatString"),
                })

            ws_payload["datasets"].append({
                "id": ds_id, "name": ds.get("name"), "description": ds.get("description"),
                "configuredBy": ds.get("configuredBy"), "createdDate": ds.get("createdDate"),
                "contentProviderType": ds.get("contentProviderType"),
                "targetStorageMode": ds.get("targetStorageMode"),
                "tables": list(tables.values()),
                "relationships": execute_dax(ws_id, ds_id, DAX_RELATIONSHIPS),
            })
            models_done += 1
            if USER_MODE_SLEEP_SECONDS:
                time.sleep(USER_MODE_SLEEP_SECONDS)

        workspaces.append(ws_payload)
        print("  {} -> {} reports, {} models".format(ws_name, len(reports), len(datasets)))

    land_raw({"workspaces": workspaces,
              "_meta": {"mode": "user", "runId": RUN_ID, "batch": 1,
                        "landedUtc": dt.datetime.utcnow().isoformat() + "Z"}},
             "scan_0001")


if EFFECTIVE_MODE == "user":
    run_user_scan()
else:
    print("Skipped - running in admin mode.")

# CELL ********************

# ============================================================================
# 6. SILVER - read the landed JSON back and flatten into tabular entities
# ============================================================================

ROWS: dict = {k: [] for k in (
    "workspaces", "reports", "semantic_models", "tables", "columns", "measures",
    "calculated_columns", "relationships", "rls_roles", "model_parameters",
    "datasources", "dashboards", "tiles", "dataflows", "other_items", "permissions",
)}


def add(bucket: str, row: dict) -> None:
    row.setdefault("scanRunId", CURRENT_RUN)
    ROWS[bucket].append(row)


def flatten_workspace(ws: dict) -> None:
    ws_id, ws_name = ws.get("id"), ws.get("name")

    add("workspaces", {
        "workspaceId": ws_id, "workspaceName": ws_name,
        "type": ws.get("type"), "state": ws.get("state"),
        "capacityId": ws.get("capacityId"),
        "defaultDatasetStorageFormat": ws.get("defaultDatasetStorageFormat"),
        "description": ws.get("description"),
        "reportCount": len(ws.get("reports", []) or []),
        "semanticModelCount": len(ws.get("datasets", []) or []),
        "dashboardCount": len(ws.get("dashboards", []) or []),
        "dataflowCount": len(ws.get("dataflows", []) or []),
    })

    for user in ws.get("users", []) or []:
        add("permissions", {
            "workspaceId": ws_id, "workspaceName": ws_name,
            "artifactType": "Workspace", "artifactId": ws_id, "artifactName": ws_name,
            "principal": user.get("emailAddress") or user.get("displayName"),
            "principalType": user.get("principalType"),
            "identifier": user.get("identifier"),
            "role": user.get("groupUserAccessRight"),
        })

    for rpt in ws.get("reports", []) or []:
        add("reports", {
            "workspaceId": ws_id, "workspaceName": ws_name,
            "reportId": rpt.get("id"), "reportName": rpt.get("name"),
            "reportType": rpt.get("reportType"),
            "semanticModelId": rpt.get("datasetId"),
            "description": rpt.get("description"),
            "createdDateTime": rpt.get("createdDateTime"),
            "modifiedDateTime": rpt.get("modifiedDateTime"),
            "modifiedBy": rpt.get("modifiedBy"), "createdBy": rpt.get("createdBy"),
            "endorsement": (rpt.get("endorsementDetails") or {}).get("endorsement"),
            "certifiedBy": (rpt.get("endorsementDetails") or {}).get("certifiedBy"),
            "sensitivityLabelId": (rpt.get("sensitivityLabel") or {}).get("labelId"),
            "appId": rpt.get("appId"),
            "originalReportObjectId": rpt.get("originalReportObjectId"),
            "webUrl": "https://app.powerbi.com/groups/{}/reports/{}".format(ws_id, rpt.get("id")),
        })
        for user in rpt.get("users", []) or []:
            add("permissions", {
                "workspaceId": ws_id, "workspaceName": ws_name,
                "artifactType": "Report", "artifactId": rpt.get("id"),
                "artifactName": rpt.get("name"),
                "principal": user.get("emailAddress") or user.get("displayName"),
                "principalType": user.get("principalType"),
                "identifier": user.get("identifier"),
                "role": user.get("reportUserAccessRight"),
            })

    for ds in ws.get("datasets", []) or []:
        ds_id, ds_name = ds.get("id"), ds.get("name")
        tables = ds.get("tables", []) or []
        n_measures = sum(len(t.get("measures", []) or []) for t in tables)
        n_columns = sum(len(t.get("columns", []) or []) for t in tables)

        add("semantic_models", {
            "workspaceId": ws_id, "workspaceName": ws_name,
            "semanticModelId": ds_id, "semanticModelName": ds_name,
            "description": ds.get("description"), "configuredBy": ds.get("configuredBy"),
            "createdDate": ds.get("createdDate"),
            "contentProviderType": ds.get("contentProviderType"),
            "targetStorageMode": ds.get("targetStorageMode"),
            "endorsement": (ds.get("endorsementDetails") or {}).get("endorsement"),
            "certifiedBy": (ds.get("endorsementDetails") or {}).get("certifiedBy"),
            "sensitivityLabelId": (ds.get("sensitivityLabel") or {}).get("labelId"),
            "tableCount": len(tables), "columnCount": n_columns, "measureCount": n_measures,
            "upstreamDataflows": as_json(ds.get("upstreamDataflows")),
            "upstreamDatasets": as_json(ds.get("upstreamDatasets")),
            "upstreamDatamarts": as_json(ds.get("upstreamDatamarts")),
            "webUrl": "https://app.powerbi.com/groups/{}/datasets/{}".format(ws_id, ds_id),
        })

        for expr in ds.get("expressions", []) or []:
            add("model_parameters", {
                "workspaceId": ws_id, "workspaceName": ws_name,
                "semanticModelId": ds_id, "semanticModelName": ds_name,
                "name": expr.get("name"), "description": expr.get("description"),
                "expression": expr.get("expression"),
            })

        for role in ds.get("roles", []) or []:
            perms = role.get("tablePermissions") or [{}]
            for perm in perms:
                add("rls_roles", {
                    "workspaceId": ws_id, "workspaceName": ws_name,
                    "semanticModelId": ds_id, "semanticModelName": ds_name,
                    "roleName": role.get("name"),
                    "modelPermission": role.get("modelPermission"),
                    "tableName": perm.get("name"),
                    "filterExpression": perm.get("filterExpression"),
                    "members": as_json(role.get("members")),
                })

        for src in ds.get("datasourceUsages", []) or []:
            add("datasources", {
                "workspaceId": ws_id, "workspaceName": ws_name,
                "artifactType": "SemanticModel", "artifactId": ds_id, "artifactName": ds_name,
                "datasourceInstanceId": src.get("datasourceInstanceId"),
                "gatewayId": src.get("gatewayId"), "details": as_json(src),
            })

        for rel in ds.get("relationships", []) or []:
            add("relationships", {
                "workspaceId": ws_id, "workspaceName": ws_name,
                "semanticModelId": ds_id, "semanticModelName": ds_name,
                "relationship": rel.get("Relationship"),
                "fromTable": rel.get("FromTable"), "fromColumn": rel.get("FromColumn"),
                "toTable": rel.get("ToTable"), "toColumn": rel.get("ToColumn"),
                "isActive": rel.get("IsActive"),
                "cardinality": "{}-{}".format(rel.get("FromCardinality"), rel.get("ToCardinality")),
                "crossFilterBehavior": rel.get("CrossFilteringBehavior"),
                "securityFilterBehavior": rel.get("SecurityFilteringBehavior"),
            })

        for tbl in tables:
            t_name = tbl.get("name")
            add("tables", {
                "workspaceId": ws_id, "workspaceName": ws_name,
                "semanticModelId": ds_id, "semanticModelName": ds_name,
                "tableName": t_name, "description": tbl.get("description"),
                "isHidden": tbl.get("isHidden"), "storageMode": tbl.get("storageMode"),
                "columnCount": len(tbl.get("columns", []) or []),
                "measureCount": len(tbl.get("measures", []) or []),
                "sourceExpression": as_json([s.get("expression") for s in (tbl.get("source") or [])]),
            })

            for col in tbl.get("columns", []) or []:
                is_calc = str(col.get("columnType") or "").lower().startswith("calc")
                add("calculated_columns" if is_calc else "columns", {
                    "workspaceId": ws_id, "workspaceName": ws_name,
                    "semanticModelId": ds_id, "semanticModelName": ds_name,
                    "tableName": t_name, "columnName": col.get("name"),
                    "dataType": col.get("dataType"), "columnType": col.get("columnType"),
                    "isHidden": col.get("isHidden"), "description": col.get("description"),
                    "displayFolder": col.get("displayFolder"),
                    "formatString": col.get("formatString"),
                    "expression": col.get("expression"),
                })

            for msr in tbl.get("measures", []) or []:
                add("measures", {
                    "workspaceId": ws_id, "workspaceName": ws_name,
                    "semanticModelId": ds_id, "semanticModelName": ds_name,
                    "tableName": t_name, "measureName": msr.get("name"),
                    "daxExpression": msr.get("expression"),
                    "description": msr.get("description"), "isHidden": msr.get("isHidden"),
                    "displayFolder": msr.get("displayFolder"),
                    "formatString": msr.get("formatString"),
                })

    for dash in ws.get("dashboards", []) or []:
        add("dashboards", {
            "workspaceId": ws_id, "workspaceName": ws_name,
            "dashboardId": dash.get("id"), "dashboardName": dash.get("displayName"),
            "isReadOnly": dash.get("isReadOnly"), "appId": dash.get("appId"),
            "sensitivityLabelId": (dash.get("sensitivityLabel") or {}).get("labelId"),
        })
        for tile in dash.get("tiles", []) or []:
            add("tiles", {
                "workspaceId": ws_id, "workspaceName": ws_name,
                "dashboardId": dash.get("id"), "tileId": tile.get("id"),
                "title": tile.get("title"), "reportId": tile.get("reportId"),
                "semanticModelId": tile.get("datasetId"),
            })

    for flow in ws.get("dataflows", []) or []:
        add("dataflows", {
            "workspaceId": ws_id, "workspaceName": ws_name,
            "dataflowId": flow.get("objectId"), "dataflowName": flow.get("name"),
            "description": flow.get("description"), "configuredBy": flow.get("configuredBy"),
            "modifiedDateTime": flow.get("modifiedDateTime"), "generation": flow.get("generation"),
        })
        for src in flow.get("datasourceUsages", []) or []:
            add("datasources", {
                "workspaceId": ws_id, "workspaceName": ws_name,
                "artifactType": "Dataflow", "artifactId": flow.get("objectId"),
                "artifactName": flow.get("name"),
                "datasourceInstanceId": src.get("datasourceInstanceId"),
                "gatewayId": src.get("gatewayId"), "details": as_json(src),
            })

    for key in ("lakehouses", "warehouses", "notebooks", "datamarts", "kqlDatabases",
                "eventstreams", "mirroredDatabases", "sqlEndpoints", "SparkJobDefinitions"):
        for item in ws.get(key, []) or []:
            add("other_items", {
                "workspaceId": ws_id, "workspaceName": ws_name, "itemKind": key,
                "itemId": item.get("id") or item.get("objectId"),
                "itemName": item.get("name") or item.get("displayName"),
                "description": item.get("description"),
                "configuredBy": item.get("configuredBy"),
                "modifiedDateTime": item.get("modifiedDateTime"),
            })


run_ids = FLATTEN_RUN_IDS or [RUN_ID]
raw_files = []
for rid in run_ids:
    raw_files += sorted(glob.glob(os.path.join(RAW_ROOT, rid, "*.json")))

if not raw_files:
    raise RuntimeError("No raw JSON found under Files/{}/raw/{}".format(FILES_SUBFOLDER, run_ids))

print("Flattening {} raw file(s):".format(len(raw_files)))
for path in raw_files:
    with open(path, "r", encoding="utf-8") as fh:
        payload = json.load(fh)
    CURRENT_RUN = (payload.get("_meta") or {}).get("runId", RUN_ID)
    for ws in payload.get("workspaces", []) or []:
        if WORKSPACE_NAME_FILTER and ws.get("name") not in WORKSPACE_NAME_FILTER:
            continue
        flatten_workspace(ws)
    print("  {} -> cumulative measures: {:,}".format(os.path.basename(path), len(ROWS["measures"])))

DFS = {name: pd.DataFrame(rows) for name, rows in ROWS.items() if rows}

summary = (pd.DataFrame([{"entity": k, "rows": len(v)} for k, v in DFS.items()])
             .sort_values("rows", ascending=False).reset_index(drop=True))
display(summary)

# CELL ********************

# ============================================================================
# 7. SILVER - surrogate keys, typed columns, date dimension, Delta tables
# ============================================================================
import hashlib

from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

# Direct Lake does not support DAX calculated columns, so every relationship key
# has to be materialised here rather than in the semantic model.
KEY_SPECS = {
    "workspaceKey": ["workspaceId"],
    "semanticModelKey": ["semanticModelId"],
    "modelTableKey": ["semanticModelId", "tableName"],
}

TYPE_MAP = {
    "workspaceKey": "long", "semanticModelKey": "long", "modelTableKey": "long",
    "dateKey": "int", "modifiedDateKey": "int",
    "reportCount": "int", "semanticModelCount": "int", "dashboardCount": "int",
    "dataflowCount": "int", "tableCount": "int", "columnCount": "int",
    "measureCount": "int", "daxLength": "int", "daxLineCount": "int",
    "isHidden": "boolean", "isReadOnly": "boolean",
    "date": "date", "year": "int", "quarterNumber": "int", "monthNumber": "int",
    "dayOfMonth": "int", "dayOfWeekNumber": "int",
}
TIMESTAMP_COLUMNS = {"modifiedDateTime", "createdDateTime", "createdDate"}


def surrogate_key(*parts) -> int:
    """Deterministic Int64 key - stable across runs so appends stay joinable."""
    raw = "|".join("" if p is None else str(p) for p in parts)
    return int.from_bytes(hashlib.md5(raw.encode("utf-8")).digest()[:8], "big", signed=True)


for name, pdf in DFS.items():
    for key, sources in KEY_SPECS.items():
        if all(c in pdf.columns for c in sources):
            pdf[key] = [surrogate_key(*vals) for vals in pdf[sources].itertuples(index=False)]

if "measures" in DFS:
    dax = DFS["measures"]["daxExpression"].fillna("")
    DFS["measures"]["daxLength"] = dax.str.len()
    DFS["measures"]["daxLineCount"] = dax.str.count("\n") + 1

if "reports" in DFS:
    modified = pd.to_datetime(DFS["reports"].get("modifiedDateTime"), errors="coerce", utc=True)
    DFS["reports"]["modifiedDateKey"] = modified.dt.strftime("%Y%m%d").astype("Int64")

calendar = pd.date_range("2018-01-01",
                         pd.Timestamp.utcnow().normalize().tz_localize(None) + pd.DateOffset(years=1),
                         freq="D")
DFS["date"] = pd.DataFrame({
    "dateKey": calendar.strftime("%Y%m%d").astype(int),
    "date": calendar.date,
    "year": calendar.year,
    "quarterNumber": calendar.quarter,
    "quarter": ["Q{}".format(q) for q in calendar.quarter],
    "monthNumber": calendar.month,
    "monthName": calendar.strftime("%B"),
    "yearMonth": calendar.strftime("%Y-%m"),
    "dayOfMonth": calendar.day,
    "dayName": calendar.strftime("%A"),
    "dayOfWeekNumber": calendar.dayofweek + 1,
})


def _scalar(value):
    if value is None:
        return None
    if isinstance(value, (dict, list)):
        return as_json(value)
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    return value if isinstance(value, str) else str(value)


def to_spark_df(pdf: pd.DataFrame):
    """String-first Spark DataFrame, then cast the columns that carry real types."""
    clean = pdf.copy()
    for col in clean.columns:
        clean[col] = clean[col].map(_scalar)
    schema = StructType([StructField(c, StringType(), True) for c in clean.columns])
    sdf = spark.createDataFrame(clean.astype(object), schema=schema)
    for col in clean.columns:
        if col in TYPE_MAP:
            sdf = sdf.withColumn(col, F.col(col).cast(TYPE_MAP[col]))
        elif col in TIMESTAMP_COLUMNS:
            sdf = sdf.withColumn(col, F.to_timestamp(F.regexp_replace(F.col(col), "Z$", "")))
    return sdf


written = []
for name, pdf in DFS.items():
    table = TABLE_PREFIX + name
    (to_spark_df(pdf).write
        .mode(WRITE_MODE).option("overwriteSchema", "true")
        .format("delta").saveAsTable(table))
    pdf.to_csv(os.path.join(FILES_ROOT, name + ".csv"), index=False, encoding="utf-8-sig")
    written.append({"table": table, "rows": len(pdf), "columns": len(pdf.columns)})
    print("  {:<28} {:>8,} rows".format(table, len(pdf)))

display(pd.DataFrame(written))
print("\nCSV copies: Files/{}/".format(FILES_SUBFOLDER))

# CELL ********************

# ============================================================================
# 8. GOLD - generate a human-readable Markdown data dictionary
# ============================================================================

def build_markdown() -> str:
    out = [
        "# Power BI Tenant Data Dictionary", "",
        "Generated: {} UTC  |  Run id: `{}`  |  Mode: `{}`".format(
            dt.datetime.utcnow().strftime("%Y-%m-%d %H:%M"), RUN_ID, EFFECTIVE_MODE),
        "", "## Inventory", "", "| Entity | Count |", "| --- | ---: |",
    ]
    for name, pdf in sorted(DFS.items()):
        out.append("| {} | {:,} |".format(name, len(pdf)))
    out.append("")

    models = DFS.get("semantic_models", pd.DataFrame())
    tables = DFS.get("tables", pd.DataFrame())
    cols = DFS.get("columns", pd.DataFrame())
    calc = DFS.get("calculated_columns", pd.DataFrame())
    msrs = DFS.get("measures", pd.DataFrame())
    rpts = DFS.get("reports", pd.DataFrame())
    rels = DFS.get("relationships", pd.DataFrame())

    for _, m in models.iterrows():
        mid = m["semanticModelId"]
        out += ["", "---", "",
                "## {} / {}".format(m.get("workspaceName"), m.get("semanticModelName")), "",
                "- Semantic model id: `{}`".format(mid),
                "- Owner: {}".format(m.get("configuredBy")),
                "- Storage mode: {}".format(m.get("targetStorageMode")),
                "- Endorsement: {}".format(m.get("endorsement")), ""]

        if not rpts.empty:
            linked = rpts[rpts["semanticModelId"] == mid]
            if not linked.empty:
                out += ["### Reports built on this model", ""]
                for _, r in linked.iterrows():
                    out.append("- **{}** (`{}`) - modified {}".format(
                        r.get("reportName"), r.get("reportId"), r.get("modifiedDateTime")))
                out.append("")

        if not tables.empty:
            for _, t in tables[tables["semanticModelId"] == mid].iterrows():
                tname = t.get("tableName")
                out += ["### Table: {}".format(tname), ""]
                if t.get("description"):
                    out += ["> {}".format(t.get("description")), ""]

                parts = [d[(d["semanticModelId"] == mid) & (d["tableName"] == tname)]
                         for d in (cols, calc) if not d.empty]
                tcols = pd.concat(parts) if parts else pd.DataFrame()
                if not tcols.empty:
                    out += ["| Column | Data type | Kind | Hidden | Description |",
                            "| --- | --- | --- | --- | --- |"]
                    for _, c in tcols.iterrows():
                        out.append("| {} | {} | {} | {} | {} |".format(
                            c.get("columnName"), c.get("dataType"), c.get("columnType"),
                            c.get("isHidden"), c.get("description") or ""))
                    out.append("")

                tmsr = (msrs[(msrs["semanticModelId"] == mid) & (msrs["tableName"] == tname)]
                        if not msrs.empty else pd.DataFrame())
                if not tmsr.empty:
                    out += ["#### Measures", ""]
                    for _, mm in tmsr.iterrows():
                        out += ["**{}**".format(mm.get("measureName"))]
                        if mm.get("description"):
                            out += ["", "_{}_".format(mm.get("description"))]
                        out += ["", "```dax", str(mm.get("daxExpression") or "").strip(), "```", ""]

        if not rels.empty:
            mrels = rels[rels["semanticModelId"] == mid]
            if not mrels.empty:
                out += ["### Relationships", "",
                        "| From | To | Active | Cardinality | Cross filter |",
                        "| --- | --- | --- | --- | --- |"]
                for _, r in mrels.iterrows():
                    out.append("| {}[{}] | {}[{}] | {} | {} | {} |".format(
                        r.get("fromTable"), r.get("fromColumn"), r.get("toTable"),
                        r.get("toColumn"), r.get("isActive"), r.get("cardinality"),
                        r.get("crossFilterBehavior")))
                out.append("")

    return "\n".join(out)


if GENERATE_MARKDOWN_DICTIONARY and DFS:
    markdown = build_markdown()
    path = os.path.join(FILES_ROOT, "data_dictionary_{}.md".format(RUN_ID))
    with open(path, "w", encoding="utf-8") as fh:
        fh.write(markdown)
    print("Data dictionary -> Files/{}/data_dictionary_{}.md  ({:,} chars)".format(
        FILES_SUBFOLDER, RUN_ID, len(markdown)))

# CELL ********************

# ============================================================================
# 9. Exit value - surfaces the run summary back to the calling pipeline
# ============================================================================
import notebookutils

notebookutils.notebook.exit(json.dumps({
    "runId": RUN_ID,
    "mode": EFFECTIVE_MODE,
    "rawFolder": "Files/{}/raw/{}".format(FILES_SUBFOLDER, RUN_ID),
    "rawFiles": len(raw_files),
    "tables": {TABLE_PREFIX + k: len(v) for k, v in DFS.items()},
}))

# MARKDOWN ********************

# ## What to do next
# 
# 1. Query the `pbi_*` Delta tables from the Lakehouse SQL endpoint - e.g. *"which reports
#    depend on a column named `CustomerId`"*, *"every DAX measure referencing `DIVIDE`"*,
#    *"semantic models with zero reports"*.
# 2. Build a semantic model over `pbi_measures` / `pbi_columns` / `pbi_reports` for a
#    self-service catalogue and impact-analysis report.
# 3. Set `WRITE_MODE = "append"` on the schedule to keep history and diff metadata over time.
#    Raw JSON is already versioned per run under `Files/pbi_documentation/raw/<run_id>/`.
# 4. Point a Copilot / RAG index at `Files/pbi_documentation/data_dictionary_*.md`.
# 5. To re-process history without re-calling the APIs, set `FLATTEN_RUN_IDS = ["<run_id>", ...]`
#    and run cells 6-8 only.
# 
# ### Troubleshooting
# 
# | Symptom | Fix |
# |---|---|
# | Falls back to `user` mode unexpectedly | Caller is not a Fabric Administrator, or *Enhanced admin APIs responses with detailed metadata* is off. |
# | Tables/columns/measures empty in admin mode | Enable *Enhanced admin APIs responses with DAX and mashup expressions*, then re-run. |
# | `executeQueries` 401/403 in user mode | Workspace not on Fabric/Premium capacity, XMLA endpoint not *Read*, or *Dataset Execute Queries REST API* tenant setting off. |
# | HTTP 429 | Expected - the helper backs off automatically. Scanner API allows 500 `getInfo` calls/hour and 16 concurrent scans. |
