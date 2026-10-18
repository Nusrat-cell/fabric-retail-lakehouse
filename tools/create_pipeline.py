"""Creates (or updates) the data pipeline pl_daily_load in the current Fabric workspace.

Run it from any notebook in the workspace after the notebooks are deployed:

    exec(__import__("requests").get("https://raw.githubusercontent.com/Nusrat-cell/fabric-retail-lakehouse/main/tools/create_pipeline.py").text)

The pipeline definition itself is built by pipeline_definition() and also saved in
pipeline/pl_daily_load.json (with placeholder ids) so it can be read on GitHub.
"""
import base64
import json

PIPELINE = "pl_daily_load"
STEPS = [  # notebook, parameters (pipeline expressions start with @)
    ("01_bronze_ingest", {"batch_id": "@pipeline().RunId"}),
    ("02_silver_transform", {"batch_id": "@pipeline().RunId"}),
    ("03_gold_model", {}),
    ("04_data_quality_gate", {"run_id": "@pipeline().RunId"}),
]


def _param(v):
    if v.startswith("@"):
        return {"value": {"value": v, "type": "Expression"}, "type": "string"}
    return {"value": v, "type": "string"}


def _notebook(name, notebook_id, workspace_id, params, depends_on):
    return {
        "name": name,
        "type": "TridentNotebook",
        "dependsOn": [{"activity": d, "dependencyConditions": ["Succeeded"]} for d in depends_on],
        "policy": {"timeout": "0.01:00:00", "retry": 1, "retryIntervalInSeconds": 60,
                   "secureOutput": False, "secureInput": False},
        "typeProperties": {"notebookId": notebook_id, "workspaceId": workspace_id,
                           "parameters": {k: _param(v) for k, v in params.items()}},
    }


def pipeline_definition(ids, workspace_id):
    simulate = {
        "name": "Simulate source exports",
        "description": "Demo only: writes today's files from the tills and the webshop into Files/landing.",
        "type": "IfCondition",
        "dependsOn": [],
        "typeProperties": {
            "expression": {"value": "@not(equals(pipeline().parameters.simulate_source, 'none'))", "type": "Expression"},
            "ifTrueActivities": [_notebook("00_generate_source_data", ids["00_generate_source_data"], workspace_id,
                                           {"mode": "@pipeline().parameters.simulate_source"}, [])],
            "ifFalseActivities": [],
        },
    }
    activities, previous = [simulate], simulate["name"]
    for name, params in STEPS:
        activities.append(_notebook(name, ids[name], workspace_id, params, [previous]))
        previous = name
    return {"properties": {
        "description": "Daily load: landing files to bronze, silver and gold, then the data quality gate.",
        "activities": activities,
        "parameters": {"simulate_source": {"type": "string", "defaultValue": "next_day"}},
    }}


if "notebookutils" in globals():  # only when run inside Fabric
    import sempy.fabric as fabric

    client = fabric.FabricRestClient()
    ws = fabric.get_workspace_id()
    items = client.get(f"/v1/workspaces/{ws}/items").json()["value"]
    ids = {i["displayName"]: i["id"] for i in items if i["type"] == "Notebook"}
    body = json.dumps(pipeline_definition(ids, ws))
    definition = {"parts": [{"path": "pipeline-content.json",
                             "payload": base64.b64encode(body.encode()).decode(), "payloadType": "InlineBase64"}]}
    existing = [i for i in items if i["type"] == "DataPipeline" and i["displayName"] == PIPELINE]
    if existing:
        r = client.post(f"/v1/workspaces/{ws}/items/{existing[0]['id']}/updateDefinition", json={"definition": definition})
        print("updated", PIPELINE, r.status_code)
    else:
        r = client.post(f"/v1/workspaces/{ws}/items",
                        json={"displayName": PIPELINE, "type": "DataPipeline", "definition": definition})
        print("created", PIPELINE, r.status_code)

    # Optional: set RUN = "backfill" (first run) or "next_day" before the exec to start a run and wait for it.
    RUN = globals().get("RUN")
    if RUN:
        import time
        import datetime as dt

        items = client.get(f"/v1/workspaces/{ws}/items").json()["value"]
        pid = next(i["id"] for i in items if i["type"] == "DataPipeline" and i["displayName"] == PIPELINE)
        started = dt.datetime.utcnow() - dt.timedelta(minutes=5)
        r = client.post(f"/v1/workspaces/{ws}/items/{pid}/jobs/instances?jobType=Pipeline",
                        json={"executionData": {"parameters": {"simulate_source": RUN}}})
        job_url = r.headers["Location"].split("/v1/", 1)[1]
        job_id = job_url.rstrip("/").split("/")[-1]
        print("started run", job_id)
        while True:
            job = client.get("/v1/" + job_url).json()
            if job.get("status") in ("Completed", "Failed", "Cancelled", "Deduped"):
                break
            time.sleep(20)
        print("pipeline:", job.get("status"), job.get("failureReason") or "")
        q = {"lastUpdatedAfter": started.isoformat() + "Z",
             "lastUpdatedBefore": (dt.datetime.utcnow() + dt.timedelta(minutes=5)).isoformat() + "Z"}
        runs = client.post(f"/v1/workspaces/{ws}/datapipelines/pipelineruns/{job_id}/queryactivityruns", json=q).json()
        for a in sorted(runs.get("value", []), key=lambda a: a.get("activityRunStart") or ""):
            err = (a.get("error") or {}).get("message", "")
            print(f"  {a.get('activityName'):28} {a.get('status'):10} {a.get('durationInMs', 0) / 1000:7.1f}s {err[:300]}")
