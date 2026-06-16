import os
from pathlib import Path

import requests
import yaml

CONFIG_DIR = Path(__file__).parent / "configs"
HOST_PORT = os.environ.get("OPENMETADATA_HOST_PORT", "http://openmetadata.observability.svc.cluster.local:8585/api")
JWT_TOKEN = os.environ["OPENMETADATA_JWT_TOKEN"]
DB_SERVICE = os.environ.get("OPENMETADATA_DB_SERVICE", "trino")
PROFILER_SCHEDULE = os.environ.get("PROFILER_SCHEDULE", "0 3 * * *")
QUALITY_SCHEDULE = os.environ.get("QUALITY_SCHEDULE", "0 4 * * *")

session = requests.Session()
session.headers.update({"Authorization": f"Bearer {JWT_TOKEN}", "Content-Type": "application/json"})


def load(name):
    with open(CONFIG_DIR / name) as f:
        return yaml.safe_load(f)


def call(method, path, payload=None, content_type=None):
    headers = {"Content-Type": content_type} if content_type else None
    resp = session.request(method, f"{HOST_PORT}{path}", json=payload, headers=headers)
    if resp.status_code >= 400:
        print(f"  {method} {path} -> {resp.status_code} {resp.text[:300]}")
        resp.raise_for_status()
    return resp.json() if resp.content else {}


def deploy(pipeline_id):
    call("POST", f"/services/ingestionPipelines/deploy/{pipeline_id}")


def bootstrap_governance(spec):
    glossary = spec["glossary"]
    call("PUT", "/glossaries", {k: glossary[k] for k in ("name", "displayName", "description")})
    for term in glossary["terms"]:
        call("PUT", "/glossaryTerms", {"glossary": glossary["name"], **term})

    for clf in spec["classifications"]:
        call("PUT", "/classifications", {"name": clf["name"], "description": clf["description"]})
        for tag in clf["tags"]:
            call("PUT", "/tags", {"classification": clf["name"], **tag})

    for item in spec["tierAssignments"]:
        schema = call("GET", f"/databaseSchemas/name/{item['schema']}?fields=tags")
        if any(t["tagFQN"] == item["tag"] for t in schema.get("tags", [])):
            continue
        patch = [{"op": "add", "path": "/tags/-", "value": {"tagFQN": item["tag"], "source": "Classification"}}]
        call("PATCH", f"/databaseSchemas/{schema['id']}", patch, content_type="application/json-patch+json")


def register_profiler(profiler):
    service = call("GET", f"/services/databaseServices/name/{DB_SERVICE}")
    pipeline = call("PUT", "/services/ingestionPipelines", {
        "name": f"{DB_SERVICE}_medallion_profiler",
        "pipelineType": "profiler",
        "sourceConfig": {"config": profiler["source"]["sourceConfig"]["config"]},
        "airflowConfig": {"scheduleInterval": PROFILER_SCHEDULE},
        "service": {"id": service["id"], "type": "databaseService"},
    })
    deploy(pipeline["id"])


def register_quality(suites):
    for suite in suites:
        entity = suite["entity"]
        suite_fqn = f"{entity}.testSuite"
        existing = session.get(f"{HOST_PORT}/dataQuality/testSuites/name/{suite_fqn}")
        if existing.status_code == 200:
            suite_obj = existing.json()
        else:
            suite_obj = call("POST", "/dataQuality/testSuites/executable", {
                "name": suite_fqn,
                "description": f"Data-quality suite for {entity}",
                "executableEntityReference": entity,
            })
        for test in suite["tests"]:
            column = test.get("columnName")
            entity_link = f"<#E::table::{entity}::columns::{column}>" if column else f"<#E::table::{entity}>"
            call("PUT", "/dataQuality/testCases", {
                "name": test["name"],
                "entityLink": entity_link,
                "testSuite": f"{entity}.testSuite",
                "testDefinition": test["testDefinitionName"],
                "parameterValues": test.get("parameterValues", []),
            })
        pipeline = call("PUT", "/services/ingestionPipelines", {
            "name": f"{entity.replace('.', '_')}_quality",
            "pipelineType": "TestSuite",
            "sourceConfig": {"config": {"type": "TestSuite", "entityFullyQualifiedName": entity}},
            "airflowConfig": {"scheduleInterval": QUALITY_SCHEDULE},
            "service": {"id": suite_obj["id"], "type": "testSuite"},
        })
        deploy(pipeline["id"])


if __name__ == "__main__":
    print("bootstrapping governance vocabulary...")
    bootstrap_governance(load("governance.yaml"))
    print("registering profiler pipeline...")
    register_profiler(load("profiler.yaml"))
    print("registering data-quality test suites...")
    register_quality(load("quality_tests.yaml")["suites"])
    print("done")
