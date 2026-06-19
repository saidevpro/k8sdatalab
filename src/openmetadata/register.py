import os
from pathlib import Path

import requests
import yaml

CONFIG_DIR = Path(__file__).parent / "configs"
HOST_PORT = os.environ.get("OPENMETADATA_HOST_PORT", "http://openmetadata.observability.svc.cluster.local:8585/api")
JWT_TOKEN = os.environ["OPENMETADATA_JWT_TOKEN"]
DB_SERVICE = os.environ.get("OPENMETADATA_DB_SERVICE", "Trino")
DB_CATALOG = os.environ.get("OPENMETADATA_DB_CATALOG", "lakehouse")
DQ_SCHEMAS = [s.strip() for s in os.environ.get("DQ_SCHEMAS", "silver,gold").split(",")]
DQ_SUITE = os.environ.get("DQ_SUITE_NAME", "medallion_quality")
PROFILER_SCHEDULE = os.environ.get("PROFILER_SCHEDULE", "0 3 * * *")
QUALITY_SCHEDULE = os.environ.get("QUALITY_SCHEDULE", "0 4 * * *")

session = requests.Session()
session.headers.update({"Authorization": f"Bearer {JWT_TOKEN}", "Content-Type": "application/json"})


def load(name):
    with open(CONFIG_DIR / name) as f:
        return yaml.safe_load(f)


def call(method, path, payload=None, content_type=None):
    headers = {"Content-Type": content_type} if content_type else None
    resp = session.request(method, f"{HOST_PORT}/v1{path}", json=payload, headers=headers)
    if resp.status_code >= 400:
        print(f"  {method} {path} -> {resp.status_code} {resp.text[:300]}")
        resp.raise_for_status()
    return resp.json() if resp.content else {}


def get_optional(path):
    resp = session.get(f"{HOST_PORT}/v1{path}")
    if resp.status_code == 404:
        return None
    resp.raise_for_status()
    return resp.json()


def discovered_schemas():
    databases = call("GET", f"/databases?service={DB_SERVICE}&limit=100").get("data", [])
    schemas = []
    for db in databases:
        page = call("GET", f"/databaseSchemas?database={db['fullyQualifiedName']}&limit=100")
        schemas += [s["fullyQualifiedName"] for s in page.get("data", [])]
    return schemas


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

    print(f"  schemas known to '{DB_SERVICE}': {discovered_schemas()}")
    for item in spec["tierAssignments"]:
        schema = get_optional(f"/databaseSchemas/name/{item['schema']}?fields=tags")
        if schema is None:
            print(f"  skip tier tag, schema not found: {item['schema']}")
            continue
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


def baseline_tests(table):
    name = table["name"]
    tests = [{
        "name": f"{name}_row_count_positive",
        "testDefinitionName": "tableRowCountToBeBetween",
        "parameterValues": [{"name": "minValue", "value": "1"}, {"name": "maxValue", "value": "1000000000"}],
    }]
    columns = table.get("columns", [])
    if columns:
        key = columns[0]["name"]
        tests.append({
            "name": f"{name}_{key}_not_null",
            "testDefinitionName": "columnValuesToBeNotNull",
            "columnName": key,
        })
    return tests


def create_test_case(entity, test):
    column = test.get("columnName")
    entity_link = f"<#E::table::{entity}::columns::{column}>" if column else f"<#E::table::{entity}>"
    return call("PUT", "/dataQuality/testCases", {
        "name": test["name"],
        "entityLink": entity_link,
        "testDefinition": test["testDefinitionName"],
        "parameterValues": test.get("parameterValues", []),
    })


def register_quality(curated_suites):
    curated = {s["entity"]: s["tests"] for s in curated_suites}
    case_ids = []
    for schema in DQ_SCHEMAS:
        tables = call("GET", f"/tables?databaseSchema={DB_SERVICE}.{DB_CATALOG}.{schema}&fields=columns&limit=1000")
        for table in tables.get("data", []):
            entity = table["fullyQualifiedName"]
            tests = curated.get(entity) or baseline_tests(table)
            case_ids += [create_test_case(entity, test)["id"] for test in tests]
            print(f"  {entity}: {len(tests)} tests")
    suite = call("PUT", "/dataQuality/testSuites", {
        "name": DQ_SUITE,
        "description": "All silver/gold medallion data-quality tests, executed in a single run.",
    })
    call("PUT", "/dataQuality/testCases/logicalTestCases", {"testSuiteId": suite["id"], "testCaseIds": case_ids})
    pipeline = call("PUT", "/services/ingestionPipelines", {
        "name": f"{DQ_SUITE}_run",
        "pipelineType": "TestSuite",
        "sourceConfig": {"config": {"type": "TestSuite", "entityFullyQualifiedName": DQ_SUITE}},
        "airflowConfig": {"scheduleInterval": QUALITY_SCHEDULE},
        "service": {"id": suite["id"], "type": "testSuite"},
    })
    deploy(pipeline["id"])
    print(f"  logical suite '{DQ_SUITE}': {len(case_ids)} test cases across {len(DQ_SCHEMAS)} schemas, 1 pipeline")


def patch_refs(collection, entity_id, field, refs):
    patch = [{"op": "add", "path": f"/{field}", "value": refs}]
    call("PATCH", f"/{collection}/{entity_id}", patch, content_type="application/json-patch+json")


def assign_domain(collection, entity_id, domain_id):
    patch_refs(collection, entity_id, "domains", [{"id": domain_id, "type": "domain"}])


def table_ref(short_fqn):
    return get_optional(f"/tables/name/{DB_SERVICE}.{DB_CATALOG}.{short_fqn}")


def register_domains(spec):
    domain_id = {}
    for d in spec["domains"]:
        domain = call("PUT", "/domains", {
            "name": d["name"], "displayName": d["displayName"],
            "description": d["description"], "domainType": d["domainType"],
        })
        domain_id[d["name"]] = domain["id"]
        for short in d["tables"]:
            table = table_ref(short)
            if table is None:
                print(f"  skip domain asset, table not found: {short}")
                continue
            assign_domain("tables", table["id"], domain["id"])
        print(f"  domain '{d['name']}': {len(d['tables'])} tables")

    for dp in spec["dataProducts"]:
        call("PUT", "/dataProducts", {
            "name": dp["name"], "displayName": dp["displayName"],
            "description": dp["description"], "domains": [dp["domain"]],
        })
        assets = [{"id": t["id"], "type": "table"} for t in (table_ref(s) for s in dp["assets"]) if t]
        if assets:
            call("PUT", f"/dataProducts/{dp['name']}/assets/add", {"assets": assets})
        print(f"  data product '{dp['name']}': {len(assets)} assets")
    return domain_id


def register_kpis(spec, domain_id):
    for k in spec["kpis"]:
        payload = {
            "name": k["name"], "displayName": k["displayName"], "description": k["description"],
            "metricType": k["metricType"], "granularity": k.get("granularity", "DAY"),
            "metricExpression": {"language": "SQL", "code": k["sql"]},
        }
        if k.get("unit"):
            payload["unitOfMeasurement"] = k["unit"]
        metric = call("PUT", "/metrics", payload)
        if k.get("domain") in domain_id:
            assign_domain("metrics", metric["id"], domain_id[k["domain"]])
        print(f"  kpi '{k['name']}'")


def register_org(org, domains):
    for p in org["policies"]:
        call("PUT", "/policies", {
            "name": p["name"], "description": p["description"],
            "rules": [{"name": f"{p['name']}Rule", "resources": ["All"], "operations": p["operations"], "effect": "allow"}],
        })
    role_id = {}
    for r in org["roles"]:
        role_id[r["name"]] = call("PUT", "/roles", {"name": r["name"], "description": r["description"], "policies": r["policies"]})["id"]
    for builtin in ("DataSteward", "DataConsumer"):
        role_id[builtin] = call("GET", f"/roles/name/{builtin}")["id"]

    org_id = call("GET", "/teams/name/Organization")["id"]
    bu = org["businessUnit"]
    team_id = {bu["name"]: call("PUT", "/teams", {
        "name": bu["name"], "displayName": bu["displayName"], "description": bu["description"],
        "teamType": "BusinessUnit", "parents": [org_id],
    })["id"]}
    squad_team, squad_owner = {}, {}
    for s in org["squads"]:
        team_id[s["name"]] = call("PUT", "/teams", {
            "name": s["name"], "displayName": s["displayName"], "teamType": "Group", "parents": [team_id[bu["name"]]],
        })["id"]
        squad_team[s["domain"]] = team_id[s["name"]]
        squad_owner[s["domain"]] = s["owner"]

    user_id = {}
    for u in org["users"]:
        user_id[u["name"]] = call("PUT", "/users", {
            "name": u["name"], "displayName": u["displayName"], "email": u["email"], "description": u["description"],
            "roles": [role_id[u["role"]]], "teams": [team_id[t] for t in u["teams"]],
        })["id"]
        print(f"  user {u['name']} ({u['role']})")

    steward = {"id": user_id[org["steward"]], "type": "user"}
    engineer = {"id": user_id[org["engineer"]], "type": "user"}

    for d in domains["domains"]:
        dom = call("GET", f"/domains/name/{d['name']}")
        owner_user = {"id": user_id[squad_owner[d["name"]]], "type": "user"}
        patch_refs("domains", dom["id"], "owners", [{"id": squad_team[d["name"]], "type": "team"}])
        patch_refs("domains", dom["id"], "experts", [owner_user, steward])
        for short in d["tables"]:
            table = table_ref(short)
            if table:
                patch_refs("tables", table["id"], "owners", [{"id": squad_team[d["name"]], "type": "team"}])
        print(f"  domain {d['name']}: squad ownership + {len(d['tables'])} tables")

    for dp in domains["dataProducts"]:
        prod = call("GET", f"/dataProducts/name/{dp['name']}")
        patch_refs("dataProducts", prod["id"], "owners", [{"id": user_id[squad_owner[dp["domain"]]], "type": "user"}])
    for k in domains["kpis"]:
        metric = call("GET", f"/metrics/name/{k['name']}")
        patch_refs("metrics", metric["id"], "owners", [{"id": user_id[squad_owner[k["domain"]]], "type": "user"}])

    glossary = get_optional(f"/glossaries/name/{org['glossaryName']}")
    if glossary:
        patch_refs("glossaries", glossary["id"], "owners", [{"id": user_id[org["glossaryOwner"]], "type": "user"}])
        patch_refs("glossaries", glossary["id"], "reviewers", [steward])

    targets = (f"{DB_SERVICE}_medallion_profiler", f"{DQ_SUITE}_run")
    for pipeline in call("GET", "/services/ingestionPipelines?limit=200").get("data", []):
        if pipeline["name"] in targets:
            patch_refs("services/ingestionPipelines", pipeline["id"], "owners", [engineer])
    print(f"  ownership assigned across domains, products, KPIs, glossary and pipelines")


if __name__ == "__main__":
    print("bootstrapping governance vocabulary...")
    bootstrap_governance(load("governance.yaml"))
    print("registering profiler pipeline...")
    register_profiler(load("profiler.yaml"))
    print("registering data-quality test suites...")
    register_quality(load("quality_tests.yaml")["suites"])
    print("registering domains and data products...")
    domain_id = register_domains(load("governance_domains.yaml"))
    print("registering KPIs...")
    register_kpis(load("governance_domains.yaml"), domain_id)
    print("registering org, roles, users and ownership...")
    register_org(load("governance_org.yaml"), load("governance_domains.yaml"))
    print("done")
