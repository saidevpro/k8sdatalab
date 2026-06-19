import os
from datetime import datetime, timezone
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


def register_descriptions(spec):
    described = 0
    for fqn, description in spec["descriptions"].items():
        table = get_optional(f"/tables/name/{fqn}")
        if table is None:
            print(f"  skip description (table not found): {fqn}")
            continue
        call("PATCH", f"/tables/{table['id']}",
             [{"op": "add", "path": "/description", "value": description}],
             content_type="application/json-patch+json")
        described += 1
    print(f"  described {described} tables")


def register_table_tiers(spec):
    for item in spec.get("tableTiers", []):
        tables = call("GET", f"/tables?databaseSchema={DB_SERVICE}.{DB_CATALOG}.{item['schema']}&limit=1000&fields=tags").get("data", [])
        for table in tables:
            if any(t["tagFQN"] == item["tag"] for t in table.get("tags", [])):
                continue
            call("PATCH", f"/tables/{table['id']}",
                 [{"op": "add", "path": "/tags/-", "value": {
                     "tagFQN": item["tag"], "source": "Classification", "labelType": "Manual", "state": "Confirmed"}}],
                 content_type="application/json-patch+json")
        print(f"  tiered {len(tables)} {item['schema']} tables -> {item['tag']}")


def register_certifications(spec):
    for item in spec.get("certifications", []):
        tables = call("GET", f"/tables?databaseSchema={DB_SERVICE}.{DB_CATALOG}.{item['schema']}&limit=1000").get("data", [])
        for table in tables:
            call("PATCH", f"/tables/{table['id']}",
                 [{"op": "add", "path": "/certification", "value": {"tagLabel": {
                     "tagFQN": item["tag"], "source": "Classification", "labelType": "Manual", "state": "Confirmed"}}}],
                 content_type="application/json-patch+json")
        print(f"  certified {len(tables)} {item['schema']} tables -> {item['tag']}")


def register_data_insight_kpis(spec):
    def to_ms(day):
        return int(datetime.strptime(day, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp() * 1000)
    for k in spec.get("dataInsightKpis", []):
        call("PUT", "/kpi", {
            "name": k["name"], "displayName": k["displayName"], "description": k["description"],
            "dataInsightChart": k["dataInsightChart"], "metricType": k["metricType"],
            "targetValue": k["targetValue"], "startDate": to_ms(k["startDate"]), "endDate": to_ms(k["endDate"]),
        })
        print(f"  kpi {k['name']} (target {k['targetValue']}%)")


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


def register_app_database(spec):
    password = os.environ.get("APP_DB_PASSWORD")
    if not password:
        print("  APP_DB_PASSWORD not set, skipping app database registration")
        return
    svc = spec["service"]
    service = call("PUT", "/services/databaseServices", {
        "name": svc["name"], "serviceType": "Postgres",
        "connection": {"config": {
            "type": "Postgres", "scheme": "postgresql+psycopg2",
            "hostPort": svc["hostPort"], "username": svc["username"],
            "authType": {"password": password}, "database": svc["database"],
        }},
    })
    schema_filter = {"includes": [f"^{svc['schema']}$"]}
    pipelines = [
        ("metadata", "DatabaseMetadata", "0 2 * * *", {}),
        ("autoClassification", "AutoClassification", "0 5 * * *",
         {"enableAutoClassification": True, "storeSampleData": True, "confidence": 80}),
        ("profiler", "Profiler", "0 6 * * *", {"profileSample": 50}),
    ]
    for ptype, cfg_type, schedule, extra in pipelines:
        pipeline = call("PUT", "/services/ingestionPipelines", {
            "name": f"{svc['name']}_{ptype.lower()}",
            "pipelineType": ptype,
            "sourceConfig": {"config": {"type": cfg_type, "schemaFilterPattern": schema_filter, **extra}},
            "airflowConfig": {"scheduleInterval": schedule},
            "service": {"id": service["id"], "type": "databaseService"},
        })
        deploy(pipeline["id"])
        if ptype == "metadata":
            call("POST", f"/services/ingestionPipelines/trigger/{pipeline['id']}")

    base = f"{svc['name']}.{svc['database']}.{svc['schema']}"
    for item in spec["piiColumns"]:
        table = get_optional(f"/tables/name/{base}.{item['table']}?fields=columns,tags")
        if table is None:
            print(f"  skip PII tag (table not ingested yet): {item['table']}")
            continue
        index = {c["name"]: i for i, c in enumerate(table["columns"])}
        position = index.get(item["column"])
        if position is None:
            continue
        if any(t["tagFQN"] == item["tag"] for t in table["columns"][position].get("tags", [])):
            continue
        patch = [{"op": "add", "path": f"/columns/{position}/tags/-", "value": {
            "tagFQN": item["tag"], "source": "Classification", "labelType": "Manual", "state": "Confirmed"}}]
        call("PATCH", f"/tables/{table['id']}", patch, content_type="application/json-patch+json")
    print(f"  app database '{svc['name']}': service + metadata + autoClassification + PII tags")


def govern_app_database(spec):
    gov = spec.get("governance")
    if not gov:
        return
    domain = call("PUT", "/domains", {
        "name": gov["domain"]["name"], "displayName": gov["domain"]["displayName"],
        "description": gov["domain"]["description"], "domainType": gov["domain"]["domainType"],
    })

    def user_ref(name):
        user = get_optional(f"/users/name/{name}")
        return {"id": user["id"], "type": "user"} if user else None

    owner = user_ref(gov["owner"])
    experts = [r for r in (user_ref(n) for n in gov.get("experts", [])) if r]
    if owner:
        patch_refs("domains", domain["id"], "owners", [owner])
    if experts:
        patch_refs("domains", domain["id"], "experts", experts)

    base = f"{spec['service']['name']}.{spec['service']['database']}.{spec['service']['schema']}"
    for tname, conf in gov["tables"].items():
        table = get_optional(f"/tables/name/{base}.{tname}?fields=tags")
        if table is None:
            print(f"  skip app governance (table not ingested yet): {tname}")
            continue
        patch_refs("tables", table["id"], "domains", [{"id": domain["id"], "type": "domain"}])
        if owner:
            patch_refs("tables", table["id"], "owners", [owner])
        existing = {t["tagFQN"] for t in table.get("tags", [])}
        for fqn in [conf["tier"], *conf.get("tags", [])]:
            if fqn in existing:
                continue
            call("PATCH", f"/tables/{table['id']}",
                 [{"op": "add", "path": "/tags/-", "value": {"tagFQN": fqn, "source": "Classification", "labelType": "Manual", "state": "Confirmed"}}],
                 content_type="application/json-patch+json")
    print(f"  app governance: domain '{gov['domain']['name']}', owner, tiers and tags applied")


def register_data_contracts(spec):
    test_case_id = {tc["name"]: tc["id"] for tc in call("GET", "/dataQuality/testCases?limit=1000").get("data", [])}
    for c in spec["contracts"]:
        table = get_optional(f"/tables/name/{c['entity']}")
        if table is None:
            print(f"  skip contract (table not found): {c['entity']}")
            continue
        quality = [{"id": test_case_id[n], "type": "testCase"} for n in c.get("qualityExpectations", []) if n in test_case_id]
        body = {
            "name": c["name"], "displayName": c["displayName"], "description": c["description"],
            "entity": {"id": table["id"], "type": "table"},
            "entityStatus": c.get("status", "Approved"),
            "schema": c.get("schema", []),
            "semantics": c.get("semantics", []),
            "qualityExpectations": quality,
        }
        if c.get("sla"):
            body["sla"] = c["sla"]
        owner = get_optional(f"/users/name/{c['owner']}") if c.get("owner") else None
        if owner:
            body["owners"] = [{"id": owner["id"], "type": "user"}]
        call("PUT", "/dataContracts", body)
        print(f"  contract {c['name']} ({len(quality)} quality checks, {len(c.get('semantics', []))} semantics)")


def register_airflow(spec):
    password = os.environ.get("AIRFLOW_DB_PASSWORD")
    if not password:
        print("  AIRFLOW_DB_PASSWORD not set, skipping airflow connection")
        return
    svc = spec["service"]
    service = call("PUT", "/services/pipelineServices", {
        "name": svc["name"], "serviceType": "Airflow",
        "connection": {"config": {
            "type": "Airflow", "hostPort": svc["hostPort"], "numberOfStatus": svc.get("numberOfStatus", 10),
            "connection": {
                "type": "Postgres", "scheme": "postgresql+psycopg2",
                "username": svc["backend"]["username"], "authType": {"password": password},
                "hostPort": svc["backend"]["hostPort"], "database": svc["backend"]["database"],
            },
        }},
    })
    pipeline = call("PUT", "/services/ingestionPipelines", {
        "name": f"{svc['name']}_metadata", "pipelineType": "metadata",
        "sourceConfig": {"config": {"type": "PipelineMetadata"}},
        "airflowConfig": {"scheduleInterval": spec.get("schedule", "0 1 * * *")},
        "service": {"id": service["id"], "type": "pipelineService"},
    })
    deploy(pipeline["id"])
    call("POST", f"/services/ingestionPipelines/trigger/{pipeline['id']}")
    print(f"  airflow pipeline service + DAG ingestion ({svc['name']})")


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
    print("registering data-insight KPIs...")
    register_data_insight_kpis(load("governance.yaml"))
    print("certifying Trino tables...")
    register_certifications(load("governance.yaml"))
    print("tiering Trino tables...")
    register_table_tiers(load("governance.yaml"))
    print("registering profiler pipeline...")
    register_profiler(load("profiler.yaml"))
    print("registering data-quality test suites...")
    register_quality(load("quality_tests.yaml")["suites"])
    print("registering domains and data products...")
    domain_id = register_domains(load("governance_domains.yaml"))
    print("registering KPIs...")
    register_kpis(load("governance_domains.yaml"), domain_id)
    print("connecting application database and configuring PII...")
    register_app_database(load("app_database.yaml"))
    print("connecting airflow...")
    register_airflow(load("airflow.yaml"))
    print("registering org, roles, users and ownership...")
    register_org(load("governance_org.yaml"), load("governance_domains.yaml"))
    print("governing application database (domain, owner, tier, tags)...")
    govern_app_database(load("app_database.yaml"))
    print("registering data contracts...")
    register_data_contracts(load("data_contracts.yaml"))
    print("adding French table descriptions...")
    register_descriptions(load("descriptions.yaml"))
    print("done")
