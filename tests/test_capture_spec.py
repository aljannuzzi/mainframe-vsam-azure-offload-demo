"""Static draft checks only: no IBM API calls, execution evidence or certification."""

import json
from pathlib import Path

import pytest


SPECS = Path(__file__).resolve().parents[1] / "specs"
CATALOG_PATHS = {
    "requirementIds": ("requirements",),
    "acceptanceIds": ("acceptance", "tests"),
    "decisionIds": ("decisionRegister",),
    "sourceIds": ("sources",),
    "artifactIds": ("acceptance", "artifactTemplates"),
    "ownerRoleIds": ("approval", "roles"),
    "dependsOnIds": ("delivery", "milestones"),
}
GATES = {"ADAPTER_CODE", "ZOS_PROOF", "PRODUCTION"}


def _unique_object(pairs):
    result = {}
    for key, value in pairs:
        assert key not in result, f"duplicate JSON key: {key}"
        result[key] = value
    return result


def _load(text):
    def reject_constant(value):
        raise ValueError(f"non-JSON numeric constant: {value}")

    return json.loads(text, object_pairs_hook=_unique_object, parse_constant=reject_constant)


@pytest.fixture
def spec():
    return _load((SPECS / "mainframe-capture.json").read_text(encoding="utf-8"))


def _at(value, path):
    for key in path:
        value = value[key]
    return value


def _walk(value, path="$"):
    if isinstance(value, dict):
        yield path, value
        for key, child in value.items():
            yield from _walk(child, f"{path}.{key}")
    elif isinstance(value, list):
        for index, child in enumerate(value):
            yield from _walk(child, f"{path}[{index}]")


def _text(value, label):
    assert isinstance(value, str) and value.strip(), f"{label}: nonempty text required"


def _strings(value, label, allow_empty=False):
    assert isinstance(value, list), f"{label}: list required"
    assert value or allow_empty, f"{label}: empty list"
    for item in value:
        _text(item, label)
    assert len(value) == len(set(value)), f"{label}: duplicate entry"
    return set(value)


def _fields(item, fields, label):
    for field in fields:
        assert field in item, f"{label}: missing {field}"
        _text(item[field], f"{label}.{field}")


def _nulls(item, fields, label):
    for field in fields:
        assert field in item and item[field] is None, f"{label}.{field}: must be null"


def _index(items, label, key="id"):
    assert isinstance(items, list) and items, f"{label}: nonempty catalog required"
    result = {}
    for item in items:
        assert isinstance(item, dict) and key in item, f"{label}: missing {key}"
        name = item[key]
        _text(name, f"{label}.{key}")
        assert name not in result, f"{label}: duplicate {key} {name}"
        result[name] = item
    return result


def _validate_references(spec):
    catalogs = {
        field: _index(_at(spec, path), field)
        for field, path in CATALOG_PATHS.items()
    }
    catalogs["requiredRoleIds"] = catalogs["ownerRoleIds"]
    for path in (("capabilityMatrix",), ("contracts", "interfaces"), ("failureMatrix",)):
        _index(_at(spec, path), ".".join(path))
    all_ids = set()
    for path, item in _walk(spec):
        if "id" in item:
            _text(item["id"], path)
            assert item["id"] not in all_ids, f"{path}: duplicate id {item['id']}"
            all_ids.add(item["id"])
        for field, values in item.items():
            if field.endswith("Ids"):
                assert field in catalogs, f"{path}: unknown reference type {field}"
                refs = _strings(values, f"{path}.{field}", allow_empty=True)
                missing = refs - catalogs[field].keys()
                assert not missing, f"{path}.{field}: dangling/typed references {sorted(missing)}"

    milestones = catalogs["dependsOnIds"]
    visiting, visited = set(), set()

    def visit(identifier):
        assert identifier not in visiting, f"milestone cycle: {identifier}"
        if identifier in visited:
            return
        visiting.add(identifier)
        for dependency in milestones[identifier]["dependsOnIds"]:
            visit(dependency)
        visiting.remove(identifier)
        visited.add(identifier)

    for identifier in milestones:
        visit(identifier)


def _validate_crosswalk(spec):
    requirements = _index(spec["requirements"], "requirements")
    tests = _index(spec["acceptance"]["tests"], "acceptance tests")
    assert 20 <= len(requirements) <= 35, "requirement count must be 20-35"
    assert len(tests) >= 20, "at least 20 acceptance tests required"
    forward, reverse = set(), set()
    for identifier, requirement in requirements.items():
        _fields(requirement, ("text", "risk"), identifier)
        assert requirement["level"] in {"MUST", "SHOULD"}, identifier
        for test_id in _strings(requirement["acceptanceIds"], identifier):
            forward.add((identifier, test_id))
    for identifier, test in tests.items():
        for requirement_id in _strings(test["requirementIds"], identifier):
            reverse.add((requirement_id, identifier))
    assert forward == reverse, (
        f"crosswalk asymmetry; missing on requirements: {sorted(reverse - forward)}; "
        f"missing on acceptance: {sorted(forward - reverse)}"
    )


def _validate_draft(spec):
    assert spec["schemaVersion"] == "1.0", "schemaVersion"
    assert spec["language"] == "pt-BR", "Portuguese document"
    assert spec["status"] == "DRAFT_FOR_MAINFRAME_REVIEW", "draft status"
    assert spec["implemented"] is False, "implemented must be false"
    assert spec["intendedScope"] == "balances_statement_capture_proposal", "proposal scope"
    assert spec["actualRepoImplementation"] == "synthetic_Azure_balances_only", "actual scope"
    for field in ("proposal", "included", "nonGoals", "forbiddenMechanisms", "productionRule"):
        assert spec["scope"][field], f"scope.{field}"
    assert spec["contracts"]["entityOrdering"]["integrationStatus"].startswith("NOT_IMPLEMENTED.")
    assert spec["snapshotProtocol"]["status"] == "BLOCKED"
    approval = spec["approval"]
    assert approval["status"] == "PENDING", "approval must remain pending"
    _nulls(approval, ("approvedAt", "approvedBy"), "approval")
    roles = _index(approval["roles"], "roles")
    assert _strings(approval["requiredRoleIds"], "approval roles") == roles.keys(), (
        "approval must require all roles"
    )
    assert {role["name"] for role in roles.values()} == {
        "z/OS system programmer", "CICS recovery", "batch owner", "dataset owner",
        "security", "SRE", "Azure architect",
    }, "all seven approval roles required"
    for role in roles.values():
        _fields(role, ("name", "responsibility"), role["id"])


def _validate_acceptance(spec):
    acceptance = spec["acceptance"]
    assert acceptance["executionStatus"] == "NOT_EXECUTED_ZOS", "acceptance executionStatus"
    artifacts = _index(acceptance["artifactTemplates"], "artifacts")
    for test in acceptance["tests"]:
        identifier = test["id"]
        assert test["executionStatus"] == "NOT_EXECUTED_ZOS", f"{identifier}: executionStatus"
        _fields(test, ("preconditions", "stimulus", "expected"), identifier)
        assert _strings(test["artifactIds"], identifier) <= artifacts.keys(), (
            f"{identifier}: proof artifacts required"
        )
    window = acceptance["window"]
    _fields(window, ("definition", "oracle", "drain"), "acceptance window")
    _nulls(window, ("startManifest", "endManifest", "durationSeconds", "writerCount", "uowCount"),
           "acceptance window")
    invariants = _index(window["invariants"], "invariants", "metric")
    expected = {
        "lostConfirmedEvents": "events",
        "abortedEventsApplied": "events",
        "olderOverwrites": "overwrites",
    }
    assert invariants.keys() == expected.keys(), "controlled window invariants"
    for name, invariant in invariants.items():
        assert type(invariant["target"]) is int and invariant["target"] == 0, f"{name}: zero target"
        assert invariant["unit"] == expected[name], f"{name}: invariant unit"
        _fields(invariant, ("definition",), name)


def _validate_decisions(spec):
    decisions = _index(spec["decisionRegister"], "decisions")
    adapter_decisions = {"D001", "D002", "D003", "D004", "D006", "D008", "D009", "D011", "D013"}
    assert {f"D{number:03d}" for number in range(1, 21)} <= decisions.keys()
    for identifier, decision in decisions.items():
        _fields(decision, ("topic",), identifier)
        assert decision["required"] is True, f"{identifier}: required decision"
        assert decision["status"] == "OPEN", f"{identifier}: OPEN decision"
        _nulls(decision, ("resolution", "evidence", "approvedAt", "approvedBy"), identifier)
        _strings(decision["requiredFields"], f"{identifier}.requiredFields")
        _strings(decision["ownerRoleIds"], f"{identifier}.ownerRoleIds")
        blocks = _strings(decision["blocks"], f"{identifier}.blocks")
        assert {"ZOS_PROOF", "PRODUCTION"} <= blocks <= GATES, f"{identifier}: required gates"
        if identifier in adapter_decisions:
            assert "ADAPTER_CODE" in blocks, f"{identifier}: required ADAPTER_CODE gate"
    _fields(spec["delivery"], ("gatePolicy", "draftAllowance", "llmStopRule"), "delivery")


def _validate_sources_capabilities(spec):
    for identifier, source in _index(spec["sources"], "sources").items():
        _fields(source, ("title", "url", "supports", "versionApplicability"), identifier)
        url = source["url"]
        assert url.startswith("https://www.ibm.com/docs/"), f"{identifier}: primary IBM HTTPS URL"
        assert not any(char.isspace() for char in url), f"{identifier}: URL whitespace"
        _nulls(source, ("verifiedAt",), identifier)
    expected = {
        "W001": ("supported_in_principle", "QUALIFY_BEFORE_ENABLE"),
        "W002": ("supported_in_principle", "QUALIFY_BEFORE_ENABLE"),
        "W003": ("conditional", "WAIT_VALIDATED_BOUNDARY"),
        "W004": ("boundary_unproven", "WAIT_VALIDATED_BOUNDARY_OR_EXCLUDE"),
        "W005": ("not_covered_by_default", "EXPLICIT_REBASELINE"),
    }
    capabilities = _index(spec["capabilityMatrix"], "capabilities")
    assert capabilities.keys() == expected.keys(), "expected writer modes"
    _index(spec["capabilityMatrix"], "writer modes", "writer")
    for identifier, capability in capabilities.items():
        _fields(capability, ("writer", "boundary"), identifier)
        assert capability["certified"] is False, f"{identifier}: not certified"
        _nulls(capability, ("evidence", "approvedAt", "approvedBy"), identifier)
        assert (capability["classification"], capability["disposition"]) == expected[identifier], (
            f"{identifier}: mode disposition"
        )


def _validate_contracts(spec):
    interfaces = _index(spec["contracts"]["interfaces"], "interfaces", "name")
    expected = {
        "Reader": {"open", "read", "reposition"},
        "Normalizer": {"normalize"},
        "StateStore": {"apply", "recover"},
        "Publisher": {"claim", "renew", "publish", "ack"},
    }
    assert expected.keys() <= interfaces.keys(), "named interfaces"
    for name, interface in interfaces.items():
        operations = _index(interface["operations"], name, "name")
        assert expected.get(name, set()) <= operations.keys(), f"{name}: required operations"
        for operation in operations.values():
            _fields(operation, ("input", "output", "effect"), f"{name}.{operation['name']}")
    machines = spec["stateMachines"]
    for name, expected_states in {
        "uow": {"OPEN", "COMMIT_OBSERVED", "BACKOUT_OBSERVED", "READY", "DISCARDED", "HELD"},
        "spool": {"READY", "CLAIMED", "ACKNOWLEDGED"},
    }.items():
        machine = machines[name]
        states = _strings(machine["states"], f"{name}.states")
        assert expected_states <= states, f"{name}: required states"
        assert isinstance(machine["transitions"], list) and machine["transitions"]
        seen = set()
        for transition in machine["transitions"]:
            _fields(transition, ("from", "event", "to", "guard"), name)
            assert transition["from"] in states and transition["to"] in states, f"{name}: invalid state"
            edge = (transition["from"], transition["event"], transition["to"])
            assert edge not in seen, f"{name}: duplicate transition"
            seen.add(edge)
        required_edges = {
            "uow": {
                ("OPEN", "COMMIT", "COMMIT_OBSERVED"),
                ("COMMIT_OBSERVED", "OUTSTANDING_CHANGE_OR_UNDO", "COMMIT_OBSERVED"),
                ("COMMIT_OBSERVED", "COMPLETENESS_PROVEN", "READY"),
                ("OPEN", "FULL_BACKOUT", "BACKOUT_OBSERVED"),
                ("BACKOUT_OBSERVED", "OUTSTANDING_CHANGE_OR_UNDO", "BACKOUT_OBSERVED"),
                ("BACKOUT_OBSERVED", "COMPLETENESS_PROVEN", "DISCARDED"),
                ("OPEN", "PARTIAL_BACKOUT", "OPEN"),
                ("OPEN", "IN_DOUBT", "HELD"),
            },
            "spool": {
                ("READY", "CLAIM_CAS", "CLAIMED"),
                ("CLAIMED", "RENEW_CAS", "CLAIMED"),
                ("CLAIMED", "ACK_CAS", "ACKNOWLEDGED"),
                ("CLAIMED", "RETRY_OR_EXPIRED_LEASE", "READY"),
            },
        }
        assert required_edges[name] <= seen, f"{name}: required transitions"
    checkpoints = _index(machines["checkpoints"], "checkpoints", "name")
    assert checkpoints.keys() == {
        "READ", "RECOVERABLE_RESUME", "PUBLISH_PREFIX", "APPLIEDDOWNSTREAM", "RECONCILE",
    }, "distinct checkpoint names"
    _strings([item["meaning"] for item in checkpoints.values()], "distinct checkpoint meanings")


def _validate_nfr(spec):
    nfr = spec["nfr"]
    assert nfr["status"] == "BLOCKED_PENDING_APPROVAL", "NFR status"
    _nulls(nfr, ("approvedAt", "approvedBy", "workloadDefinition"), "NFR")
    expected = {
        "sustainedCaptureRate": ("records/second", "minimum"),
        "commitToBrokerP99": ("milliseconds", "maximum"),
        "commitToVisibleP99": ("milliseconds", "maximum"),
        "maxBacklogAge": ("seconds", "maximum"),
        "maxSpoolDisk": ("bytes", "maximum"),
        "sourceLogRetention": ("seconds", "minimum"),
        "spoolRetentionAfterAck": ("seconds", "minimum"),
        "dedupRetention": ("seconds", "minimum"),
        "RPO": ("seconds", "maximum"),
        "RTO": ("seconds", "maximum"),
        "sourceCpuRegression": ("percent", "maximum"),
    }
    metrics = _index(nfr["metrics"], "NFR metrics", "name")
    assert metrics.keys() == expected.keys(), "NFR metric coverage"
    for name, metric in metrics.items():
        _nulls(metric, ("threshold",), name)
        assert (metric["unit"], metric["direction"]) == expected[name], f"{name}: unit/direction"
        _fields(metric, ("definition",), name)
    limits = nfr["limits"]
    assert isinstance(limits, dict) and {
        "maxRecordBytes", "maxUowMemoryBytes", "maxUowSpillBytes", "maxOpenUows",
        "backpressureStartBytes", "leaseSeconds", "retryMaxAttempts",
        "retryMaxDelaySeconds", "acceptanceDrainSeconds",
    } <= limits.keys(), "NFR limit coverage"
    _nulls(limits, limits.keys(), "NFR limits")
    widths = spec["contracts"]["position"]["widths"]
    _nulls(widths, ("cursorMaxBytes", "blockIdBytes", "recordOrdinalMax", "identifierMaxUtf8Bytes"),
           "unapproved position widths")


def _validate_artifacts(spec):
    paths = set()
    for artifact in spec["acceptance"]["artifactTemplates"]:
        identifier = artifact["id"]
        _fields(artifact, ("file",), identifier)
        # Treat both separators alike even when this validator runs on Linux.
        name = artifact["file"].replace("\\", "/")
        parts = name.split("/")
        assert all(part not in {"", ".", ".."} for part in parts), f"{identifier}: relative path"
        assert not any(char in name for char in ':*?"<>|'), f"{identifier}: portable path"
        assert not any(part.endswith((" ", ".")) for part in parts), f"{identifier}: portable path"
        assert name.casefold() not in paths, f"{identifier}: duplicate artifact path"
        paths.add(name.casefold())
        suffix = Path(name).suffix
        assert suffix in {".csv", ".json", ".jsonl"}, f"{identifier}: artifact format"
        field = "columns" if suffix == ".csv" else "fields"
        _strings(artifact[field], f"{identifier}.{field}")


VALIDATORS = (
    _validate_references, _validate_crosswalk, _validate_draft,
    _validate_acceptance, _validate_decisions, _validate_sources_capabilities,
    _validate_contracts, _validate_nfr, _validate_artifacts,
)


@pytest.mark.parametrize("validate", VALIDATORS, ids=lambda validate: validate.__name__)
def test_capture_spec(spec, validate):
    validate(spec)


def test_review_preserves_draft_boundary():
    review = (SPECS / "mainframe-capture-review.txt").read_text(encoding="utf-8")
    for marker in (
        "mainframe-capture.json", "DRAFT_FOR_MAINFRAME_REVIEW", "implemented=false",
        "NOT_EXECUTED_ZOS", "sintético", "somente saldo",
    ):
        assert marker in review, f"review missing draft boundary: {marker}"


@pytest.mark.parametrize("text", [
    '{"status": "DRAFT", "status": "APPROVED"}',
    '{"nested": [{"id": "CAP-001", "id": "CAP-002"}]}',
])
def test_duplicate_json_keys_rejected(text):
    with pytest.raises(AssertionError, match="duplicate JSON key"):
        _load(text)


@pytest.mark.parametrize("constant", ["NaN", "Infinity", "-Infinity"])
def test_non_json_numeric_constants_rejected(constant):
    with pytest.raises(ValueError, match="non-JSON numeric constant"):
        _load('{"threshold": ' + constant + "}")


def _deepcopy(value):
    """JSON round-trip gives independent nested copies without another dependency."""
    return json.loads(json.dumps(value))


@pytest.mark.parametrize("field", [*CATALOG_PATHS, "requiredRoleIds"])
@pytest.mark.parametrize("bad_reference", ["missing-id", "wrong-catalog", "not-a-list", "non-string"])
def test_invalid_recursive_references_rejected(spec, field, bad_reference):
    mutated = _deepcopy(spec)
    wrong_id = "AT-001" if field == "requirementIds" else "CAP-001"
    value = {
        "missing-id": ["DOES-NOT-EXIST"],
        "wrong-catalog": [wrong_id],
        "not-a-list": wrong_id,
        "non-string": [None],
    }[bad_reference]
    mutated["nestedProbe"] = [{"deeper": {field: value}}]
    with pytest.raises(AssertionError, match=field):
        _validate_references(mutated)


@pytest.mark.parametrize("path", [
    *CATALOG_PATHS.values(), ("capabilityMatrix",), ("contracts", "interfaces"), ("failureMatrix",),
])
def test_duplicate_catalog_id_rejected(spec, path):
    mutated = _deepcopy(spec)
    catalog = _at(mutated, path)
    catalog[1]["id"] = catalog[0]["id"]
    with pytest.raises(AssertionError, match="duplicate"):
        _validate_references(mutated)


@pytest.mark.parametrize("cycle", [["M001"], ["M006"]])
def test_milestone_cycle_rejected(spec, cycle):
    mutated = _deepcopy(spec)
    mutated["delivery"]["milestones"][0]["dependsOnIds"] = cycle
    with pytest.raises(AssertionError, match="milestone cycle"):
        _validate_references(mutated)


@pytest.mark.parametrize("side", ["requirements", "acceptance"])
def test_crosswalk_asymmetry_rejected(spec, side):
    _validate_crosswalk(spec)
    mutated = _deepcopy(spec)
    if side == "requirements":
        mutated["requirements"][0]["acceptanceIds"].append("AT-026")
    else:
        mutated["acceptance"]["tests"][-1]["requirementIds"].append("CAP-001")
    with pytest.raises(AssertionError, match="crosswalk asymmetry"):
        _validate_crosswalk(mutated)


@pytest.mark.parametrize("validate,path,value,error", [
    (_validate_draft, ("schemaVersion",), 1, "schemaVersion"),
    (_validate_draft, ("language",), "en", "Portuguese document"),
    (_validate_draft, ("implemented",), True, "implemented"),
    (_validate_draft, ("implemented",), 0, "implemented"),
    (_validate_draft, ("status",), "APPROVED", "draft status"),
    (_validate_draft, ("actualRepoImplementation",), "native_zos_capture", "actual scope"),
    (_validate_draft, ("approval", "approvedBy"), "invented", "must be null"),
    (_validate_draft, ("approval", "requiredRoleIds"), ["R001"], "all roles"),
    (_validate_acceptance, ("acceptance", "executionStatus"), "PASSED", "executionStatus"),
    (_validate_acceptance, ("acceptance", "tests", 0, "executionStatus"), "PASSED", "executionStatus"),
    (_validate_acceptance, ("acceptance", "tests", 0, "expected"), "", "nonempty"),
    (_validate_acceptance, ("acceptance", "tests", 0, "artifactIds"), [], "empty list"),
    (_validate_acceptance, ("acceptance", "window", "invariants", 0, "target"), False, "zero target"),
    (_validate_decisions, ("decisionRegister", 0, "status"), "CLOSED", "OPEN decision"),
    (_validate_decisions, ("decisionRegister", 0, "blocks"), ["PRODUCTION"], "required gates"),
    (_validate_decisions, ("decisionRegister", 0, "approvedAt"), "invented", "must be null"),
    (_validate_sources_capabilities, ("sources", 0, "url"), "http://www.ibm.com/docs/", "IBM HTTPS"),
    (_validate_sources_capabilities, ("sources", 0, "url"), "https://www.ibm.com.evil/docs/", "IBM HTTPS"),
    (_validate_sources_capabilities, ("capabilityMatrix", 0, "certified"), True, "not certified"),
    (_validate_sources_capabilities, ("capabilityMatrix", 0, "approvedBy"), "invented", "must be null"),
    (_validate_sources_capabilities, ("capabilityMatrix", 3, "disposition"), "ENABLE", "disposition"),
    (_validate_contracts, ("contracts", "interfaces", 0, "operations", 1, "name"), "open", "duplicate"),
    (_validate_contracts, ("stateMachines", "uow", "transitions", 0, "to"), "MISSING", "invalid state"),
    (_validate_contracts, ("stateMachines", "uow", "transitions", 2, "event"), "UNKNOWN", "required transitions"),
    (_validate_contracts, ("stateMachines", "checkpoints", 1, "name"), "READ", "duplicate"),
    (_validate_nfr, ("nfr", "metrics", 0, "threshold"), 100, "must be null"),
    (_validate_nfr, ("nfr", "metrics", 0, "unit"), "fast", "unit/direction"),
    (_validate_nfr, ("nfr", "metrics", 0, "direction"), "maximum", "unit/direction"),
    (_validate_nfr, ("nfr", "limits", "maxRecordBytes"), 100, "must be null"),
    (_validate_artifacts, ("acceptance", "artifactTemplates", 1, "file"), "WRITER-MATRIX.CSV", "duplicate"),
    (_validate_artifacts, ("acceptance", "artifactTemplates", 0, "file"), "..\\escape.csv", "relative path"),
    (_validate_artifacts, ("acceptance", "artifactTemplates", 0, "columns"), [], "empty list"),
])
def test_invalid_claims_and_contracts_rejected(spec, validate, path, value, error):
    mutated = _deepcopy(spec)
    _at(mutated, path[:-1])[path[-1]] = value
    with pytest.raises(AssertionError, match=error):
        validate(mutated)
