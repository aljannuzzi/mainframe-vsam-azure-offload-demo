"""Static checks for design examples, not a COBOL compiler or z/OS acceptance."""
import json
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[1]
EXAMPLES = ROOT / "samples" / "mainframe"


@pytest.mark.parametrize("name", [
    "CAPTURE-EVENT.cpy.example",
    "CAPTURE-STATE.cbl.example",
    "PUBLISH-ACK.cbl.example",
    "BATCH-GATE.cbl.example",
])
def test_code_examples_declare_their_unimplemented_scope(name):
    text = (EXAMPLES / name).read_text(encoding="utf-8")
    assert "DESIGN EXAMPLE - NOT COMPILED OR PRODUCTION READY" in text
    assert "CALL 'IXGBRWSE'" not in text and 'CALL "IXGBRWSE"' not in text


def test_example_acceptances_reference_the_normative_spec():
    spec = json.loads((ROOT / "specs" / "mainframe-capture.json").read_text(encoding="utf-8"))
    cases = json.loads((EXAMPLES / "acceptance-cases.json").read_text(encoding="utf-8"))
    assert cases["status"] == "SPECIFICATION_ONLY_NOT_EXECUTED_ZOS"
    allowed = {case["id"] for case in spec["acceptance"]["tests"]}
    assert len({case["id"] for case in cases["cases"]}) == len(cases["cases"])
    for case in cases["cases"]:
        assert case["acceptanceIds"] and set(case["acceptanceIds"]) <= allowed
        assert case["steps"] and case["expected"]
    assert "BACKOUT_OBSERVED" in cases["cases"][1]["expected"]


def test_prompts_require_decisions_and_evidence_not_fabricated_approval():
    text = (EXAMPLES / "llm-prompts.txt").read_text(encoding="utf-8")
    for word in ("BLOCKED", "D001", "D020", "CAP-", "AT-", "BACKOUT_OBSERVED"):
        assert word in text
    assert "não solicite cadeia interna de pensamento" in text


def test_open_source_review_is_scoped_and_has_source_links():
    data = json.loads((ROOT / "specs" / "opensource-vsam-evaluation.json").read_text(encoding="utf-8"))
    assert data["reviewDate"] == "2026-09-23"
    assert data["method"]["limitations"]
    for project in data["projects"]:
        assert project["url"].startswith("https://github.com/")
        assert project["role"] and project["gap"]
        assert project["qualifiedAsVsamCdc"] is False
