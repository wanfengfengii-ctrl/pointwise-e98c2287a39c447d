import json
import re
import sys
from pathlib import Path
from checks import check
from gc_observe import ProbeObjects, run
from gc_samples import cases


def ends(trace):
    return {e["id"]: e for e in trace if e["event"] == "end"}


def baseline():
    check()
    traces = {name: run(spec) for name, spec in cases().items()}
    reader = ends(traces["reader"])
    assert reader["during"]["objects"] == ["a", "b", "rewrite-r-0", "rewrite-r-1"]
    assert [r["value"]["reading"] for r in reader["reader"]["result"]["rows"]] == [7, 7, 9, 11]
    assert reader["late"]["result"]["snapshot"] == 1
    prepared = ends(traces["prepared"])
    assert prepared["partial"]["objects"] == ["a", "b", "rewrite-p-0"]
    assert prepared["conflict"]["error"] == "PUBLISH_CONFLICT"
    assert prepared["released"]["objects"] == ["a", "b", "rewrite-p-0", "rewrite-p-1"]
    rolled = ends(traces["rollback"])
    assert rolled["read_inside"]["result"]["snapshot"] == 2
    assert rolled["tx"]["head"] == 1
    assert rolled["tx"]["objects"] == ["a", "b", "rewrite-p-0", "rewrite-p-1"]
    assert rolled["publish"]["result"] == 2
    shared = ends(traces["shared_empty"])
    assert shared["prepare"]["result"]["outputs"] == []
    assert [r["value"]["reading"] for r in shared["current"]["result"]["rows"]] == [7, 9, 11]
    reuse = ends(traces["cancel_name"])
    assert "error" not in reuse["raw_reuse"]
    objects = ProbeObjects(lambda *args: None)
    objects.put("x", [1])
    assert objects.inventory() == ["x"]
    objects.drop_raw("x")
    assert objects.inventory() == []
    objects.put("x", [2])
    assert objects.get("x") == [2]
    print("GC_OBSERVATIONS_OK: read gap, partial outputs, conflict, rollback, sharing, raw deletion and name reuse")
    return traces


def verify():
    traces = baseline()
    text = Path("DESIGN.md").read_text(encoding="utf-8")
    block = re.search(r"<!-- gc-evidence -->\s*~~~json\s*([\s\S]*?)\s*~~~", text)
    assert block, "missing gc-evidence JSON block"
    evidence = json.loads(block.group(1))
    assert set(evidence) == set(traces), "case coverage"
    for name, trace in traces.items():
        item = evidence[name]
        assert item["actual"] == trace, "baseline replay mismatch: " + name
        prediction = item["predicted"]
        assert len(prediction) == len(ends(trace))
        assert [p["id"] for p in prediction] == list(ends(trace)), "prediction order/coverage"
        assert len({p["id"] for p in prediction}) == len(prediction)
        for p in prediction:
            assert isinstance(p["outcome"], str) and p["outcome"].strip()
            assert isinstance(p["reason"], str) and p["reason"].strip()
            for key in ("protected", "deleted"):
                assert isinstance(p[key], list)
                assert all(isinstance(fid, str) for fid in p[key])
                assert len(set(p[key])) == len(p[key])
            assert not set(p["protected"]) & set(p["deleted"]), "deleting own claimed protection"
    for filename in ("lake.py", "catalog.py", "objects.py", "gc_observe.py"):
        lines = Path(filename).read_text(encoding="utf-8").splitlines()
        refs = [int(n) for n in re.findall(re.escape(filename) + r":([0-9]+)", text)]
        assert refs and all(1 <= n <= len(lines) for n in refs), "source location: " + filename
    print("DESIGN_EVIDENCE_REPLAYED: protocol safety and liveness still require design review")


if __name__ == "__main__":
    if sys.argv[1:] == ["baseline"]:
        baseline()
    elif sys.argv[1:] == ["verify"]:
        verify()
    else:
        raise SystemExit("usage: gc_check.py baseline|verify")
