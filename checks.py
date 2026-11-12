import copy
import sys
from pipeline import canonical, run_graph, transform
from samples import fixture

def must_reject(call, code):
    try:
        call()
    except ValueError as exc:
        assert str(exc) == code, (str(exc), code)
    else:
        raise AssertionError("EXPECTED_" + code)

def normal():
    plan, snapshot, expected = fixture()
    before = canonical([plan, snapshot])
    assert run_graph(plan, snapshot) == expected
    assert canonical([plan, snapshot]) == before
    reversed_plan = copy.deepcopy(plan)
    reversed_plan["nodes"].reverse()
    assert run_graph(reversed_plan, snapshot) == expected
    changed = copy.deepcopy(snapshot)
    changed["east"].append(dict(id="a", rev=1, station="E", t=0, raw=1, valid=True, deleted=False))
    assert run_graph(plan, changed) == expected
    changed["east"].append(dict(id="a", rev=3, station="E", t=3600, raw=-1, valid=True, deleted=False))
    assert run_graph(plan, changed) == [
        {"station": "E", "hour": 0, "count": 1, "sum": 41},
        {"station": "E", "hour": 1, "count": 1, "sum": -1},
        {"station": "W", "hour": -1, "count": 1, "sum": 13},
        {"station": "W", "hour": 1, "count": 1, "sum": 19}]
    conflict = copy.deepcopy(snapshot)
    conflict["east"].append(dict(id="a", rev=2, station="E", t=0, raw=99, valid=True, deleted=False))
    must_reject(lambda: run_graph(plan, conflict), "REV_CONFLICT")
    bad = copy.deepcopy(plan)
    bad["nodes"][0]["inputs"] = [{"node": "hourly"}]
    must_reject(lambda: run_graph(bad, snapshot), "PLAN_CYCLE")
    missing = copy.deepcopy(snapshot)
    del missing["calibration"]["E"]
    must_reject(lambda: run_graph(plan, missing), "MISSING_CALIBRATION")
    must_reject(lambda: transform("hourly", [[{"id": "same", "station": "A", "t": 0, "value": 1, "valid": True}]] * 2),
                "CROSS_PARTITION_ID")
    print("BUSINESS_REGRESSION_OK")

if __name__ == "__main__":
    if sys.argv[1:] != ["normal"]:
        raise SystemExit("usage: checks.py normal")
    normal()
