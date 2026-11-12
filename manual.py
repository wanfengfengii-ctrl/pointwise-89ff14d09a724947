import json
import sys
from pipeline import canonical, run_graph
from samples import fixture

def main():
    args = sys.argv[1:]
    if args == ["demo"]:
        plan, snapshot, expected = fixture()
        result = run_graph(plan, snapshot)
        if result != expected:
            raise AssertionError("BUSINESS_RESULT")
        print(canonical({"result": result}).decode("utf-8"))
    elif len(args) == 3 and args[0] == "run":
        with open(args[1], encoding="utf-8") as handle:
            plan = json.load(handle)
        with open(args[2], encoding="utf-8") as handle:
            snapshot = json.load(handle)
        print(canonical({"result": run_graph(plan, snapshot)}).decode("utf-8"))
    elif len(args) == 2 and args[0] == "sample":
        plan, snapshot, _ = fixture()
        value = {"plan": plan, "snapshot": snapshot}[args[1]]
        print(canonical(value).decode("utf-8"))
    else:
        raise SystemExit("usage: manual.py demo | run PLAN SNAPSHOT | sample plan|snapshot")

if __name__ == "__main__":
    main()
