import json
import sys

OPS = ("normalize", "calibrate", "hourly")

def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode("utf-8")

def integer(value):
    return isinstance(value, int) and not isinstance(value, bool)

def transform(op, inputs):
    if op == "normalize":
        if len(inputs) != 1 or not isinstance(inputs[0], list):
            raise ValueError("RAW_SHAPE")
        latest = {}
        for row in inputs[0]:
            if not isinstance(row, dict) or set(row) != {"id", "rev", "station", "t", "raw", "valid", "deleted"}:
                raise ValueError("RAW_ROW")
            if not all(isinstance(row[k], str) and row[k] for k in ("id", "station")):
                raise ValueError("RAW_ID")
            if not integer(row["rev"]) or row["rev"] < 1:
                raise ValueError("RAW_REV")
            if not integer(row["t"]) or not integer(row["raw"]):
                raise ValueError("RAW_NUMBER")
            if not all(isinstance(row[k], bool) for k in ("valid", "deleted")):
                raise ValueError("RAW_FLAG")
            old = latest.get(row["id"])
            if old is not None and old["rev"] == row["rev"] and old != row:
                raise ValueError("REV_CONFLICT")
            if old is None or row["rev"] > old["rev"]:
                latest[row["id"]] = dict(row)
        return [latest[k] for k in sorted(latest) if not latest[k]["deleted"]]
    if op == "calibrate":
        if len(inputs) != 2 or not isinstance(inputs[1], dict):
            raise ValueError("CALIBRATION_SHAPE")
        result = []
        for row in inputs[0]:
            rule = inputs[1].get(row["station"])
            if not isinstance(rule, dict) or set(rule) != {"scale", "offset"}:
                raise ValueError("MISSING_CALIBRATION")
            if not all(integer(rule[k]) for k in rule):
                raise ValueError("CALIBRATION_NUMBER")
            result.append({"id": row["id"], "station": row["station"], "t": row["t"],
                           "value": row["raw"] * rule["scale"] + rule["offset"], "valid": row["valid"]})
        return result
    if op == "hourly":
        groups = {}
        seen = set()
        for partition in inputs:
            if not isinstance(partition, list):
                raise ValueError("PARTITION_SHAPE")
            for row in partition:
                if row["id"] in seen:
                    raise ValueError("CROSS_PARTITION_ID")
                seen.add(row["id"])
                if row["valid"]:
                    key = (row["station"], row["t"] // 3600)
                    count, total = groups.get(key, (0, 0))
                    groups[key] = count + 1, total + row["value"]
        return [{"station": k[0], "hour": k[1], "count": groups[k][0], "sum": groups[k][1]}
                for k in sorted(groups)]
    raise ValueError("UNKNOWN_OP")

def validate_plan(plan, snapshot):
    if not isinstance(plan, dict) or set(plan) != {"nodes", "target"}:
        raise ValueError("PLAN_SHAPE")
    nodes = plan["nodes"]
    if not isinstance(nodes, list) or not nodes:
        raise ValueError("PLAN_NODES")
    by_id = {}
    for node in nodes:
        if not isinstance(node, dict) or set(node) != {"id", "op", "inputs", "reserve"}:
            raise ValueError("NODE_SHAPE")
        if not isinstance(node["id"], str) or not node["id"] or node["id"] in by_id:
            raise ValueError("NODE_ID")
        if node["op"] not in OPS or not isinstance(node["inputs"], list):
            raise ValueError("NODE_OP")
        if not integer(node["reserve"]) or node["reserve"] < 1:
            raise ValueError("NODE_RESERVE")
        by_id[node["id"]] = node
    if plan["target"] not in by_id:
        raise ValueError("TARGET")
    state = {}
    order = []
    def visit(key):
        if state.get(key) == 1:
            raise ValueError("PLAN_CYCLE")
        if state.get(key) == 2:
            return
        if key not in by_id:
            raise ValueError("NODE_MISSING")
        state[key] = 1
        for ref in by_id[key]["inputs"]:
            if not isinstance(ref, dict) or len(ref) != 1:
                raise ValueError("INPUT_REF")
            if "node" in ref:
                visit(ref["node"])
            elif "source" not in ref or ref["source"] not in snapshot:
                raise ValueError("SOURCE_MISSING")
        state[key] = 2
        order.append(key)
    for key in by_id:
        visit(key)
    return by_id, order

def run_graph(plan, snapshot):
    nodes, order = validate_plan(plan, snapshot)
    outputs = {}
    for key in order:
        node = nodes[key]
        values = [outputs[r["node"]] if "node" in r else snapshot[r["source"]] for r in node["inputs"]]
        outputs[key] = transform(node["op"], values)
    return outputs[plan["target"]]

def main():
    if len(sys.argv) != 3 or sys.argv[1] != "stage" or sys.argv[2] not in OPS:
        raise SystemExit("usage: pipeline.py stage normalize|calibrate|hourly")
    request = json.load(sys.stdin)
    result = transform(sys.argv[2], request["inputs"])
    sys.stdout.buffer.write(canonical(result) + b"\n")

if __name__ == "__main__":
    main()
