def fixture():
    def row(key, rev, station, t, raw, valid=True, deleted=False):
        return dict(id=key, rev=rev, station=station, t=t, raw=raw, valid=valid, deleted=deleted)
    snapshot = {
        "east": [row("a", 1, "E", 0, 10), row("a", 2, "E", 0, 12),
                 row("b", 1, "E", 3599, 20), row("z", 1, "E", 0, 999, False)],
        "west": [row("c", 1, "W", -1, 5), row("d", 1, "W", 3600, 7),
                 row("gone", 1, "W", 0, 100), row("gone", 2, "W", 0, 100, True, True)],
        "calibration": {"E": {"scale": 2, "offset": 1}, "W": {"scale": 3, "offset": -2}}
    }
    def node(key, op, refs):
        return {"id": key, "op": op, "inputs": refs, "reserve": 4096}
    plan = {"nodes": [
        node("east_raw", "normalize", [{"source": "east"}]),
        node("west_raw", "normalize", [{"source": "west"}]),
        node("east_cal", "calibrate", [{"node": "east_raw"}, {"source": "calibration"}]),
        node("west_cal", "calibrate", [{"node": "west_raw"}, {"source": "calibration"}]),
        node("hourly", "hourly", [{"node": "east_cal"}, {"node": "west_cal"}])
    ], "target": "hourly"}
    expected = [
        {"station": "E", "hour": 0, "count": 2, "sum": 66},
        {"station": "W", "hour": -1, "count": 1, "sum": 13},
        {"station": "W", "hour": 1, "count": 1, "sum": 19}
    ]
    return plan, snapshot, expected
