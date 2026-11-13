"""
ops_checks.py — 发布运行层的真实上线回归与演示。

  python3 ops_checks.py verify   重跑原业务断言 + 实际部署恢复核对
  python3 ops_checks.py demo     展示安装版真实小时结果及一次共享执行恢复状态

全部使用真实 stage 子进程与真实安装，不用假 worker。
"""

import json
import os
import shutil
import signal
import subprocess
import sys
import tempfile
import time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

from pipeline import run_graph  # noqa: E402
from samples import fixture  # noqa: E402

OPS = [sys.executable, os.path.join(HERE, "ops.py")]
CHECKS = [sys.executable, os.path.join(HERE, "checks.py")]

BIG_RESERVE = 60_000_000
INSTALL_BYTES = 600_000_000


def fail(message):
    raise AssertionError(message)


def run(args, cwd=None, ok=True, timeout=120):
    proc = subprocess.run(args, cwd=cwd, stdout=subprocess.PIPE,
                          stderr=subprocess.PIPE, timeout=timeout)
    out = proc.stdout.decode("utf-8", "replace").strip()
    err = proc.stderr.decode("utf-8", "replace").strip()
    if ok and proc.returncode != 0:
        fail("command failed rc=%d: %s\nstdout=%s\nstderr=%s"
             % (proc.returncode, " ".join(args), out, err))
    if not ok and proc.returncode == 0:
        fail("command unexpectedly succeeded: %s\nstdout=%s" % (" ".join(args), out))
    return proc.returncode, out, err


def run_json(args, cwd=None, timeout=120):
    _rc, out, _err = run(args, cwd=cwd, timeout=timeout)
    return json.loads(out)


def run_error(args, cwd=None):
    rc, out, _err = run(args, cwd=cwd, ok=False)
    if rc != 2:
        fail("expected exit 2, got %d: %s" % (rc, " ".join(args)))
    return json.loads(out)


class Serve(object):
    """协调器进程句柄。"""

    def __init__(self, root, ops=None):
        self.root = root
        self.ops = ops or OPS
        self.proc = None

    def start(self):
        self.proc = subprocess.Popen(
            self.ops + ["serve", self.root],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        deadline = time.time() + 15
        while time.time() < deadline:
            if os.path.isfile(os.path.join(self.root, "usage.json")):
                return self
            if self.proc.poll() is not None:
                fail("serve exited early rc=%s" % self.proc.returncode)
            time.sleep(0.1)
        fail("serve did not start in time")

    def kill9(self):
        if self.proc and self.proc.poll() is None:
            self.proc.kill()
        self.wait()

    def stop(self):
        if self.proc and self.proc.poll() is None:
            self.proc.terminate()
        self.wait()

    def wait(self):
        if self.proc:
            try:
                self.proc.wait(timeout=15)
            except subprocess.TimeoutExpired:
                self.proc.kill()
                self.proc.wait()

    def alive(self):
        return self.proc is not None and self.proc.poll() is None


def write_json(path, value):
    with open(path, "w", encoding="utf-8") as fh:
        fh.write(json.dumps(value, ensure_ascii=False))


def status(root, bid, ops=None):
    return run_json((ops or OPS) + ["status", root, bid])


def wait_state(root, bid, states, timeout=90, ops=None):
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        last = status(root, bid, ops=ops)
        if last["state"] in states:
            return last
        time.sleep(0.25)
    fail("batch %s did not reach %s in time (last=%s)" % (bid, states, last))


def usage(root):
    try:
        with open(os.path.join(root, "usage.json"), "r", encoding="utf-8") as fh:
            return json.load(fh)
    except (OSError, ValueError):
        return {}


def exec_dirs(root):
    base = os.path.join(root, "exec")
    try:
        return [os.path.join(base, d) for d in os.listdir(base)]
    except OSError:
        return []


def wait_executor(root, timeout=30):
    deadline = time.time() + timeout
    while time.time() < deadline:
        for path in exec_dirs(root):
            pid_file = os.path.join(path, "pid.json")
            if os.path.isfile(pid_file):
                with open(pid_file, "r", encoding="utf-8") as fh:
                    return path, json.load(fh)["pid"]
        time.sleep(0.1)
    fail("no executor appeared")


def pid_alive(pid):
    try:
        os.kill(pid, 0)
        return True
    except OSError:
        return False


def big_fixture(rows, tag=""):
    def row(key, station, t, raw):
        return dict(id=key, rev=1, station=station, t=t, raw=raw,
                    valid=True, deleted=False)
    snapshot = {
        "east": [row("e%s-%d" % (tag, i), "E", i % 7200, i % 50) for i in range(rows)],
        "west": [row("w%s-%d" % (tag, i), "W", i % 7200, i % 7) for i in range(rows)],
        "calibration": {"E": {"scale": 2, "offset": 1}, "W": {"scale": 3, "offset": -2}},
    }

    def node(key, op, refs):
        return {"id": key, "op": op, "inputs": refs, "reserve": BIG_RESERVE}
    plan = {"nodes": [
        node("east_raw", "normalize", [{"source": "east"}]),
        node("west_raw", "normalize", [{"source": "west"}]),
        node("east_cal", "calibrate", [{"node": "east_raw"}, {"source": "calibration"}]),
        node("west_cal", "calibrate", [{"node": "west_raw"}, {"source": "calibration"}]),
        node("hourly", "hourly", [{"node": "east_cal"}, {"node": "west_cal"}]),
    ], "target": "hourly"}
    return plan, snapshot


def tiny_reserve_plan(base_plan, hourly_reserve):
    plan = json.loads(json.dumps(base_plan))
    for node in plan["nodes"]:
        if node["id"] == "hourly":
            node["reserve"] = hourly_reserve
    return plan


def section(name):
    print("== %s" % name, flush=True)


# ---------------------------------------------------------------------------
# verify
# ---------------------------------------------------------------------------

def verify():
    tmp = tempfile.mkdtemp(prefix="ops-verify-")
    serves = []
    renamed = []
    succeeded = False
    try:
        section("原业务断言")
        _rc, out, _err = run(CHECKS + ["normal"])
        if "BUSINESS_REGRESSION_OK" not in out:
            fail("business regression missing: %s" % out)
        print(out.strip())

        section("构建可复现与包校验")
        pkg_a = os.path.join(tmp, "pkgA.zip")
        rel_a = run_json(OPS + ["build", pkg_a])["release"]
        pkg_a2 = os.path.join(tmp, "pkgA2.zip")
        rel_a2 = run_json(OPS + ["build", pkg_a2])["release"]
        if rel_a != rel_a2:
            fail("build not reproducible: %s != %s" % (rel_a, rel_a2))
        root = os.path.join(tmp, "运行 根")  # 含空格与非 ASCII
        bad = os.path.join(tmp, "tampered.zip")
        data = bytearray(open(pkg_a, "rb").read())
        data[len(data) // 2] ^= 0x01
        open(bad, "wb").write(bytes(data))
        run_error(OPS + ["install", root, bad, "2", str(INSTALL_BYTES)])
        trunc = os.path.join(tmp, "truncated.zip")
        open(trunc, "wb").write(open(pkg_a, "rb").read()[:100])
        run_error(OPS + ["install", root, trunc, "2", str(INSTALL_BYTES)])
        import zipfile
        illegal = os.path.join(tmp, "illegal.zip")
        shutil.copyfile(pkg_a, illegal)
        with zipfile.ZipFile(illegal, "a") as zf:
            zf.writestr("evil.txt", "x")
        run_error(OPS + ["install", root, illegal, "2", str(INSTALL_BYTES)])
        if os.path.exists(os.path.join(root, "install.json")):
            fail("bad package changed run root")

        section("安装与配额一致性")
        rel_inst = run_json(OPS + ["install", root, pkg_a, "2", str(INSTALL_BYTES)])["release"]
        if rel_inst != rel_a:
            fail("install release mismatch")
        run_error(OPS + ["install", root, pkg_a, "3", str(INSTALL_BYTES)])
        run_error(OPS + ["install", root, pkg_a, "2", str(INSTALL_BYTES + 1)])
        run_json(OPS + ["install", root, pkg_a, "2", str(INSTALL_BYTES)])  # 幂等
        label = None
        ldir = os.path.join(root, "launchd")
        names = os.listdir(ldir)
        if len(names) != 1 or not names[0].endswith(".plist"):
            fail("launchd plist missing: %s" % names)
        label = names[0][:-7]
        plist = open(os.path.join(ldir, names[0]), encoding="utf-8").read()
        for needle in (sys.executable, os.path.join(root, "ops.py"), "serve",
                       "KeepAlive", "RunAtLoad", label):
            if needle not in plist:
                fail("plist missing %r" % needle)

        section("提交幂等与冲突拒绝")
        plan, snapshot, expected = fixture()
        plan_p = os.path.join(tmp, "plan.json")
        snap_p = os.path.join(tmp, "snap.json")
        write_json(plan_p, plan)
        write_json(snap_p, snapshot)
        st = run_json(OPS + ["submit", root, "b1", plan_p, snap_p])
        if st["state"] != "queued" or st["release"] != rel_a:
            fail("unexpected submit status: %s" % st)
        st2 = run_json(OPS + ["submit", root, "b1", plan_p, snap_p])
        if st2["release"] != rel_a:
            fail("idempotent resubmit changed binding")
        changed = json.loads(json.dumps(snapshot))
        changed["east"].append(dict(id="a", rev=9, station="E", t=0, raw=1,
                                    valid=True, deleted=False))
        snap2_p = os.path.join(tmp, "snap2.json")
        write_json(snap2_p, changed)
        run_error(OPS + ["submit", root, "b1", plan_p, snap2_p])
        os.unlink(plan_p)  # 删除原输入文件不影响已接纳批次
        os.unlink(snap_p)

        serve = Serve(root)
        serves.append(serve)
        serve.start()

        section("真实 stage 端到端")
        st = wait_state(root, "b1", ["succeeded"])
        if st["result"] != expected:
            fail("wrong result: %s" % st["result"])
        # 结束后异内容仍拒绝
        write_json(plan_p, plan)
        err = run_error(OPS + ["submit", root, "b1", plan_p, snap2_p])
        if "different" not in err["error"]:
            fail("post-completion conflict not rejected properly: %s" % err)
        os.unlink(plan_p)

        section("共享节点跨批复用")
        write_json(plan_p, plan)
        write_json(snap_p, snapshot)
        st = run_json(OPS + ["submit", root, "b2", plan_p, snap_p])
        st = wait_state(root, "b2", ["succeeded"])
        if st["result"] != expected:
            fail("b2 wrong result")
        cells = os.listdir(os.path.join(root, "cells"))
        if len(cells) != 5:
            fail("shared execution duplicated cells: %d" % len(cells))

        section("两批独立分支重叠与峰值配额")
        plan_big, snap_big = big_fixture(40000, tag="p")
        write_json(plan_p, plan_big)
        write_json(snap_p, snap_big)
        expected_big = run_graph(plan_big, snap_big)
        run_json(OPS + ["submit", root, "par1", plan_p, snap_p])
        run_json(OPS + ["submit", root, "par2", plan_p, snap_p])
        t0 = time.time()
        while time.time() - t0 < 30:
            u = usage(root)
            if u.get("slots_used", 0) >= 2:
                break
            time.sleep(0.05)
        s1 = wait_state(root, "par1", ["succeeded"], timeout=180)
        s2 = wait_state(root, "par2", ["succeeded"], timeout=180)
        if s1["result"] != expected_big or s2["result"] != expected_big:
            fail("parallel batches wrong result")
        u = usage(root)
        if u.get("peak_slots", 0) < 2:
            fail("branches did not overlap: peak_slots=%s" % u.get("peak_slots"))
        if u.get("peak_bytes", 0) < 2 * BIG_RESERVE:
            fail("peak bytes not joint-admitted: %s" % u.get("peak_bytes"))
        print("peak_slots=%d peak_bytes=%d" % (u["peak_slots"], u["peak_bytes"]))

        section("输出上限按订阅独立")
        write_json(snap_p, snapshot)  # 回到小夹具快照
        plan_tiny = tiny_reserve_plan(plan, 10)
        plan_big_r = tiny_reserve_plan(plan, 100000)
        write_json(plan_p, plan_tiny)
        run_json(OPS + ["submit", root, "tiny1", plan_p, snap_p])
        st = wait_state(root, "tiny1", ["failed"])
        if "exceeds reserve" not in (st["error"] or "") and \
           "exceeded reserve" not in (st["error"] or ""):
            fail("tiny batch wrong error: %s" % st["error"])
        write_json(plan_p, plan_big_r)
        run_json(OPS + ["submit", root, "big1", plan_p, snap_p])
        st = wait_state(root, "big1", ["succeeded"])
        if st["result"] != expected:
            fail("adequate-reserve batch hurt by tiny subscription")
        write_json(plan_p, plan_tiny)
        run_json(OPS + ["submit", root, "tiny2", plan_p, snap_p])
        st = wait_state(root, "tiny2", ["failed"])
        if "exceeds reserve" not in (st["error"] or ""):
            fail("adoption-time reserve check missing: %s" % st["error"])
        if status(root, "big1")["state"] != "succeeded":
            fail("adequate batch flipped")

        section("业务失败与重试幂等")
        conflict = json.loads(json.dumps(snapshot))
        conflict["east"].append(dict(id="a", rev=2, station="E", t=0, raw=99,
                                     valid=True, deleted=False))
        write_json(snap2_p, conflict)
        write_json(plan_p, plan)
        run_json(OPS + ["submit", root, "biz1", plan_p, snap2_p])
        st = wait_state(root, "biz1", ["failed"])
        if "REV_CONFLICT" not in (st["error"] or ""):
            fail("business error not propagated: %s" % st["error"])
        st = run_json(OPS + ["retry", root, "biz1", "tok-1"])
        if st["state"] != "queued":
            fail("retry did not requeue: %s" % st)
        st = wait_state(root, "biz1", ["failed"])
        st = run_json(OPS + ["retry", root, "biz1", "tok-1"])
        if st["state"] != "failed":
            fail("same token requeued: %s" % st)
        st = run_json(OPS + ["retry", root, "biz1", "tok-2"])
        if st["state"] != "queued":
            fail("fresh token did not requeue: %s" % st)
        st = wait_state(root, "biz1", ["failed"])
        st = run_json(OPS + ["cancel", root, "biz1"])
        if st["state"] != "cancelled":
            fail("cancel failed: %s" % st)
        run_error(OPS + ["retry", root, "biz1", "tok-3"])
        st = run_json(OPS + ["retry", root, "b1", "tok-x"])
        if st["state"] != "succeeded":
            fail("retry on succeeded changed state: %s" % st)

        section("取消不伤及其他订阅")
        plan_c, snap_c = big_fixture(150000, tag="c")
        write_json(plan_p, plan_c)
        write_json(snap_p, snap_c)
        run_json(OPS + ["submit", root, "cx1", plan_p, snap_p])
        run_json(OPS + ["submit", root, "cx2", plan_p, snap_p])
        wait_state(root, "cx1", ["running", "succeeded"])
        st = run_json(OPS + ["cancel", root, "cx1"])
        if st["state"] != "cancelled":
            fail("cancel running failed: %s" % st)
        st = wait_state(root, "cx2", ["succeeded"], timeout=180)
        expected_c = run_graph(plan_c, snap_c)
        if st["result"] != expected_c:
            fail("co-subscribed batch hurt by cancel")
        st = run_json(OPS + ["cancel", root, "cx1"])
        if st["state"] != "cancelled":
            fail("cancel not idempotent")
        st = run_json(OPS + ["cancel", root, "b1"])
        if st["state"] != "succeeded":
            fail("cancel changed succeeded batch")

        section("failed 批次保留引用占额：等待、说明、释放后推进")
        from pipeline import canonical as _canonical, transform as _transform

        def norm_rows(key, n):
            return [dict(id="%s-%d" % (key, i), rev=1, station="E", t=i, raw=i,
                         valid=True, deleted=False) for i in range(n)]
        rows1 = norm_rows("a", 300)
        rows2 = norm_rows("b", 300)
        size_a = len(_canonical(_transform("normalize", [rows1]))) + 1
        res_a = size_a + 5000
        bytes2 = 2 * res_a + 10  # 恰容纳 f1 全部 reserve
        root2 = os.path.join(tmp, "root2")
        run_json(OPS + ["install", root2, pkg_a, "2", str(bytes2)])
        f1_plan = {"nodes": [
            {"id": "a", "op": "normalize", "inputs": [{"source": "s1"}], "reserve": res_a},
            {"id": "b", "op": "normalize", "inputs": [{"source": "s2"}], "reserve": res_a},
            {"id": "c", "op": "normalize", "inputs": [{"node": "a"}], "reserve": 10}],
            "target": "c"}
        f1_snap = {"s1": rows1, "s2": rows2}
        write_json(plan_p, f1_plan)
        write_json(snap_p, f1_snap)
        run_json(OPS + ["submit", root2, "f1", plan_p, snap_p])
        g1_plan = {"nodes": [
            {"id": "solo", "op": "normalize", "inputs": [{"source": "src"}],
             "reserve": bytes2}],
            "target": "solo"}
        g1_snap = {"src": norm_rows("g", 3)}
        gplan_p = os.path.join(tmp, "gplan.json")
        gsnap_p = os.path.join(tmp, "gsnap.json")
        write_json(gplan_p, g1_plan)
        write_json(gsnap_p, g1_snap)
        run_json(OPS + ["submit", root2, "g1", gplan_p, gsnap_p])
        serve2x = Serve(root2)
        serves.append(serve2x)
        serve2x.start()
        st = wait_state(root2, "f1", ["failed"])
        # f1 保留引用占额，g1 须等待而非伪报失败
        time.sleep(3)
        st = status(root2, "g1")
        if st["state"] not in ("queued", "running"):
            fail("g1 falsely failed under capacity pressure: %s" % st)
        run_json(OPS + ["collect", root2])  # 无引用载荷回收，f1 保留引用仍受保护
        time.sleep(1)
        if status(root2, "g1")["state"] not in ("queued", "running"):
            fail("g1 proceeded while failed batch refs held capacity")
        run_json(OPS + ["cancel", root2, "f1"])
        st = wait_state(root2, "g1", ["succeeded"], timeout=90)
        if st["result"] != run_graph(g1_plan, g1_snap):
            fail("g1 wrong result")
        serve2x.stop()
        serves.remove(serve2x)

        section("暂停执行器仍占槽")
        plan_s, snap_s = big_fixture(150000, tag="s")
        write_json(plan_p, plan_s)
        write_json(snap_p, snap_s)
        run_json(OPS + ["submit", root, "pause1", plan_p, snap_p])
        _dir, pid = wait_executor(root)
        os.kill(pid, signal.SIGSTOP)
        time.sleep(1.5)
        u = usage(root)
        held = u.get("slots_used", 0) >= 1
        os.kill(pid, signal.SIGCONT)
        if not held:
            fail("paused executor lost its slot: %s" % u)
        st = wait_state(root, "pause1", ["succeeded"], timeout=180)
        if st["result"] != run_graph(plan_s, snap_s):
            fail("paused batch wrong result")

        section("杀协调器重开归接活执行器")
        plan_k, snap_k = big_fixture(150000, tag="k")
        write_json(plan_p, plan_k)
        write_json(snap_p, snap_k)
        run_json(OPS + ["submit", root, "crash1", plan_p, snap_p])
        _dir, pid = wait_executor(root)
        serve.kill9()
        if not pid_alive(pid):
            fail("executor died with coordinator (should be orphan)")
        serve2 = Serve(root)
        serves.append(serve2)
        serve2.start()
        st = wait_state(root, "crash1", ["succeeded"], timeout=180)
        if st["result"] != run_graph(plan_k, snap_k):
            fail("crash1 wrong result")
        serve_log = open(os.path.join(root, "logs", "serve.log"),
                         encoding="utf-8").read()
        if "adopted live executor" not in serve_log:
            fail("live executor not adopted after coordinator restart")
        cells_crash = os.listdir(os.path.join(root, "cells"))
        if len(cells_crash) != len(set(cells_crash)):
            fail("duplicate cells after crash")

        section("启动登记前后中断")
        serve2.stop()
        serves.remove(serve2)
        run_json(OPS + ["submit", root, "early1", plan_p, snap_p])
        serve3 = Serve(root)
        serves.append(serve3)
        serve3.start()
        serve3.kill9()  # 登记前后即刻杀掉
        serve4 = Serve(root)
        serves.append(serve4)
        serve4.start()
        st = wait_state(root, "early1", ["succeeded"], timeout=180)
        if st["result"] != run_graph(plan_k, snap_k):
            fail("early1 wrong result")

        section("重复 serve 安全")
        rc, out, _err = run(OPS + ["serve", root])
        if rc != 0 or "already_serving" not in out:
            fail("duplicate serve unsafe: rc=%s out=%s" % (rc, out))

        section("并发调用原子历史")
        import threading
        outcomes = []

        def concurrent_submit():
            outcomes.append(run(OPS + ["submit", root, "conc1", plan_p, snap_p],
                                ok=True)[1])
        threads = [threading.Thread(target=concurrent_submit) for _ in range(6)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        releases = {json.loads(o)["release"] for o in outcomes}
        if len(releases) != 1:
            fail("concurrent submits diverged: %s" % releases)
        st = wait_state(root, "conc1", ["succeeded"], timeout=240)
        if st["result"] != run_graph(plan_k, snap_k):
            fail("conc1 wrong result")
        cancels = []

        def concurrent_cancel():
            cancels.append(run(OPS + ["cancel", root, "biz1"], ok=True)[1])
        threads = [threading.Thread(target=concurrent_cancel) for _ in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        if {json.loads(o)["state"] for o in cancels} != {"cancelled"}:
            fail("concurrent cancels diverged: %s" % cancels)

        section("更新后旧批继续用原包")
        src_b = os.path.join(tmp, "srcB")
        os.makedirs(src_b)
        shutil.copyfile(os.path.join(HERE, "ops.py"), os.path.join(src_b, "ops.py"))
        with open(os.path.join(HERE, "pipeline.py"), "r", encoding="utf-8") as fh:
            pipe_src = fh.read()
        with open(os.path.join(src_b, "pipeline.py"), "w", encoding="utf-8") as fh:
            fh.write(pipe_src + "\n# release B comment\n")
        pkg_b = os.path.join(tmp, "pkgB.zip")
        rel_b = run_json([sys.executable, os.path.join(src_b, "ops.py"),
                          "build", pkg_b])["release"]
        if rel_b == rel_a:
            fail("package B not distinct")
        plan_o, snap_o = big_fixture(200000, tag="o")
        write_json(plan_p, plan_o)
        write_json(snap_p, snap_o)
        st = run_json(OPS + ["submit", root, "old1", plan_p, snap_p])
        if st["release"] != rel_a:
            fail("old batch not bound to A")
        wait_state(root, "old1", ["running"])
        rel_inst = run_json(OPS + ["install", root, pkg_b, "2", str(INSTALL_BYTES)])["release"]
        if rel_inst != rel_b:
            fail("update failed")
        st = run_json(OPS + ["submit", root, "new1", plan_p, snap_p])
        if st["release"] != rel_b:
            fail("new batch not bound to B")
        write_json(plan_p, plan)      # b1 的原始冻结输入
        write_json(snap_p, snapshot)
        st = run_json(OPS + ["submit", root, "b1", plan_p, snap_p])
        if st["release"] != rel_a:
            fail("resubmitted b1 rebound to B")
        write_json(plan_p, plan_o)
        write_json(snap_p, snap_o)
        removed = run_json(OPS + ["collect", root])["removed_bytes"]
        if not os.path.isfile(os.path.join(root, "packages", rel_a + ".pkg")):
            fail("referenced old package collected while old batch active")
        st = wait_state(root, "old1", ["succeeded"], timeout=240)
        if st["release"] != rel_a or st["result"] != run_graph(plan_o, snap_o):
            fail("old batch did not finish on original package")
        st = wait_state(root, "new1", ["succeeded"], timeout=240)
        if st["release"] != rel_b:
            fail("new batch wrong release")
        # failed 批次仍持有原包引用（重试需沿原包）；取消后引用解除
        for bid in ("tiny1", "tiny2"):
            st = run_json(OPS + ["cancel", root, bid])
            if st["state"] != "cancelled":
                fail("cancel failed batch failed: %s" % st)
        removed = run_json(OPS + ["collect", root])["removed_bytes"]
        if os.path.isfile(os.path.join(root, "packages", rel_a + ".pkg")):
            fail("unreferenced old package not collected")
        if not os.path.isfile(os.path.join(root, "packages", rel_b + ".pkg")):
            fail("current package collected")
        print("collect removed_bytes=%d" % removed)

        section("移走原源码后安装版自足")
        for name in ("pipeline.py", "manual.py", "checks.py", "samples.py"):
            src = os.path.join(HERE, name)
            dst = src + ".hidden"
            os.rename(src, dst)
            renamed.append((dst, src))
        try:
            empty = os.path.join(tmp, "empty")
            os.makedirs(empty)
            installed_ops = [sys.executable, os.path.join(root, "ops.py")]
            st = run_json(installed_ops + ["submit", root, "late1", plan_p, snap_p],
                          cwd=empty)
            if st["release"] != rel_b:
                fail("installed submit bound wrong release")
            st = wait_state(root, "late1", ["succeeded"], ops=installed_ops)
            if st["result"] != run_graph(plan_o, snap_o):
                fail("installed run wrong result")
            run_json(installed_ops + ["collect", root], cwd=empty)
            run_json(installed_ops + ["status", root, "b1"], cwd=empty)
        finally:
            for dst, src in renamed:
                os.rename(dst, src)
            renamed.clear()

        section("回收口径")
        out = run_json(OPS + ["collect", root])
        if not isinstance(out.get("removed_bytes"), int) or out["removed_bytes"] < 0:
            fail("collect output malformed: %s" % out)

        print("OPS_VERIFY_OK")
        succeeded = True
    finally:
        for dst, src in renamed:
            if os.path.exists(dst):
                os.rename(dst, src)
        for serve in serves:
            serve.stop()
        if succeeded:
            shutil.rmtree(tmp, ignore_errors=True)
        else:
            print("VERIFY FAILED; scene preserved at %s" % tmp, file=sys.stderr)


# ---------------------------------------------------------------------------
# demo
# ---------------------------------------------------------------------------

def demo():
    tmp = tempfile.mkdtemp(prefix="ops-demo-")
    serves = []
    try:
        section("构建与安装")
        pkg = os.path.join(tmp, "hourly.pkg")
        rel = run_json(OPS + ["build", pkg])["release"]
        root = os.path.join(tmp, "demo-root")
        run_json(OPS + ["install", root, pkg, "2", str(INSTALL_BYTES)])
        print("release=%s" % rel)

        section("安装版真实小时结果")
        plan, snapshot, expected = fixture()
        plan_p = os.path.join(tmp, "plan.json")
        snap_p = os.path.join(tmp, "snap.json")
        write_json(plan_p, plan)
        write_json(snap_p, snapshot)
        installed_ops = [sys.executable, os.path.join(root, "ops.py")]
        st = run_json(installed_ops + ["submit", root, "hourly-demo", plan_p, snap_p])
        print("submit: %s" % json.dumps(st, ensure_ascii=False))
        serve = Serve(root, ops=installed_ops)
        serves.append(serve)
        serve.start()
        st = wait_state(root, "hourly-demo", ["succeeded"], ops=installed_ops)
        print("hourly result: %s" % json.dumps(st["result"], ensure_ascii=False))
        if st["result"] != expected:
            fail("demo result mismatch")

        section("共享执行与协调器崩溃恢复")
        plan_big, snap_big = big_fixture(150000, tag="d")
        write_json(plan_p, plan_big)
        write_json(snap_p, snap_big)
        run_json(installed_ops + ["submit", root, "shared-1", plan_p, snap_p])
        run_json(installed_ops + ["submit", root, "shared-2", plan_p, snap_p])
        _dir, pid = wait_executor(root)
        print("shared executor pid=%d running; killing coordinator..." % pid)
        serve.kill9()
        print("coordinator killed; orphan executor alive=%s" % pid_alive(pid))
        serve2 = Serve(root, ops=installed_ops)
        serves.append(serve2)
        serve2.start()
        print("coordinator restarted; adopted executors, waiting batches...")
        s1 = wait_state(root, "shared-1", ["succeeded"], timeout=240, ops=installed_ops)
        s2 = wait_state(root, "shared-2", ["succeeded"], timeout=240, ops=installed_ops)
        cells = os.listdir(os.path.join(root, "cells"))
        print("shared-1: %s" % json.dumps(s1, ensure_ascii=False))
        print("shared-2: %s" % json.dumps(s2, ensure_ascii=False))
        print("cells held after recovery: %d（两批同图同输入，共享同一份计算载荷）" % len(cells))
        if s1["result"] != s2["result"]:
            fail("shared batches diverged")
        print("OPS_DEMO_OK")
    finally:
        for serve in serves:
            serve.stop()
        shutil.rmtree(tmp, ignore_errors=True)


if __name__ == "__main__":
    if sys.argv[1:] == ["verify"]:
        verify()
    elif sys.argv[1:] == ["demo"]:
        demo()
    else:
        raise SystemExit("usage: ops_checks.py verify|demo")
