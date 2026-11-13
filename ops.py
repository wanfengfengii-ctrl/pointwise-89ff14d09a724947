"""
ops.py — 小时加工链常驻作业服务运行层（仅标准库，Python 3.9+）。

公开入口（均为直接 argv，不借助 shell）：
  python3 ops.py build OUT
  python3 ops.py install ROOT PACKAGE SLOTS BYTES
  python3 ops.py submit ROOT ID PLAN SNAPSHOT
  python3 ops.py serve ROOT
  python3 ops.py status ROOT ID
  python3 ops.py cancel ROOT ID
  python3 ops.py retry ROOT ID TOKEN
  python3 ops.py collect ROOT

命令错误退出 2 并输出 {"error": "可读说明"}。
业务核 pipeline.py 不作修改；批次绑定接纳时的完整安装包。
"""

import fcntl
import hashlib
import importlib.util
import io
import json
import os
import re
import shutil
import signal
import subprocess
import sys
import time
import traceback
import uuid
import zipfile

FORMAT = 1
IDENTITY_TAG = "pointwise-identity"
LAUNCHD_LABEL_PREFIX = "local.pointwise.ops"
PASS_INTERVAL = 0.2
GC_INTERVAL = 15.0
LOST_ATTEMPT_LIMIT = 3

# ---------------------------------------------------------------------------
# 执行器源码（随发布包分发；旧批只用绑定包，因此执行器也打进包内）
# ---------------------------------------------------------------------------
EXECUTOR_SOURCE = r'''
import hashlib
import json
import os
import shutil
import subprocess
import sys


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False,
                      allow_nan=False).encode("utf-8")


def fsync_dir(path):
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def reject_constant(name):
    raise ValueError("NON_FINITE:" + name)


def write_exit_record(execdir, record):
    tmp = os.path.join(execdir, "exit-record.json.tmp")
    with open(tmp, "wb") as fh:
        fh.write(canonical(record))
        fh.flush()
        os.fsync(fh.fileno())
    os.rename(tmp, os.path.join(execdir, "exit-record.json"))
    fsync_dir(execdir)


def feed_stdin(proc, inputs):
    # 单元格载荷为 canonical + 换行，stdin 组装时去掉该换行；源值文件无换行。
    out = proc.stdin
    out.write(b'{"inputs":[')
    first = True
    for spec in inputs:
        if not first:
            out.write(b",")
        first = False
        with open(spec["path"], "rb") as fh:
            data = fh.read()
        if spec.get("kind") == "cell":
            if not data.endswith(b"\n"):
                raise ValueError("CELL_PAYLOAD_SHAPE")
            data = data[:-1]
        out.write(data)
    out.write(b"]}")
    out.close()


def run(execdir, req):
    pkgdir = os.path.dirname(os.path.abspath(__file__))
    stage = os.path.join(pkgdir, "pipeline.py")
    reserve = int(req["reserve"])
    outpath = os.path.join(execdir, "out.tmp")
    errpath = os.path.join(execdir, "stderr.log")
    with open(os.path.join(execdir, "pid.json"), "wb") as fh:
        fh.write(canonical({"pid": os.getpid(), "ppid": os.getppid()}))
    errfh = open(errpath, "wb")
    try:
        proc = subprocess.Popen(
            [sys.executable, stage, "stage", req["op"]],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=errfh,
            cwd=execdir, pass_fds=(int(req["lock_fd"]),))
    finally:
        errfh.close()
    try:
        feed_stdin(proc, req["inputs"])
    except (BrokenPipeError, ConnectionResetError, OSError):
        pass
    total = 0
    exceeded = False
    with open(outpath, "wb") as out:
        while True:
            chunk = proc.stdout.read(65536)
            if not chunk:
                break
            total += len(chunk)
            if total > reserve:
                allowed = len(chunk) - (total - reserve)
                if allowed > 0:
                    out.write(chunk[:allowed])
                exceeded = True
                proc.kill()
                break
            out.write(chunk)
        out.flush()
        os.fsync(out.fileno())
    proc.wait()
    if exceeded:
        return {"outcome": "failed", "error_kind": "reserve",
                "error": "stage output exceeded reserve of %d bytes" % reserve}
    if proc.returncode != 0:
        tail = b""
        try:
            size = os.path.getsize(errpath)
            with open(errpath, "rb") as fh:
                fh.seek(max(0, size - 400))
                tail = fh.read()
        except OSError:
            pass
        return {"outcome": "failed", "error_kind": "business",
                "error": "stage %s exited %d: %s" % (
                    req["op"], proc.returncode,
                    tail.decode("utf-8", "replace").strip()[-300:])}
    with open(outpath, "rb") as fh:
        data = fh.read()
    if not data.endswith(b"\n"):
        return {"outcome": "failed", "error_kind": "bad_output",
                "error": "stage output missing trailing newline"}
    body = data[:-1]
    try:
        value = json.loads(body.decode("utf-8"), parse_constant=reject_constant)
    except Exception:
        return {"outcome": "failed", "error_kind": "bad_output",
                "error": "stage output is not valid JSON"}
    if canonical(value) != body:
        return {"outcome": "failed", "error_kind": "bad_output",
                "error": "stage output is not canonical JSON"}
    cells_dir = req["cells_dir"]
    identity = req["identity"]
    tmpcell = os.path.join(cells_dir, ".tmp-" + os.path.basename(execdir))
    if os.path.isdir(tmpcell):
        shutil.rmtree(tmpcell)
    os.mkdir(tmpcell)
    os.rename(outpath, os.path.join(tmpcell, "payload.json"))
    meta = {"format": 1, "identity": identity, "release": req["release"], "op": req["op"],
            "size": len(data), "payload_sha256": hashlib.sha256(data).hexdigest(),
            "value_sha256": hashlib.sha256(body).hexdigest()}
    with open(os.path.join(tmpcell, "meta.json"), "wb") as fh:
        fh.write(canonical(meta))
        fh.flush()
        os.fsync(fh.fileno())
    fsync_dir(tmpcell)
    final = os.path.join(cells_dir, identity)
    try:
        os.rename(tmpcell, final)
    except OSError:
        if not os.path.isdir(final):
            raise
        # 同身份载荷已被发布（崩溃竞态）：采用既有载荷，丢弃本份。
        shutil.rmtree(tmpcell)
    fsync_dir(cells_dir)
    return {"outcome": "published", "identity": identity, "size": len(data),
            "value_sha256": meta["value_sha256"]}


def main():
    execdir = sys.argv[1]
    with open(os.path.join(execdir, "request.json"), "r", encoding="utf-8") as fh:
        req = json.load(fh)
    try:
        record = run(execdir, req)
    except Exception as exc:  # 执行器自身故障：登记为 lost，由协调器决定重试
        record = {"outcome": "failed", "error_kind": "internal",
                  "error": "executor internal: %r" % (exc,)}
    write_exit_record(execdir, record)


if __name__ == "__main__":
    main()
'''

# ---------------------------------------------------------------------------
# 基础工具
# ---------------------------------------------------------------------------


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False,
                      allow_nan=False).encode("utf-8")


def sha256_bytes(data):
    return hashlib.sha256(data).hexdigest()


def die(message):
    sys.stdout.write(canonical({"error": str(message)}).decode("utf-8") + "\n")
    raise SystemExit(2)


def print_json(obj):
    sys.stdout.write(canonical(obj).decode("utf-8") + "\n")


def reject_constant(name):
    raise ValueError("NON_FINITE:" + name)


def no_duplicate_keys(pairs):
    obj = {}
    for key, value in pairs:
        if key in obj:
            raise ValueError("DUPLICATE_KEY:" + key)
        obj[key] = value
    return obj


def load_json_text(text, what):
    try:
        return json.loads(text, object_pairs_hook=no_duplicate_keys,
                          parse_constant=reject_constant)
    except ValueError as exc:
        die("%s is not valid strict JSON: %s" % (what, exc))


def read_json_file(path):
    try:
        with open(path, "r", encoding="utf-8") as fh:
            return json.load(fh)
    except (OSError, ValueError):
        return None


def fsync_dir(path):
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def write_json_atomic(path, obj):
    data = canonical(obj)
    tmp = "%s.tmp-%s" % (path, uuid.uuid4().hex)
    with open(tmp, "wb") as fh:
        fh.write(data)
        fh.flush()
        os.fsync(fh.fileno())
    os.rename(tmp, path)
    fsync_dir(os.path.dirname(path))


def dir_size(path):
    total = 0
    for base, _dirs, files in os.walk(path):
        for name in files:
            try:
                total += os.path.getsize(os.path.join(base, name))
            except OSError:
                pass
    return total


def rmtree_safe(path):
    try:
        if os.path.isdir(path) and not os.path.islink(path):
            shutil.rmtree(path)
        else:
            os.unlink(path)
        return True
    except OSError:
        return False


def flock_held(path):
    """锁被其他进程持有则 True；获得即释放。flock 随进程死亡释放，与 PID 复用无关。"""
    try:
        fd = os.open(path, os.O_RDWR | os.O_CREAT)
    except OSError:
        return False
    try:
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            return True
        fcntl.flock(fd, fcntl.LOCK_UN)
        return False
    finally:
        os.close(fd)


class FileLock(object):
    """阻塞型 flock，用于注册表串行化与 GC 互斥。"""

    def __init__(self, path, blocking=True):
        self.path = path
        self.blocking = blocking
        self.fd = None

    def acquire(self):
        self.fd = os.open(self.path, os.O_RDWR | os.O_CREAT)
        flags = fcntl.LOCK_EX if self.blocking else fcntl.LOCK_EX | fcntl.LOCK_NB
        try:
            fcntl.flock(self.fd, flags)
        except OSError:
            os.close(self.fd)
            self.fd = None
            if self.blocking:
                raise
            return False
        return True

    def release(self):
        if self.fd is not None:
            os.close(self.fd)
            self.fd = None

    def __enter__(self):
        self.acquire()
        return self

    def __exit__(self, *exc):
        self.release()
        return False


# ---------------------------------------------------------------------------
# 路径布局
# ---------------------------------------------------------------------------

def canon_root(path):
    return os.path.realpath(os.path.abspath(path))


def p_install(root):
    return os.path.join(root, "install.json")


def p_packages(root):
    return os.path.join(root, "packages")


def p_pkgdir(root, release):
    return os.path.join(root, "pkg", release)


def p_registry(root):
    return os.path.join(root, "registry")


def p_batches(root):
    return os.path.join(p_registry(root), "batches")


def p_srcdir(root, idhash):
    return os.path.join(p_registry(root), "src", idhash)


def p_cells(root):
    return os.path.join(root, "cells")


def p_exec(root):
    return os.path.join(root, "exec")


def p_trash(root):
    return os.path.join(root, "trash")


def p_logs(root):
    return os.path.join(root, "logs")


def ensure_layout(root):
    for path in (root, p_packages(root), os.path.join(root, "pkg"), p_registry(root),
                 p_batches(root), os.path.join(p_registry(root), "src"), p_cells(root),
                 p_exec(root), p_trash(root), p_logs(root),
                 os.path.join(root, "launchd")):
        os.makedirs(path, exist_ok=True)


def registry_lock(root):
    return FileLock(os.path.join(p_registry(root), "lock"))


def gc_lock_path(root):
    return os.path.join(root, "gc.lock")


def read_install(root):
    data = read_json_file(p_install(root))
    if not isinstance(data, dict) or data.get("format") != FORMAT:
        return None
    return data


def require_install(root):
    install = read_install(root)
    if install is None:
        die("run root is not installed: %s" % root)
    return install


# ---------------------------------------------------------------------------
# 批次记录
# ---------------------------------------------------------------------------

def idhash_of(bid):
    return sha256_bytes(bid.encode("utf-8"))


def batch_path(root, idhash):
    return os.path.join(p_batches(root), idhash + ".json")


def load_batch(root, idhash):
    data = read_json_file(batch_path(root, idhash))
    if isinstance(data, dict) and data.get("format") == FORMAT:
        return data
    return None


def save_batch(root, rec):
    write_json_atomic(batch_path(root, idhash_of(rec["id"])), rec)


def list_batches(root):
    out = []
    bdir = p_batches(root)
    try:
        names = os.listdir(bdir)
    except OSError:
        return out
    for name in names:
        if not name.endswith(".json"):
            continue
        data = read_json_file(os.path.join(bdir, name))
        if isinstance(data, dict) and data.get("format") == FORMAT:
            out.append(data)
    return out


def next_seq(root):
    """调用方须已持有注册表锁。"""
    path = os.path.join(p_registry(root), "seq")
    try:
        with open(path, "r", encoding="utf-8") as fh:
            seq = int(fh.read().strip() or "0")
    except (OSError, ValueError):
        seq = 0
    seq += 1
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as fh:
        fh.write(str(seq))
        fh.flush()
        os.fsync(fh.fileno())
    os.rename(tmp, path)
    return seq


def status_of(rec):
    return {"id": rec["id"], "release": rec["release"], "state": rec["state"],
            "result": rec.get("result"), "error": rec.get("error")}


def mutate_batch(root, idhash, fn):
    """在注册表锁内重读记录、应用 fn(rec)->bool(是否写回)，保证实时先后的原子历史。"""
    with registry_lock(root):
        rec = load_batch(root, idhash)
        if rec is None:
            return None
        if fn(rec):
            save_batch(root, rec)
        return rec


# ---------------------------------------------------------------------------
# 计算身份与单元格
# ---------------------------------------------------------------------------

def node_map(rec):
    return {node["id"]: node for node in rec["plan"]["nodes"]}


def compute_identity(rec, nid):
    """身份=(包摘要, op, 有序输入规范值摘要)；输入未全部解析时返回 None。"""
    node = node_map(rec)[nid]
    digests = []
    for ref in node["inputs"]:
        if "source" in ref:
            digest = rec["source_digests"].get(ref["source"])
            if digest is None:
                return None
            digests.append(digest)
        else:
            up = rec["nodes"][ref["node"]]
            if up["state"] != "completed":
                return None
            digests.append(up["value_sha256"])
    return sha256_bytes(canonical([IDENTITY_TAG, FORMAT, rec["release"], node["op"], digests]))


def cell_dir(root, identity):
    return os.path.join(p_cells(root), identity)


def read_cell_meta(root, identity):
    meta = read_json_file(os.path.join(cell_dir(root, identity), "meta.json"))
    if isinstance(meta, dict) and meta.get("format") == FORMAT \
            and meta.get("identity") == identity:
        return meta
    return None


def downstream_map(rec):
    children = {nid: [] for nid in rec["nodes"]}
    for node in rec["plan"]["nodes"]:
        for ref in node["inputs"]:
            if "node" in ref:
                children[ref["node"]].append(node["id"])
    return children


def needed_identities_for_failed(rec):
    """failed 批次续跑仍需：已完成 target + 有直接下游未完成的已完成节点。"""
    nodes = rec["nodes"]
    children = downstream_map(rec)
    incomplete = {nid for nid, st in nodes.items() if st["state"] != "completed"}
    needed = set()
    target = rec["plan"]["target"]
    if nodes[target]["state"] == "completed":
        needed.add(target)
    for nid, st in nodes.items():
        if st["state"] != "completed":
            continue
        if any(child in incomplete for child in children[nid]):
            needed.add(nid)
    return {nodes[nid]["identity"] for nid in needed if nodes[nid].get("identity")}


def compute_refs(root):
    """受保护集合：活动批次的完成载荷与待运行订阅、failed 批次续跑所需、
    当前安装包及活动/failed 批次绑定包。"""
    cells = set()
    pkgs = set()
    install = read_install(root)
    if install:
        pkgs.add(install["release"])
    for rec in list_batches(root):
        state = rec["state"]
        if state in ("queued", "running"):
            pkgs.add(rec["release"])
            for st in rec["nodes"].values():
                if st["state"] in ("completed", "waiting") and st.get("identity"):
                    cells.add(st["identity"])
        elif state == "failed":
            pkgs.add(rec["release"])
            cells |= needed_identities_for_failed(rec)
    return cells, pkgs


# ---------------------------------------------------------------------------
# 包构建与校验
# ---------------------------------------------------------------------------

def _zip_info(name):
    info = zipfile.ZipInfo(name, date_time=(1980, 1, 1, 0, 0, 0))
    info.compress_type = zipfile.ZIP_DEFLATED
    info.external_attr = 0o100644 << 16
    info.create_system = 0
    return info


def cmd_build(out_path):
    src_dir = os.path.dirname(os.path.abspath(__file__))
    pipeline_path = os.path.join(src_dir, "pipeline.py")
    if not os.path.isfile(pipeline_path):
        die("pipeline.py not found next to ops.py: %s" % pipeline_path)
    with open(pipeline_path, "rb") as fh:
        pipeline_bytes = fh.read()
    members = {"pipeline.py": pipeline_bytes,
               "executor.py": EXECUTOR_SOURCE.encode("utf-8")}
    manifest = {"format": FORMAT,
                "members": {name: {"size": len(data), "sha256": sha256_bytes(data)}
                            for name, data in sorted(members.items())}}
    out_path = os.path.abspath(out_path)
    tmp = "%s.tmp-%s" % (out_path, uuid.uuid4().hex)
    try:
        with zipfile.ZipFile(tmp, "w") as zf:
            for name in sorted(members):
                zf.writestr(_zip_info(name), members[name])
            zf.writestr(_zip_info("manifest.json"), canonical(manifest))
        os.rename(tmp, out_path)
    except OSError as exc:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        die("cannot write package: %s" % exc)
    with open(out_path, "rb") as fh:
        release = sha256_bytes(fh.read())
    print_json({"release": release})


MEMBER_NAME_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]*$")


def verify_package(path):
    """截断、篡改及非法成员在此拒绝；返回 (release, members)。"""
    try:
        with open(path, "rb") as fh:
            blob = fh.read()
    except OSError as exc:
        die("cannot read package: %s" % exc)
    release = sha256_bytes(blob)
    try:
        zf = zipfile.ZipFile(io.BytesIO(blob))
        names = zf.namelist()
    except zipfile.BadZipFile as exc:
        die("package is not a valid archive (truncated or corrupt): %s" % exc)
    if len(set(names)) != len(names):
        die("package has duplicate members")
    for name in names:
        if not MEMBER_NAME_RE.match(name):
            die("package has illegal member name: %r" % name)
        mode = (zf.getinfo(name).external_attr >> 16) & 0o170000
        if mode == 0o120000:
            die("package member must not be a symlink: %s" % name)
    if "manifest.json" not in names:
        die("package manifest missing")
    try:
        manifest = json.loads(zf.read("manifest.json").decode("utf-8"))
    except (ValueError, KeyError, zipfile.BadZipFile) as exc:
        die("package manifest corrupt: %s" % exc)
    if not isinstance(manifest, dict) or manifest.get("format") != FORMAT \
            or not isinstance(manifest.get("members"), dict):
        die("package manifest format unsupported")
    listed = manifest["members"]
    if set(listed) != set(names) - {"manifest.json"}:
        die("package manifest members mismatch")
    for name in sorted(listed):
        try:
            data = zf.read(name)
        except zipfile.BadZipFile as exc:
            die("package member corrupt: %s (%s)" % (name, exc))
        ent = listed[name]
        if len(data) != ent.get("size") or sha256_bytes(data) != ent.get("sha256"):
            die("package member tampered: %s" % name)
    for required in ("pipeline.py", "executor.py"):
        if required not in listed:
            die("package required member missing: %s" % required)
    try:
        compile(zf.read("pipeline.py"), "pipeline.py", "exec")
        compile(zf.read("executor.py"), "executor.py", "exec")
    except SyntaxError as exc:
        die("package member does not compile: %s" % exc)
    return release, listed


def stage_package(root, package, release):
    """把已校验包落位：packages/<release>.pkg 与 pkg/<release>/。幂等。"""
    pkg_file = os.path.join(p_packages(root), release + ".pkg")
    if not os.path.isfile(pkg_file):
        tmp = "%s.tmp-%s" % (pkg_file, uuid.uuid4().hex)
        shutil.copyfile(package, tmp)
        with open(tmp, "rb") as fh:
            if sha256_bytes(fh.read()) != release:
                os.unlink(tmp)
                die("package changed while installing")
        os.rename(tmp, pkg_file)
        fsync_dir(p_packages(root))
    pkgdir = p_pkgdir(root, release)
    if not os.path.isdir(pkgdir):
        tmpdir = os.path.join(os.path.dirname(pkgdir), ".tmp-" + uuid.uuid4().hex)
        os.makedirs(tmpdir)
        with zipfile.ZipFile(pkg_file) as zf:
            for name in zf.namelist():
                if name == "manifest.json":
                    continue
                data = zf.read(name)
                dst = os.path.join(tmpdir, name)
                with open(dst, "wb") as fh:
                    fh.write(data)
                    fh.flush()
                    os.fsync(fh.fileno())
        fsync_dir(tmpdir)
        os.rename(tmpdir, pkgdir)
        fsync_dir(os.path.dirname(pkgdir))


_pipeline_cache = {}


def load_bound_pipeline(root, release):
    """从运行根内绑定包加载业务核；脱离源码树。"""
    if release in _pipeline_cache:
        return _pipeline_cache[release]
    path = os.path.join(p_pkgdir(root, release), "pipeline.py")
    if not os.path.isfile(path):
        die("bound package is not unpacked: %s" % release)
    spec = importlib.util.spec_from_file_location("ops_bound_" + release[:16], path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    _pipeline_cache[release] = module
    return module


# ---------------------------------------------------------------------------
# install
# ---------------------------------------------------------------------------

def probe_interpreter(interp):
    if not os.path.isabs(interp) or not os.path.isfile(interp):
        die("interpreter is not an absolute existing path: %s" % interp)
    try:
        proc = subprocess.run(
            [interp, "-I", "-c",
             "import sys,json,fcntl,zipfile,hashlib,subprocess,importlib.util;"
             "sys.exit(0 if sys.version_info>=(3,9) else 1)"],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=30)
    except (OSError, subprocess.TimeoutExpired) as exc:
        die("interpreter probe failed for %s: %s" % (interp, exc))
    if proc.returncode != 0:
        die("interpreter %s lacks required capabilities (need Python 3.9+ stdlib): %s"
            % (interp, proc.stderr.decode("utf-8", "replace").strip()[:200]))


def launchd_label(root):
    return "%s.%s" % (LAUNCHD_LABEL_PREFIX, sha256_bytes(root.encode("utf-8"))[:12])


def plist_xml(label, program_args, root):
    def esc(text):
        return (text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;"))
    args = "\n".join("    <string>%s</string>" % esc(a) for a in program_args)
    return """<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key>
  <string>%s</string>
  <key>ProgramArguments</key>
  <array>
%s
  </array>
  <key>RunAtLoad</key>
  <true/>
  <key>KeepAlive</key>
  <true/>
  <key>ThrottleInterval</key>
  <integer>5</integer>
  <key>WorkingDirectory</key>
  <string>%s</string>
  <key>StandardOutPath</key>
  <string>%s</string>
  <key>StandardErrorPath</key>
  <string>%s</string>
</dict>
</plist>
""" % (esc(label), args, esc(root),
       esc(os.path.join(p_logs(root), "launchd.out.log")),
       esc(os.path.join(p_logs(root), "launchd.err.log")))


def note(message):
    sys.stderr.write("install: %s\n" % message)


def setup_launchd(root, install):
    """交付用户级 LaunchAgent：始终生成 plist；有登录会话则实际装载，
    无会话则直接执行其 ProgramArguments 验证（区分配置验证与实际装载）。"""
    label = launchd_label(root)
    program = [install["interpreter"], os.path.join(root, "ops.py"), "serve", root]
    content = plist_xml(label, program, root)
    ldir = os.path.join(root, "launchd")
    os.makedirs(ldir, exist_ok=True)
    plist_path = os.path.join(ldir, label + ".plist")
    with open(plist_path, "w", encoding="utf-8") as fh:
        fh.write(content)
    loaded = False
    if sys.platform == "darwin":
        launchctl = shutil.which("launchctl")
        uid = os.getuid()
        domain = "gui/%d" % uid
        if launchctl and subprocess.run([launchctl, "print", domain],
                                        stdout=subprocess.DEVNULL,
                                        stderr=subprocess.DEVNULL).returncode == 0:
            agents = os.path.expanduser("~/Library/LaunchAgents")
            os.makedirs(agents, exist_ok=True)
            target = os.path.join(agents, label + ".plist")
            shutil.copyfile(plist_path, target)
            subprocess.run([launchctl, "bootout", "%s/%s" % (domain, label)],
                           stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            if subprocess.run([launchctl, "bootstrap", domain, target],
                              stdout=subprocess.DEVNULL,
                              stderr=subprocess.DEVNULL).returncode == 0:
                loaded = True
                note("LaunchAgent loaded: %s (unload: launchctl bootout %s/%s)"
                     % (target, domain, label))
    if not loaded:
        # 配置验证：直接执行 ProgramArguments。已有协调器在跑则视为通过。
        if flock_held(os.path.join(root, "serve.lock")):
            note("LaunchAgent configured at %s (coordinator already running; "
                 "config not re-executed)" % plist_path)
            return
        log_path = os.path.join(p_logs(root), "verify-serve.log")
        with open(log_path, "ab") as logfh:
            proc = subprocess.Popen(program, stdout=logfh, stderr=logfh,
                                    stdin=subprocess.DEVNULL)
        time.sleep(1.5)
        rc = proc.poll()
        if rc is None:
            proc.terminate()
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait()
            note("LaunchAgent configured at %s (no user session; ProgramArguments "
                 "executed and verified, not loaded)" % plist_path)
        elif rc == 0:
            note("LaunchAgent configured at %s (no user session; ProgramArguments "
                 "verified, not loaded)" % plist_path)
        else:
            die("LaunchAgent ProgramArguments verification failed, see %s" % log_path)


def copy_ops_entry(root):
    src = os.path.abspath(__file__)
    dst = os.path.join(root, "ops.py")
    try:
        if os.path.samefile(src, dst):
            return
    except OSError:
        pass
    shutil.copyfile(src, dst)


def cmd_install(root, package, slots_text, bytes_text):
    root = canon_root(root)
    try:
        slots = int(slots_text)
        nbytes = int(bytes_text)
    except ValueError:
        die("SLOTS and BYTES must be positive integers")
    if slots < 1 or nbytes < 1:
        die("SLOTS and BYTES must be positive integers")
    package = os.path.abspath(package)
    if not os.path.isfile(package):
        die("package not found: %s" % package)
    release, _members = verify_package(package)  # 改变运行根前拒绝坏包
    interp = os.path.realpath(sys.executable)
    probe_interpreter(interp)
    ensure_layout(root)
    stage_package(root, package, release)
    copy_ops_entry(root)
    with registry_lock(root):
        install = read_install(root)
        if install is not None:
            if install["slots"] != slots or install["bytes"] != nbytes:
                die("quotas must match first install: SLOTS=%d BYTES=%d"
                    % (install["slots"], install["bytes"]))
            install = dict(install)
            install["release"] = release
            install["interpreter"] = interp
        else:
            install = {"format": FORMAT, "release": release, "slots": slots,
                       "bytes": nbytes, "interpreter": interp}
        write_json_atomic(p_install(root), install)
    setup_launchd(root, install)
    print_json({"release": release})


# ---------------------------------------------------------------------------
# submit / status / cancel / retry
# ---------------------------------------------------------------------------

def read_json_arg(path, what):
    try:
        with open(path, "r", encoding="utf-8") as fh:
            text = fh.read()
    except OSError as exc:
        die("cannot read %s file %s: %s" % (what, path, exc))
    return load_json_text(text, what)


def source_file(root, idhash, source):
    return os.path.join(p_srcdir(root, idhash), sha256_bytes(source.encode("utf-8")) + ".json")


def freeze_sources(root, idhash, plan, snapshot, canon):
    """把引用到的源值以规范字节冻结到临时目录；调用方在锁内 rename 归位。"""
    wanted = []
    for node in plan["nodes"]:
        for ref in node["inputs"]:
            if "source" in ref and ref["source"] not in wanted:
                wanted.append(ref["source"])
    tmpdir = os.path.join(p_registry(root), "src", ".tmp-" + uuid.uuid4().hex)
    os.makedirs(tmpdir)
    digests = {}
    for name in wanted:
        data = canon(snapshot[name])
        digests[name] = sha256_bytes(data)
        dst = os.path.join(tmpdir, sha256_bytes(name.encode("utf-8")) + ".json")
        with open(dst, "wb") as fh:
            fh.write(data)
            fh.flush()
            os.fsync(fh.fileno())
    fsync_dir(tmpdir)
    return tmpdir, digests


def cmd_submit(root, bid, plan_path, snapshot_path):
    root = canon_root(root)
    if not isinstance(bid, str) or not bid:
        die("batch ID must be a non-empty string")
    install = require_install(root)
    plan = read_json_arg(plan_path, "plan")
    snapshot = read_json_arg(snapshot_path, "snapshot")
    if not isinstance(snapshot, dict):
        die("snapshot must be a JSON object")
    pipe = load_bound_pipeline(root, install["release"])
    try:
        _by_id, order = pipe.validate_plan(plan, snapshot)
    except ValueError as exc:
        die("invalid plan: %s" % exc)
    total_reserve = sum(node["reserve"] for node in plan["nodes"])
    if total_reserve > install["bytes"]:
        die("plan reserve total %d exceeds installed BYTES %d"
            % (total_reserve, install["bytes"]))
    plan_sha = sha256_bytes(pipe.canonical(plan))
    snapshot_sha = sha256_bytes(pipe.canonical(snapshot))
    idhash = idhash_of(bid)
    try:
        tmpdir, digests = freeze_sources(root, idhash, plan, snapshot, pipe.canonical)
    except OSError as exc:
        die("cannot freeze input snapshot: %s" % exc)
    with registry_lock(root):
        existing = load_batch(root, idhash)
        if existing is not None:
            rmtree_safe(tmpdir)
            if existing["plan_sha"] != plan_sha or existing["snapshot_sha"] != snapshot_sha:
                die("batch ID %r is already bound to different plan/snapshot" % bid)
            print_json(status_of(existing))
            return
        final_dir = p_srcdir(root, idhash)
        if os.path.isdir(final_dir):
            rmtree_safe(final_dir)
        os.rename(tmpdir, final_dir)
        fsync_dir(os.path.dirname(final_dir))
        rec = {"format": FORMAT, "id": bid, "release": install["release"],
               "plan": plan, "plan_sha": plan_sha, "snapshot_sha": snapshot_sha,
               "source_digests": digests, "order": order,
               "state": "queued", "result": None, "error": None,
               "nodes": {nid: {"state": "waiting", "identity": None,
                               "value_sha256": None, "error": None}
                         for nid in (node["id"] for node in plan["nodes"])},
               "retry_tokens": [], "seq": next_seq(root)}
        save_batch(root, rec)
    print_json(status_of(rec))


def cmd_status(root, bid):
    root = canon_root(root)
    require_install(root)
    rec = load_batch(root, idhash_of(bid))
    if rec is None:
        die("unknown batch ID: %r" % bid)
    print_json(status_of(rec))


def cmd_cancel(root, bid):
    root = canon_root(root)
    require_install(root)
    idhash = idhash_of(bid)

    def work(rec):
        if rec["state"] in ("queued", "running", "failed"):
            rec["state"] = "cancelled"
            rec["result"] = None
            rec["error"] = None
            return True
        return False

    rec = mutate_batch(root, idhash, work)
    if rec is None:
        die("unknown batch ID: %r" % bid)
    print_json(status_of(rec))


def cmd_retry(root, bid, token):
    root = canon_root(root)
    require_install(root)
    if not token:
        die("retry TOKEN must be a non-empty string")
    idhash = idhash_of(bid)
    with registry_lock(root):
        rec = load_batch(root, idhash)
        if rec is None:
            die("unknown batch ID: %r" % bid)
        if rec["state"] == "cancelled":
            die("batch %r is cancelled; retry rejected" % bid)
        if token in rec["retry_tokens"]:
            print_json(status_of(rec))  # 同 TOKEN 重试只返回现状
            return
        if rec["state"] == "failed":
            rec["retry_tokens"].append(token)
            for st in rec["nodes"].values():
                if st["state"] == "failed":
                    st["state"] = "waiting"
                    st["identity"] = None
                    st["error"] = None
            rec["state"] = "queued"
            rec["error"] = None
            save_batch(root, rec)
        print_json(status_of(rec))


# ---------------------------------------------------------------------------
# 回收
# ---------------------------------------------------------------------------

def exec_lock_path(root, execid):
    return os.path.join(p_exec(root), execid, "lock")


def trash_move(root, path):
    dst = os.path.join(p_trash(root), uuid.uuid4().hex)
    try:
        os.rename(path, dst)
        return dst
    except OSError:
        return None


def empty_trash(root):
    removed = 0
    tdir = p_trash(root)
    try:
        names = os.listdir(tdir)
    except OSError:
        return 0
    for name in names:
        path = os.path.join(tdir, name)
        if os.path.isdir(path) and not os.path.islink(path):
            removed += dir_size(path)
        else:
            try:
                removed += os.path.getsize(path)
            except OSError:
                pass
        rmtree_safe(path)
    return removed


def gc_sweep(root, log=None):
    """两阶段回收：注册表锁内判定引用并改名入 trash，锁外实际删除。
    返回实际删除字节数。调用方须已持有 gc.lock。"""
    moved = []
    with registry_lock(root):
        cells_ref, pkgs_ref = compute_refs(root)
        install = read_install(root)
        current = install["release"] if install else None
        # 单元格：无引用载荷与发布失败的临时目录
        for name in os.listdir(p_cells(root)):
            path = os.path.join(p_cells(root), name)
            if name.startswith(".tmp-"):
                execid = name[len(".tmp-"):]
                if flock_held(exec_lock_path(root, execid)):
                    continue  # 活跃执行器正在发布
                dst = trash_move(root, path)
            elif name in cells_ref:
                continue
            else:
                dst = trash_move(root, path)
            if dst:
                moved.append(dst)
        # 包：当前安装包始终保留，其他包最后引用解除后回收
        for name in os.listdir(p_packages(root)):
            if not name.endswith(".pkg"):
                continue
            release = name[:-4]
            if release == current or release in pkgs_ref:
                continue
            dst = trash_move(root, os.path.join(p_packages(root), name))
            if dst:
                moved.append(dst)
        pkg_root = os.path.join(root, "pkg")
        for name in os.listdir(pkg_root):
            path = os.path.join(pkg_root, name)
            if name.startswith(".tmp-"):
                dst = trash_move(root, path)
            elif name == current or name in pkgs_ref:
                continue
            else:
                dst = trash_move(root, path)
            if dst:
                moved.append(dst)
        # 执行器目录：锁未被持有即证明退出，残留文件实际删除前仍占额；
        # 新近变更的目录留给协调器先归接退出记录，避免抢删造成误判
        now = time.time()
        for execid in os.listdir(p_exec(root)):
            path = os.path.join(p_exec(root), execid)
            if flock_held(os.path.join(path, "lock")):
                continue
            try:
                if now - os.path.getmtime(path) < 2.0:
                    continue
            except OSError:
                pass
            dst = trash_move(root, path)
            if dst:
                moved.append(dst)
    removed = empty_trash(root)
    if log and moved:
        log("gc: removed %d bytes from %d objects" % (removed, len(moved)))
    return removed


def cmd_collect(root):
    root = canon_root(root)
    require_install(root)
    lock = FileLock(gc_lock_path(root))
    lock.acquire()
    try:
        removed = gc_sweep(root)
    finally:
        lock.release()
    print_json({"removed_bytes": removed})


# ---------------------------------------------------------------------------
# 协调器
# ---------------------------------------------------------------------------

class Coordinator(object):
    def __init__(self, root, install):
        self.root = root
        self.install = install
        self.interp = install["interpreter"]
        self.slots = install["slots"]
        self.quota_bytes = install["bytes"]
        self.records = {}          # idhash -> rec（近缓存，锁内重读为准）
        self.mtimes = {}
        self.executions = {}       # identity -> dict(execid, dir, cap, release, op, popen)
        self.cell_meta = {}        # identity -> meta
        self.cell_sizes = {}       # identity -> size（计费）
        self.abandoned = {}        # path -> bytes（未删除废弃文件仍占额）
        self.attempts = {}         # identity -> lost 重试次数
        self.waiters = {}          # identity -> [(idhash, nid)]
        self.peak_slots = 0
        self.peak_bytes = 0
        self.launch_retry = {}   # identity -> 下次允许启动时刻（启动失败退避）
        self.stop = False
        self.last_gc = 0.0
        self.last_capacity_log = 0.0
        self.logfh = open(os.path.join(p_logs(root), "serve.log"), "ab", buffering=0)

    # -- 基础 ----------------------------------------------------------------

    def log(self, message):
        line = ("%s %s\n" % (time.strftime("%Y-%m-%dT%H:%M:%S"), message)).encode("utf-8")
        try:
            self.logfh.write(line)
        except OSError:
            pass

    def bytes_used(self):
        return (sum(ex["cap"] for ex in self.executions.values())
                + sum(self.cell_sizes.values()) + sum(self.abandoned.values()))

    def slots_used(self):
        return len(self.executions)

    def get_cell(self, identity):
        meta = self.cell_meta.get(identity)
        if meta is not None and not os.path.isfile(
                os.path.join(cell_dir(self.root, identity), "meta.json")):
            # 外部回收已删除：剔除过期缓存并同步配额账
            self.cell_meta.pop(identity, None)
            self.cell_sizes.pop(identity, None)
            meta = None
        if meta is None:
            meta = read_cell_meta(self.root, identity)
            if meta:
                self.cell_meta[identity] = meta
                self.cell_sizes.setdefault(identity, meta["size"])
        return meta

    # -- 恢复 ----------------------------------------------------------------

    def recover(self):
        for rec in list_batches(self.root):
            self.records[idhash_of(rec["id"])] = rec
        self.waiters = self.compute_waiters()  # 死执行器的失败须能落到订阅者
        # 单元格建账
        for name in os.listdir(p_cells(self.root)):
            path = os.path.join(p_cells(self.root), name)
            if name.startswith(".tmp-"):
                self.abandoned[path] = dir_size(path)
                continue
            meta = read_cell_meta(self.root, name)
            if meta:
                self.cell_meta[name] = meta
                self.cell_sizes[name] = meta["size"]
            else:
                self.abandoned[path] = dir_size(path)  # 破损单元格按废弃计
        # 执行器目录：归接活执行器，不重复启动同一节点
        for execid in os.listdir(p_exec(self.root)):
            path = os.path.join(p_exec(self.root), execid)
            req = read_json_file(os.path.join(path, "request.json"))
            if not isinstance(req, dict) or "identity" not in req:
                self.abandoned[path] = dir_size(path)
                continue
            if flock_held(os.path.join(path, "lock")):
                identity = req["identity"]
                self.executions[identity] = {
                    "execid": execid, "dir": path, "cap": int(req["reserve"]),
                    "release": req["release"], "op": req["op"], "popen": None}
                self.log("recover: adopted live executor %s for %s"
                         % (execid, identity[:12]))
            else:
                self.handle_dead_executor(req["identity"], path, req)
        self.log("recover: %d batches, %d cells, %d live executors, bytes_used=%d"
                 % (len(self.records), len(self.cell_sizes), len(self.executions),
                    self.bytes_used()))

    # -- 记录刷新 ------------------------------------------------------------

    def reload_records(self):
        bdir = p_batches(self.root)
        try:
            names = os.listdir(bdir)
        except OSError:
            return
        for name in names:
            if not name.endswith(".json"):
                continue
            path = os.path.join(bdir, name)
            try:
                mtime = os.path.getmtime(path)
            except OSError:
                continue
            if self.mtimes.get(path) == mtime:
                continue
            rec = read_json_file(path)
            if isinstance(rec, dict) and rec.get("format") == FORMAT:
                self.records[idhash_of(rec["id"])] = rec
                self.mtimes[path] = mtime
            else:
                self.log("reload: skipping unreadable record %s" % name)

    def reap_children(self):
        while True:
            try:
                pid, _status = os.waitpid(-1, os.WNOHANG)
            except (ChildProcessError, OSError):
                return
            if pid <= 0:
                return

    # -- 订阅 ----------------------------------------------------------------

    def compute_waiters(self):
        waiters = {}
        for idhash, rec in self.records.items():
            if rec["state"] not in ("queued", "running"):
                continue
            for nid, st in rec["nodes"].items():
                if st["state"] != "waiting":
                    continue
                identity = st.get("identity") or compute_identity(rec, nid)
                if identity:
                    waiters.setdefault(identity, []).append((idhash, nid))
        return waiters

    # -- 执行器退出处理 --------------------------------------------------------

    def handle_dead_executor(self, identity, execdir, req):
        meta = self.get_cell(identity)
        if meta:
            self.cell_sizes[identity] = meta["size"]  # 完成载荷计实际大小
        else:
            record = read_json_file(os.path.join(execdir, "exit-record.json"))
            kind = record.get("error_kind") if isinstance(record, dict) else None
            error = record.get("error") if isinstance(record, dict) else None
            if kind in ("business", "bad_output"):
                self.fail_waiters(identity, error or "stage failed")
            elif kind == "reserve":
                cap = int(req.get("reserve", 0))
                for idhash, nid in list(self.waiters.get(identity, [])):
                    rec = self.records.get(idhash)
                    if not rec:
                        continue
                    reserve = node_map(rec)[nid]["reserve"]
                    if reserve <= cap:
                        self.fail_waiter(idhash, nid, identity,
                                         "node %s output exceeded reserve %d bytes"
                                         % (nid, reserve))
                # 额度更大的订阅者等待大额度执行（admit 阶段以更大 cap 重试）
            else:
                attempts = self.attempts.get(identity, 0) + 1
                self.attempts[identity] = attempts
                if attempts >= LOST_ATTEMPT_LIMIT:
                    self.fail_waiters(identity, "executor lost repeatedly: %s"
                                    % (error or "no exit record"))
                else:
                    self.log("executor lost for %s (attempt %d); will relaunch"
                             % (identity[:12], attempts))
        if not rmtree_safe(execdir):
            self.abandoned[execdir] = dir_size(execdir)
        self.executions.pop(identity, None)

    def fail_waiters(self, identity, error):
        for idhash, nid in list(self.waiters.get(identity, [])):
            self.fail_waiter(idhash, nid, identity, "node %s: %s" % (nid, error))

    def fail_waiter(self, idhash, nid, identity, error):
        def work(rec):
            if rec["state"] not in ("queued", "running"):
                return False
            st = rec["nodes"].get(nid)
            if not st or st["state"] != "waiting" or st.get("identity") not in (None, identity):
                return False
            st["state"] = "failed"
            st["error"] = error
            rec["state"] = "failed"
            rec["error"] = error
            return True
        rec = mutate_batch(self.root, idhash, work)
        if rec:
            self.records[idhash] = rec
            self.log("batch %s failed: %s" % (rec["id"], error))

    def process_dead_executions(self):
        for identity, ex in list(self.executions.items()):
            if flock_held(os.path.join(ex["dir"], "lock")):
                continue  # 暂停、失联执行器继续占槽
            req = read_json_file(os.path.join(ex["dir"], "request.json")) or {
                "identity": identity, "reserve": ex["cap"]}
            self.handle_dead_executor(identity, ex["dir"], req)

    # -- 批次推进 --------------------------------------------------------------

    def advance_batches(self):
        for idhash, rec in list(self.records.items()):
            if rec["state"] not in ("queued", "running"):
                continue
            self.advance_batch(idhash)

    def advance_batch(self, idhash):
        finalize = []

        def work(rec):
            if rec["state"] not in ("queued", "running"):
                return False
            changed = False
            by_id = node_map(rec)
            for nid in rec["order"]:
                st = rec["nodes"][nid]
                if st["state"] != "waiting":
                    continue
                identity = st.get("identity") or compute_identity(rec, nid)
                if not identity:
                    continue
                if st.get("identity") != identity:
                    st["identity"] = identity  # 持久化订阅：待运行消费者保护载荷
                    changed = True
                meta = self.get_cell(identity)
                if not meta:
                    continue
                reserve = by_id[nid]["reserve"]
                if meta["size"] <= reserve:
                    st["state"] = "completed"
                    st["value_sha256"] = meta["value_sha256"]
                    changed = True
                else:
                    st["state"] = "failed"
                    st["error"] = ("node %s output %d bytes exceeds reserve %d"
                                   % (nid, meta["size"], reserve))
                    rec["state"] = "failed"
                    rec["error"] = st["error"]
                    changed = True
            if rec["state"] in ("queued", "running"):
                if any(s["state"] != "waiting" for s in rec["nodes"].values()) \
                        and rec["state"] == "queued":
                    rec["state"] = "running"
                    changed = True
                if all(s["state"] == "completed" for s in rec["nodes"].values()):
                    finalize.append(rec["nodes"][rec["plan"]["target"]]["identity"])
            return changed

        rec = mutate_batch(self.root, idhash, work)
        if rec is not None:
            self.records[idhash] = rec
        if finalize:
            self.finalize_success(idhash, finalize[0])

    def finalize_success(self, idhash, target_identity):
        path = os.path.join(cell_dir(self.root, target_identity), "payload.json")
        try:
            with open(path, "rb") as fh:
                data = fh.read()
            value = json.loads(data[:-1].decode("utf-8"))
        except (OSError, ValueError) as exc:
            self.log("finalize: target payload %s unreadable (%s); "
                     "reverting target node to waiting" % (target_identity[:12], exc))
            # 目标载荷缺失：回退 target 节点重新执行，避免永久停滞
            def revert(rec):
                if rec["state"] in ("queued", "running"):
                    st = rec["nodes"][rec["plan"]["target"]]
                    if st["state"] == "completed":
                        st["state"] = "waiting"
                        st["value_sha256"] = None
                        return True
                return False
            rec = mutate_batch(self.root, idhash, revert)
            if rec:
                self.records[idhash] = rec
            return

        def work(rec):
            if rec["state"] in ("queued", "running") and all(
                    s["state"] == "completed" for s in rec["nodes"].values()):
                rec["state"] = "succeeded"
                rec["result"] = value
                rec["error"] = None
                return True
            return False

        rec = mutate_batch(self.root, idhash, work)
        if rec and rec["state"] == "succeeded":
            self.records[idhash] = rec
            self.log("batch %s succeeded" % rec["id"])

    # -- 准入与启动 ------------------------------------------------------------

    def admit(self):
        pending = []
        for identity, subs in self.waiters.items():
            if identity in self.executions or self.get_cell(identity):
                continue
            seqs = [self.records[ih]["seq"] for ih, _n in subs if ih in self.records]
            if not seqs:
                continue
            if time.time() < self.launch_retry.get(identity, 0.0):
                continue
            pending.append((min(seqs), identity, subs))
        pending.sort()
        gc = FileLock(gc_lock_path(self.root), blocking=False)
        for _seq, identity, subs in pending:
            if self.slots_used() >= self.slots:
                break
            caps = [node_map(self.records[ih])[_nid]["reserve"] for ih, _nid in subs
                    if ih in self.records]
            if not caps:
                continue
            cap = max(caps)
            if self.bytes_used() + cap > self.quota_bytes:
                self.log_capacity_wait(identity, cap)
                continue
            if not gc.acquire():
                return  # 回收进行中，下一趟再启动，避免目录竞态
            try:
                self.launch(identity, cap, subs)
            finally:
                gc.release()

    def log_capacity_wait(self, identity, cap):
        now = time.time()
        if now - self.last_capacity_log < 5.0:
            return
        self.last_capacity_log = now
        self.log("capacity wait: identity=%s need=%d bytes_used=%d quota=%d "
                 "slots=%d/%d（failed 批次保留引用占额时须取消或重试释放）"
                 % (identity[:12], cap, self.bytes_used(), self.quota_bytes,
                    self.slots_used(), self.slots))

    def launch(self, identity, cap, subs):
        idhash, nid = subs[0]
        rec = self.records[idhash]
        node = node_map(rec)[nid]
        release = rec["release"]
        inputs = []
        for ref in node["inputs"]:
            if "source" in ref:
                inputs.append({"kind": "source",
                               "path": source_file(self.root, idhash, ref["source"])})
            else:
                up = rec["nodes"][ref["node"]]
                inputs.append({"kind": "cell",
                               "path": os.path.join(
                                   cell_dir(self.root, up["identity"]), "payload.json")})
        execid = uuid.uuid4().hex
        execdir = os.path.join(p_exec(self.root), execid)
        lockfd = None
        logfd = None
        try:
            os.makedirs(execdir)
            lockfd = os.open(os.path.join(execdir, "lock"), os.O_RDWR | os.O_CREAT)
            fcntl.flock(lockfd, fcntl.LOCK_EX)  # 启动登记前持锁：退出证明不依赖 PID
            req = {"format": FORMAT, "identity": identity, "release": release,
                   "op": node["op"], "reserve": cap, "lock_fd": lockfd,
                   "inputs": inputs, "cells_dir": p_cells(self.root)}
            write_json_atomic(os.path.join(execdir, "request.json"), req)
            logfd = os.open(os.path.join(execdir, "executor.log"),
                            os.O_WRONLY | os.O_CREAT | os.O_APPEND)
            executor = os.path.join(p_pkgdir(self.root, release), "executor.py")
            popen = subprocess.Popen(
                [self.interp, executor, execdir],
                stdin=subprocess.DEVNULL, stdout=logfd, stderr=logfd,
                pass_fds=(lockfd,), start_new_session=True)
        except OSError as exc:
            if lockfd is not None:
                os.close(lockfd)
            if logfd is not None:
                os.close(logfd)
            rmtree_safe(execdir)
            fails, _until = self.launch_retry.get(identity + ":n", (0, 0.0))
            fails += 1
            self.launch_retry[identity + ":n"] = (fails, time.time() + min(2 ** fails, 30))
            self.launch_retry[identity] = time.time() + min(2 ** fails, 30)
            self.log("launch failed for %s: %s (backoff %d)" % (identity[:12], exc, fails))
            return  # 资源性失败：退避重试，不伪报业务失败
        os.close(lockfd)
        os.close(logfd)
        self.executions[identity] = {"execid": execid, "dir": execdir, "cap": cap,
                                     "release": release, "op": node["op"],
                                     "popen": popen}
        self.log("launch: executor %s identity=%s op=%s cap=%d slots=%d bytes=%d"
                 % (execid, identity[:12], node["op"], cap, self.slots_used(),
                    self.bytes_used()))
        for ih, _nid in subs:
            def work(rec2):
                if rec2["state"] == "queued":
                    rec2["state"] = "running"
                    return True
                return False
            updated = mutate_batch(self.root, ih, work)
            if updated:
                self.records[ih] = updated

    def prune_executions(self):
        """无订阅执行器：自家子进程终止回收；归接的孤儿让其自然结束。"""
        for identity, ex in list(self.executions.items()):
            if self.waiters.get(identity):
                continue
            popen = ex.get("popen")
            if popen is not None and popen.poll() is None:
                try:
                    popen.kill()
                    self.log("prune: killed unsubscribed executor %s" % ex["execid"])
                except OSError:
                    pass

    # -- 用量与 GC -------------------------------------------------------------

    def write_usage(self):
        self.peak_slots = max(self.peak_slots, self.slots_used())
        self.peak_bytes = max(self.peak_bytes, self.bytes_used())
        usage = {"slots": self.slots, "slots_used": self.slots_used(),
                 "bytes": self.quota_bytes, "bytes_used": self.bytes_used(),
                 "peak_slots": self.peak_slots, "peak_bytes": self.peak_bytes,
                 "executions": [{"identity": i, "op": ex["op"], "cap": ex["cap"]}
                                for i, ex in sorted(self.executions.items())],
                 "ts": time.time()}
        try:
            write_json_atomic(os.path.join(self.root, "usage.json"), usage)
        except OSError:
            pass

    def maybe_gc(self):
        if time.time() - self.last_gc < GC_INTERVAL:
            return
        self.last_gc = time.time()
        lock = FileLock(gc_lock_path(self.root), blocking=False)
        if not lock.acquire():
            return
        try:
            gc_sweep(self.root, log=self.log)
            # 回收后同步内存账目
            for identity in list(self.cell_sizes):
                if not os.path.isdir(cell_dir(self.root, identity)):
                    self.cell_sizes.pop(identity, None)
                    self.cell_meta.pop(identity, None)
            for path in list(self.abandoned):
                if not os.path.exists(path):
                    self.abandoned.pop(path, None)
        finally:
            lock.release()

    # -- 主循环 ----------------------------------------------------------------

    def run(self):
        def stop_handler(_signum, _frame):
            self.stop = True
        signal.signal(signal.SIGTERM, stop_handler)
        signal.signal(signal.SIGINT, stop_handler)
        self.log("serve: started slots=%d bytes=%d release=%s"
                 % (self.slots, self.quota_bytes, self.install["release"]))
        while not self.stop:
            try:
                self.reload_records()
                self.reap_children()
                self.waiters = self.compute_waiters()
                self.process_dead_executions()
                self.advance_batches()
                self.admit()
                self.prune_executions()
                self.maybe_gc()
                self.write_usage()
            except Exception:
                self.log("serve loop error:\n" + traceback.format_exc())
            time.sleep(PASS_INTERVAL)
        self.log("serve: stopped")


def cmd_serve(root):
    root = canon_root(root)
    install = require_install(root)
    ensure_layout(root)
    lock = FileLock(os.path.join(root, "serve.lock"), blocking=False)
    if not lock.acquire():
        print_json({"status": "already_serving"})
        return
    try:
        coord = Coordinator(root, install)
        coord.recover()
        coord.run()
    finally:
        lock.release()


# ---------------------------------------------------------------------------
# 入口
# ---------------------------------------------------------------------------

USAGE = ("usage: ops.py build OUT | install ROOT PACKAGE SLOTS BYTES | "
         "submit ROOT ID PLAN SNAPSHOT | serve ROOT | status ROOT ID | "
         "cancel ROOT ID | retry ROOT ID TOKEN | collect ROOT")


def main(argv):
    if not argv:
        die(USAGE)
    cmd, args = argv[0], argv[1:]
    if cmd == "build" and len(args) == 1:
        cmd_build(args[0])
    elif cmd == "install" and len(args) == 4:
        cmd_install(args[0], args[1], args[2], args[3])
    elif cmd == "submit" and len(args) == 4:
        cmd_submit(args[0], args[1], args[2], args[3])
    elif cmd == "serve" and len(args) == 1:
        cmd_serve(args[0])
    elif cmd == "status" and len(args) == 2:
        cmd_status(args[0], args[1])
    elif cmd == "cancel" and len(args) == 2:
        cmd_cancel(args[0], args[1])
    elif cmd == "retry" and len(args) == 3:
        cmd_retry(args[0], args[1], args[2])
    elif cmd == "collect" and len(args) == 1:
        cmd_collect(args[0])
    else:
        die(USAGE)


if __name__ == "__main__":
    main(sys.argv[1:])
