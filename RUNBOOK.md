# 小时加工链常驻作业服务：操作与恢复说明

本文档覆盖 `ops.py` 发布运行层的安装、更新、提交、取消、失败续跑、回收及异常恢复。
业务核 `pipeline.py` 不作修改；所有命令均为直接 argv（路径允许空格与非 ASCII），
命令错误退出 2 并输出 `{"error":"可读说明"}`。

## 组件与运行根布局

```
ROOT/
  ops.py                 # 安装时复制的统一入口
  install.json           # 当前安装：release、SLOTS、BYTES、解释器绝对路径
  packages/<sha>.pkg     # 发布包文件（不可变，按摘要命名）
  pkg/<sha>/             # 解包后的 pipeline.py + executor.py（按摘要隔离）
  registry/              # 注册元数据：批次记录、冻结输入、顺序号、互斥锁
  cells/<identity>/      # 持久完成载荷：payload.json + meta.json（跨批共享）
  exec/<execid>/         # 执行器工作目录：request、lock、out.tmp、退出记录
  trash/                 # 两阶段回收暂存
  logs/serve.log         # 协调器持久日志（含容量等待原因）
  usage.json             # 协调器每趟写出的用量视图（含峰值）
  launchd/<label>.plist  # 用户级 LaunchAgent 配置
```

计算身份 =（包摘要, op, 有序输入规范值摘要），不含节点名或批次名；
同身份至多一个执行器，多批共享同一份完成载荷。

## 构建

```
python3 ops.py build OUT
```

输出 `{"release":"包文件SHA256"}`。包字节不受构建时刻及源码绝对路径影响
（固定时间戳、排序成员、相对路径），内含校验清单 `manifest.json`；
重复构建同一源码得到同一 release。

## 安装与更新

```
python3 ops.py install ROOT PACKAGE SLOTS BYTES
```

- SLOTS/BYTES 为正整数，输出 `{"release":"包摘要"}`。
- 安装前校验包：截断、篡改、非法成员在改变运行根前拒绝。
- 安装前探测解释器能力（绝对路径、Python 3.9+ 标准库），失败给出可读说明。
- 重复安装可更新版本；SLOTS/BYTES 须与首次安装一致，否则拒绝。
- 更新后旧批继续用绑定时的原包执行，新批绑定新包；当前安装包始终保留，
  其他包在最后引用（活动或 failed 批次）解除后由 collect 实际删除。

## 提交、查询、取消、失败续跑

```
python3 ops.py submit ROOT ID PLAN SNAPSHOT   # 持久接纳后返回批次状态
python3 ops.py status ROOT ID                 # 查询一致视图
python3 ops.py cancel ROOT ID                 # 取消并解除订阅
python3 ops.py retry  ROOT ID TOKEN           # 失败批次幂等重排
```

批次状态：`{"id","release","state","result","error"}`，
state ∈ queued|running|succeeded|failed|cancelled。

- 接纳时冻结 plan 与全部源值、绑定当前完整安装包；删除或覆盖原输入文件不影响。
- 同 ID 同规范 plan/snapshot 返回原批次当前状态（忽略后来安装版本）；
  异内容拒绝，结束后也不例外。非法图或 reserve 总和超 BYTES 在接纳前拒绝。
- cancel 对 queued/running/failed 生效并解除订阅；succeeded/cancelled 返回原状态。
  取消一批不终止仍有其他订阅的执行。
- retry 的 TOKEN 为非空请求标识：首次请求把 failed 批次沿原包、原输入重新排队；
  同 TOKEN 重试只返回现状，后续再次失败不会重排；cancelled 拒绝 retry。
- 响应丢失后用同一 ID/TOKEN 重试不会产生第二份效果（注册表互斥串行化）。

## 常驻协调与 LaunchAgent

```
python3 ops.py serve ROOT     # 常驻协调；同根重复启动安全（选主，重复者退出 0）
```

安装时在 `ROOT/launchd/<label>.plist` 生成用户级 LaunchAgent：
绝对解释器与安装路径、`KeepAlive` 自动重启、`RunAtLoad`、持久日志
（`logs/launchd.out.log` / `launchd.err.log`）。

- 有登录用户会话（macOS gui 域可用）时，安装会实际装载到
  `~/Library/LaunchAgents/<label>.plist` 并输出已装载说明。
- 无会话时仅生成配置，并直接执行其 ProgramArguments 验证
  （输出会区分"配置验证"与"实际装载"）。

装载/卸载 argv（手动）：

```
launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/<label>.plist   # 装载
launchctl bootout  gui/$(id -u)/<label>                                 # 卸载
# 旧式：launchctl load -w <plist> / launchctl unload -w <plist>
```

## 回收

```
python3 ops.py collect ROOT    # 输出 {"removed_bytes":本次实际删除字节数}
```

回收无引用单元格载荷、废弃执行器残留、最后引用已解除的旧包。
引用口径：活动批次的完成载荷与待运行订阅、failed 批次续跑仍需的完成结果、
活跃执行器、当前安装包及活动/failed 批次绑定包。协调器每 15 秒亦自动回收；
回收与执行并发，两阶段（锁内判定改名、锁外删除），不持锁读/复制/哈希载荷。

## 配额口径

- SLOTS：仍可能执行的节点数；暂停（SIGSTOP）与失联执行器继续占槽，
  槽位释放以 flock 获得为退出证明（不凭 PID、心跳或超时）。
- BYTES（中间区）：未退出执行器各计完整 reserve；完成载荷计实际大小；
  废弃文件实际删除前仍占额；共享载荷只计一次。输入快照、唯一 target 最终交付、
  注册元数据与包文件不计入。超过 reserve 的字节不写入中间区（流式截断即杀）。
- 各批 reserve 独立生效：结果超某订阅上限仅使该订阅批次失败；
  额度更大者可等待大额度执行（失败执行以更大上限重试）。
- 容量不足时等待并在 `logs/serve.log` 说明原因（含 failed 批次保留引用占额提示），
  不伪报业务失败；failed 批次占满容量时，取消或重试该批可释放。

## 异常恢复

- **协调器被杀/崩溃**：直接重新 `serve ROOT`（或由 LaunchAgent 自动重启）。
  重开扫描执行目录：锁被持有的活执行器归接监控，不重复启动同一节点；
  已死执行器按退出记录归接（已发布载荷直接采用，未发布可重试）。
- **执行器孤儿**：协调器死亡不影响执行器；其完成载荷在新协调器归接后采用，
  仍占配额直至退出被证明。
- **中断于启动登记前后**：登记即持锁，崩溃留下的空目录按废弃计费并回收；
  批次记录原子落盘，未接纳的提交不产生效果。
- **磁盘压力**：BYTES 满时新执行器等待；`collect` 释放无引用载荷；
  failed 批次保留引用占额时取消或重试该批。
- **坏包**：install 校验拒绝，运行根不变；线上旧包不受影响。
- **核对**：`python3 ops_checks.py verify` 重跑原业务断言并做实际部署恢复核对；
  `python3 ops_checks.py demo` 展示安装版真实小时结果及一次共享执行恢复状态。
