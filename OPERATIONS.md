# 小时加工链：调用与现场约束

## 已有业务
pipeline.py 不作业务修改。manual.py run PLAN SNAPSHOT 读取两个 UTF-8 JSON 文件，输出 {"result":[...]}。
manual.py demo 展示真实样例；manual.py sample plan 和 manual.py sample snapshot 输出可保存的完整输入。
samples.fixture() 返回 plan、snapshot、expected 的新对象；checks.py normal 成功输出 BUSINESS_REGRESSION_OK，保留原断言。
样例预期：E/0 为 count=2,sum=66；W/-1 为 count=1,sum=13；W/1 为 count=1,sum=19。

PLAN={"nodes":[节点...],"target":"节点ID"}。
节点={"id":"唯一字符串","op":"normalize|calibrate|hourly","inputs":[引用...],"reserve":正整数字节数}。
引用只能为 {"source":"源名"} 或 {"node":"节点ID"}，有序且允许重复；snapshot 为源名到JSON值的映射。
全部节点为有限DAG且全部执行，target为唯一交付结果；业务转换见transform及样例。
输入不含重复对象键或非有限数。reserve为节点stdout的字节上界，含换行。
既有子进程入口：python3 pipeline.py stage OP。
stdin为 {"inputs":[按引用顺序解析的值...]}，stdout为一个JSON值及换行，非零退出为业务失败。
业务进程可装入内存；不要求流式改造。既有stage只写stdout，不启动后代。

## 发布运行层的公开入口
以下均是直接argv，路径允许空格与非ASCII字符，不借助shell。新增ops.py；内部存储布局自行设计。
build OUT：python3 ops.py build OUT；输出 {"release":"包文件SHA256"}。
install ROOT PACKAGE SLOTS BYTES：python3 ops.py install ROOT PACKAGE SLOTS BYTES；
SLOTS/BYTES为正整数，输出 {"release":"包摘要"}；重复安装可更新版本，配额须与首次一致。
submit ROOT ID PLAN SNAPSHOT：python3 ops.py submit ROOT ID PLAN SNAPSHOT；持久接纳后返回批次状态。
serve ROOT：python3 ops.py serve ROOT，常驻协调；同根重复启动须安全，可以选主。
status ROOT ID、cancel ROOT ID、retry ROOT ID TOKEN：均由python3 ops.py直接调用，返回批次状态。
collect ROOT：python3 ops.py collect ROOT，输出 {"removed_bytes":本次实际删除的中间载荷及包字节数}。

批次状态={"id":ID,"release":"绑定包摘要","state":"queued|running|succeeded|failed|cancelled",
"result":成功时target值否则null,"error":失败说明字符串否则null}。
成功须全部节点完成且结果持久；status为一致视图。命令错误退出2并输出 {"error":"可读说明"}。
submit的ID为非空字符串，接纳时冻结plan和全部源值并绑定当前完整安装包；
同ID同规范plan/snapshot返回原批次当前状态，忽略后来安装版本；异内容拒绝，结束后也不例外。
规范字节采用pipeline.canonical；删除或覆盖原输入文件不影响已接纳批次。
非法图或全部节点reserve之和超过BYTES须在接纳前拒绝。
cancel将queued/running/failed批次取消并解除其订阅，succeeded/cancelled返回原状态。
retry的TOKEN为非空请求标识，首次请求可把failed批次重新排队，沿原包、原输入继续；
同TOKEN重试只返回现状，不能在后续再次失败时重排。queued/running/succeeded只返回现状，cancelled拒绝retry。
安装、提交、取消、重试及完成发布须符合调用实时先后的原子历史；响应丢失重试不能产生第二份效果。

## 执行、引用和配额口径
计算身份=(包摘要,op,有序输入规范值摘要)，不含节点名或批次名。
同身份至多有一个仍可能执行的执行器；不同发布包不共享节点。多个批次共享同一份完成载荷。
不同订阅的reserve独立生效：结果超过某订阅上限仅使该订阅批次失败，不能使额度足够者误失败；
可等待大额度执行，不要求对正在运行的执行器在线扩容，失败执行允许重试。
共享节点失败不会自动重排其他failed批次。取消一批不终止仍有其他订阅的执行。

SLOTS计仍可能执行的节点，暂停和失联执行器继续占槽。
BYTES计中间区：未退出执行器各计完整reserve，已退出完成载荷计实际大小；
输出和完成文件计费转换不能重复或漏计，废弃文件实际删除前仍占额，共享载荷只计一次。
输入快照、唯一target的最终交付、注册元数据和包文件不计BYTES；不得把其他中间结果伪作最终交付。
超过reserve的字节不得写入中间区，不能只结束后检查。辅助进程不得留下未计费的旧stage。
释放槽位须证明执行器退出；PID可能复用，PID查询、心跳或超时单独不能作为退出证明。

待运行消费者、活跃执行器及其他批次的引用都保护载荷。
failed批次保留续跑仍需的完成结果；同批恢复、重试不能重算仍受引用的持久完成节点。
完整stdout校验并连同计算身份发布持久完成记录后才算完成；算完但未发布可重试。
无引用载荷回收后，新批次可重新计算；原批已完成下游不要求保留其不再需要的祖先载荷。
成功交付、取消可解除该批中间订阅；最终结果及请求绑定永久保留。
活动引用保护旧包；当前安装包始终保留，其他包最后引用解除后collect实际删除。

资源不足可等待，不能伪报业务失败。允许保守安全调度，不要求最优吞吐；
不能把未启动节点登记成已占用配额，也不能固定串行所有批次来规避恢复和配额问题；允许准入时作保守安全检查。
两批互不依赖且两批全部reserve总和可容纳时须能重叠执行；同批可容纳的就绪分支也须能并行。
有限任务、执行器最终结束且存在可行推进次序时，不能因调度造成半成品互等而永久停滞。
failed批次的保留引用确实占满容量时可以等待并说明原因；其余可安全推进的节点须继续运行。
GC、取消、重试、安装可与执行并发；不能跨载荷读取、复制、哈希或进程等待持全局排他锁。

## 现场发布和回归
验收为macOS本机、已装Python 3.9+，仅用标准库，无网络、账号或外部系统服务；不用Docker/Compose。
包字节须不受构建时刻及源码绝对路径影响；包内含校验清单，截断、篡改及非法成员在改变运行根前拒绝。
不要求打包解释器，但须探测解释器能力并给出可读失败。安装目录提供ops.py入口。
移走原源码后安装版仍能提交、执行、查询和回收；旧批只用绑定包。无需业务Schema迁移及自动回退。
运行根为本地APFS目录；要求进程持久化边界中断恢复，不要求断电及网络文件系统。
安装交付用户级LaunchAgent，绝对解释器和安装路径、持久日志、自动重启，并说明装载/卸载argv。
有登录用户会话时实际装载；无会话时生成配置并直接执行其ProgramArguments验证，区分配置验证与实际装载。
上线证据使用真实stage子进程，实际暂停执行器、杀协调器重开，包含启动登记前后中断；
核对共享节点下取消/重试/回收、输出上限、旧包续跑及两种分支重叠的业务结果和峰值配额。
操作说明须覆盖安装、更新、提交、取消、失败续跑、回收及异常恢复，不用假worker代替现场证据。
保持.pointwise-runtime.json的version=1、kind=cli、setup为argv列表；
最终verify为["python3","ops_checks.py","verify"]，重跑原断言和实际部署恢复核对；
demo为["python3","ops_checks.py","demo"]，展示安装版真实小时结果及一次共享执行恢复状态。
