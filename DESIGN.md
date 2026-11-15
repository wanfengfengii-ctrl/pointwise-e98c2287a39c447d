# 历史快照到期与对象回收设计（DESIGN）

本轮只交付本讨论文档与可重跑证据，不实现回收器。lake.py、catalog.py、objects.py、checks.py、replay.py、samples.py、gc_samples.py、gc_observe.py、gc_check.py 全部保持原样；运行清单 .pointwise-runtime.json 仅把 verify 切换为 `["python3","gc_check.py","verify"]`，demo 保持观察入口。

## 0. 范围与假设

- 单进程、一个 SQLite 目录连接、一个回收者；操作按观察器暂停点交错（gc_observe.py:29-38 的 get/put 探针、gc_observe.py:51-54 的 after_state、gc_observe.py:56-59 的 after_commit）。
- 对象读写与删除成功，不注入存储故障；不做跨进程租约、崩溃恢复、多回收者、目录性能优化、订阅/迁移/分析索引。
- 实际载荷 I/O（objects.get/put/drop）期间不得持目录写锁；允许另设短临界区（见 3.3）。
- 到期前已取得保护的请求可完成；到期后的新 read/delete 拒绝（EXPIRED_SNAPSHOT）。取得快照与到期之间有明确排序（3.2 不变式 I3）。

## 1. 现状调用链与源码定位

目录事务与对象存储相互独立：Catalog.write（catalog.py:22-34）在最外层执行 BEGIN IMMEDIATE（catalog.py:26）/COMMIT（catalog.py:30）/ROLLBACK（catalog.py:33），嵌套调用复用外层事务（catalog.py:24）；Catalog.commit 要求事务内（catalog.py:49-50），快照号 sid=parent+1（catalog.py:51-52）并推进 head（catalog.py:57）。Objects.put 产出规范字节（objects.py:16），文件模式排他创建（objects.py:20），同名异值报 IMMUTABLE_CONFLICT（objects.py:24、objects.py:26），但内存模式在对象缺失时直接接受写入（objects.py:25-28）——这是回收后名称保护的缺口。

- seed（lake.py:10-16）：先写全部对象（lake.py:11-12），再开目录事务检查 ALREADY_SEEDED（lake.py:14-15）并提交快照1（lake.py:16）。对象写入不在目录事务内。
- read（lake.py:18-25）：catalog.state 取得快照（lake.py:19；未知快照 NO_SNAPSHOT 在 catalog.py:41-42），随后逐文件 objects.get（lake.py:22，objects.py:30-32）并过滤位置墓碑（lake.py:23-24）。全程不持目录写锁。
- delete（lake.py:27-37）：先 state(snapshot) 验证源快照（lake.py:28），再读载荷验证位置（lake.py:29-30，BAD_POSITION），再进目录写事务（lake.py:31）判断当前布局（STALE_LAYOUT，lake.py:33-34；重复删除直接返回，lake.py:35-36），最后 commit（lake.py:37）。位置删除也读载荷，不能只保护 read。
- prepare（lake.py:39-54）：state 固定 base（lake.py:40）→ 校验 inputs（lake.py:41-42，BAD_INPUTS）→ 读输入载荷（lake.py:43）→ 校验 layout 恰好覆盖全部物理位置（lake.py:44-47，BAD_LAYOUT）→ 逐组 objects.put 输出 rewrite-JOB-组号（lake.py:48-53）→ 返回计划（lake.py:54）。部分输出在交错中真实存在（观察点 after_put）。
- publish（lake.py:56-66）：目录写事务（lake.py:57）内取当前 state（lake.py:58），head≠base 报 PUBLISH_CONFLICT（lake.py:59-60），输入失效报 BAD_PLAN（lake.py:61-63），否则 commit 新快照（lake.py:64-66）。失败发布不取消计划。
- rewrite（lake.py:68-71）：原封装用一个目录写事务包住 prepare+publish，载荷 I/O 落在目录写锁内；按题意缩短为顺序调用（3.4），串行无交错时结果不变。
- 观察器：对象清单 ProbeObjects.inventory（gc_observe.py:16-19，内存/文件两种存储同款）；raw 删除 drop_raw 不检查保护（gc_observe.py:21-27）；expire/cancel/collect 基线仅记录请求（gc_observe.py:101-103）；rollback 在真实外层 Catalog.write 中执行再回滚（gc_observe.py:104-113）；raw_drop/raw_put 是原始存储诊断（gc_observe.py:114-117）；end 事件携带 head、objects、in_transaction（gc_observe.py:128-129）；run 入口 gc_observe.py:134-141。

## 2. 两种保护策略比较

### 策略 A：目录驻留保护状态（本设计选择）

到期集合、在途读保护、重写计划、永久名称登记全部作为 SQLite 表放进目录连接，与快照同事务、同回滚：

- 取得保护与到期由 BEGIN IMMEDIATE 的写事务序列化定序，无隐含前提；
- 发布移交（计划保护 → 快照保护）与外层事务回滚恢复直接复用 catalog.py:22-34 的嵌套事务语义，不需要自建补偿；
- collect 只读已提交视图，活动事务期间整轮暂缓；
- 代价：每个 read/delete 多两个 O(1) 短写事务（pin 插入/删除），读路径出现短暂目录写锁。

### 策略 B：进程内内存注册表

expired、pins、plans、held 放进程内存，用一把互斥锁保证"检查到期+登记保护"的原子性；目录最多保留 names 表：

- 优点：读路径不加目录写事务、无模式变更，collect 取内存快照更便宜；
- 缺点：外层事务回滚不会自动恢复内存态，必须在 catalog.write（catalog.py:22-34）最外层挂提交/回滚钩子并重放撤销日志，expire/cancel/发布移交每条变更路径都要登记，漏一处即泄漏或误放保护；目录与内存双真相源需在每个提交边界对账；内存名称登记在进程重启后丢失，"永久不复用"约定变弱（本轮虽不要求崩溃恢复，但约定本身要求永久）；证据链不如 A 直观。

**选择 A**：用每操作 O(1) 个短目录事务换取 SQLite 保证的序列化与回滚语义，单进程单连接下这把短写锁不是瓶颈，载荷 I/O 始终在其外。

## 3. 策略 A 详细设计

### 3.1 新增目录状态（与快照同一连接、同一事务）

```sql
CREATE TABLE expired(sid INTEGER PRIMARY KEY);                    -- 已到期快照，永久保留
CREATE TABLE pins(pin INTEGER PRIMARY KEY, sid INTEGER NOT NULL); -- 在途 read/delete 的快照保护
CREATE TABLE plans(job TEXT PRIMARY KEY, base INTEGER NOT NULL,
                   status TEXT NOT NULL,     -- active / published / cancelled
                   inputs TEXT NOT NULL,     -- JSON 数组，活动期保护输入
                   outputs TEXT NOT NULL);   -- JSON 数组，移交/取消时解除 held 用
CREATE TABLE names(name TEXT PRIMARY KEY, held INTEGER NOT NULL); -- 永久名称登记；held=1 表示尚未由快照接管
```

可达集（collect 的保护视图）：

```
R = ⋃ files(未到期快照 sid)            -- head 不可到期，天然包含在内
  ∪ ⋃ files(pins 指向的 sid)           -- 在途 read/delete 已取得的保护
  ∪ ⋃ plans[status=active].inputs      -- 活动重写的输入（含已返回未结束的计划）
  ∪ names[held=1]                      -- 已登记未接管的名字（seed 进行中、活动计划输出）
```

### 3.2 不变式

- **I1 登记先于写入**：任何业务 Objects.put 之前，名字已在 names 登记且被 held 或计划/快照覆盖；collect 先枚举对象清单再读保护视图，晚登记的名字不可能出现在早先的清单里。
- **I2 已提交不可达单调**：名字一旦在已提交视图中不可达，之后的已提交变迁（新 pin 只落在未到期快照、新计划输入只取自当前 head、输出名被 names 唯一拦截、到期集合只增不减）都不会让它重新可达；回滚只撤销未提交变更，已提交视图不受影响。
- **I3 取得与到期有序**：pin 插入事务与 expire 事务都是 BEGIN IMMEDIATE 短写事务，SQLite 的序列化就是"取得快照"与"到期"的明确排序；不以载荷已打开作为隐含前提。
- **I4 移交原子**：publish 的快照行、plans.status、names.held 在同一目录事务内变更；外层提交前不算已提交，回滚后计划保护原样恢复。
- **I5 回滚平衡**：pin 的插入与释放在操作自身的同一事务作用域内成对出现，外层回滚同时撤销两者，既不泄漏也不误放保护。

### 3.3 短临界区清单（载荷 I/O 全部在锁外）

| 临界区 | 类型 | 内容 | 粒度 |
|---|---|---|---|
| CS1 取得保护 | 写事务 | 校验 NO_SNAPSHOT/EXPIRED_SNAPSHOT，插入 pins 行 | O(1) |
| CS2 释放保护 | 写事务 | 删除 pins 行 | O(1) |
| CS3 计划登记 | 写事务 | 读 base state、校验 inputs、插入 plans 与输出名 held=1 | O(|inputs|+|outputs|) |
| CS4 发布移交 | 写事务 | 原 publish 校验与 commit，同事务改 plans.status 与 held | O(|outputs|) |
| CS5 取消 | 写事务 | plans.status=cancelled，输出名 held=0 | O(|outputs|) |
| CS6 到期 | 写事务 | 校验 HEAD_SNAPSHOT/NO_SNAPSHOT，插入 expired | O(1) |
| CS7 回收决策 | 读事务 | 读 expired/pins/plans/names/files 计算 R | O(元数据) |

objects.get/put/drop 与对象清单枚举（gc_observe.py:16-19 同款）不持目录写锁；决策（CS7）与实际删除分离，删除在锁外逐个进行。

### 3.4 关键伪代码

```python
def acquire(snapshot):                      # CS1，取得与到期的排序点（I3）
    with catalog.write():                   # catalog.py:22-34，嵌套时并入外层
        sid = catalog.head() if snapshot is None else snapshot
        if sid not in snapshots: raise NO_SNAPSHOT        # catalog.py:41-42 同款
        if sid in expired:     raise EXPIRED_SNAPSHOT
        return sid, insert_into_pins(sid)

def release(pin):                           # CS2，与 acquire 同事务作用域（I5）
    with catalog.write(): delete_from_pins(pin)

def read(snapshot=None):                    # 改写 lake.py:18-25
    sid, pin = acquire(snapshot)
    try:   ... 原 lake.py:19-25：state(sid) 后逐文件 objects.get，不持目录写锁
    finally: release(pin)

def delete(snapshot, fid, pos):             # 改写 lake.py:27-37
    sid, pin = acquire(snapshot)            # 顺序：NO_SNAPSHOT → EXPIRED_SNAPSHOT
    try:   ... 原 lake.py:28-37：BAD_POSITION（读载荷，锁外）→ 写事务内
               STALE_LAYOUT/重复删除 → commit；pin 覆盖验证与布局判断全程
    finally: release(pin)

def prepare(job, inputs, layout):           # 改写 lake.py:39-54
    with catalog.write():                   # CS3：base 取得与计划保护原子完成
        base, files, dead = catalog.state() # 原 lake.py:40
        校验 inputs（原 lake.py:41-42，BAD_INPUTS）
        outputs = ["rewrite-%s-%d" % (job, i) for i in range(len(layout))]
        for n in outputs:
            if n in names: raise NAME_RETIRED           # 永久名称约定（3.8）
        insert plans(job, base, "active", inputs, outputs)
        insert names(outputs, held=1)
    try:
        payload = {f: objects.get(f) for f in inputs}   # 原 lake.py:43，锁外
        校验 layout（原 lake.py:44-47，BAD_LAYOUT）
        逐组 objects.put（原 lake.py:48-53，锁外；部分输出已被 held 保护）
    except:                                  # 失败不留下保护泄漏
        with catalog.write(): delete plans[job]; names[outputs].held = 0
        raise
    return {"job": job, "base": base, "inputs": sorted(inputs), "outputs": outputs}

def publish(plan):                             # 改写 lake.py:56-66
    with catalog.write():                   # CS4，原 lake.py:57 的写事务
        row = plans.get(plan["job"]) or raise NO_PLAN
        if row.status == "cancelled": raise CANCELLED_JOB
        sid, files, dead = catalog.state()  # 原 lake.py:58
        if sid != plan["base"]: raise PUBLISH_CONFLICT   # 原 lake.py:59-60
        if not set(plan["inputs"]) <= files: raise BAD_PLAN  # 原 lake.py:61-63
        result = catalog.commit(...)         # 原 lake.py:64-66
        row.status = "published"             # 移交：计划保护 → 快照保护（I4）
        names[row.outputs].held = 0          # 同事务；外层回滚整体恢复（I5）
        return result

def cancel(job):                             # CS5
    with catalog.write():
        row = plans.get(job) or raise NO_JOB
        if row.status == "active":
            row.status = "cancelled"; names[row.outputs].held = 0
        # 重复取消、已发布：无副作用；目录事务内的取消随外层回滚恢复

def expire(snapshot):                        # CS6
    with catalog.write():
        sid = snapshot（None 视同 head）
        if sid not in snapshots: raise NO_SNAPSHOT
        if sid == catalog.head(): raise HEAD_SNAPSHOT
        insert or ignore expired(sid)        # 重复到期无副作用；回滚恢复到期前状态

def collect():                               # 唯一回收者，可多次调用
    if catalog.db.in_transaction: return []  # 活动事务期间暂缓；不缓存候选，下轮重算
    inv = objects.inventory()                # 先枚举对象名（I1），不读载荷
    with catalog.read_tx():                  # CS7：已提交保护视图
        R = files(未到期) ∪ files(pins) ∪ plans[active].inputs ∪ names[held=1]
    doomed = sorted(set(inv) - R)            # I2 保证决策后删除仍安全
    for fid in doomed: objects.drop(fid)     # 锁外删除载荷；names 行永久保留
    return doomed

def seed(files):                             # 改写 lake.py:10-16，满足 I1
    with catalog.write():
        if catalog.head() != 0: raise ALREADY_SEEDED   # 原 lake.py:14-15
        insert names(files, held=1)                     # 重名 → NAME_RETIRED
    for fid, rows in files.items(): objects.put(fid, rows)   # 原 lake.py:11-12，锁外
    with catalog.write():
        sid = catalog.commit(set(files), set())         # 原 lake.py:16
        names[files].held = 0
        return sid

def rewrite(job, inputs, layout):            # 缩短原 lake.py:68-71 的锁范围
    return publish(prepare(job, inputs, layout))   # 串行无交错时结果与原封装一致
```

Objects 增加存储原语 `drop(fid)`（文件 unlink / 内存 del），仅 collect 在 I1–I5 判定之后调用；它与 gc_observe.py:21-27 的 drop_raw 的区别在于调用点之前必有保护判定，raw 探针不是业务端口。

### 3.5 外层事务接入

- 显式外层事务（gc_observe.py:104-113 的 rollback 场景）中，expire、cancel、publish 移交、pin 的插入/释放都经 catalog.write 的嵌套分支（catalog.py:24）并入外层事务，回滚时整体恢复到期前状态、计划状态与保护状态——这是选策略 A 的核心收益。
- collect 用同一连接的 in_transaction 判据（catalog.py:24 所用标志）检测活动事务，事务期间返回空表整轮暂缓：未提交的到期与移交绝不能驱动删除（否则 rollback 样例中 a/b 会被误删）。
- 暂缓不缓存候选：每轮 collect 从已提交状态重算 R 与清单差集，事务结束后的下一轮自然看到全部候选，"一次暂缓"不会变成"永久遗忘"。
- 不要求把新 prepare 放进调用者尚未提交的外层事务：prepare 的登记事务若被外层回滚撤销，已返回计划随后 publish 报 NO_PLAN，属于明确不支持的用法。

### 3.6 安全性：为什么不会误删

- head 与未到期快照：files(未到期) ⊆ R，head 不可到期（expire 报 HEAD_SNAPSHOT），当前与历史读取目标恒受保护。
- 在途读取与位置删除：read/delete 先经 CS1 取得 pin（I3 排序），pin 指向快照的文件集 ⊆ R，到期与回收穿插不影响已完成取保护的操作；delete 的载荷验证（lake.py:29-30）与布局判断（lake.py:33-34）全程在 pin 覆盖下。
- 活动重写：prepare 的输入读取、部分输出、已返回未结束的计划分别由 plans[active].inputs 与 names.held 覆盖；CS3 登记先于任何输出写入（I1），prepared 样例中 after_put 暂停点的 collect 看不到可删候选。
- 共享对象：R 是各持有者的并集，shared_empty 样例中 empty 同时被快照1/2引用，仅到期其一不会进入候选；取消或失败只解除自身持有的 held/计划保护，不触碰其他持有者。
- 发布移交窗口：I4 保证快照行与 held 解除同生共死；提交前 collect 的已提交视图里计划仍在保护，提交后快照接管，回滚则计划保护原样恢复——任何时刻输出都不会掉出 R。
- 决策竞争：collect 先枚举清单再读已提交视图；I1 保证晚登记的名字不在早先清单中，I2 保证已提交不可达的名字不会再可达，故决策之后、删除之前出现的任何新保护都不指向候选。

### 3.7 活性：为什么最终实际删除

操作停止、事务结束、保护解除后：pins 为空、plans 全部终态（published/cancelled）、names.held 全 0、无活动事务。此时任何无引用对象 o 不在任何未到期快照的文件集中（否则即为有引用），于是下一轮 collect：不在事务内（不暂缓）→ o 在清单中、不在 R 中 → 当轮实际删除并返回。每轮 collect 都重算全集差集，不存在被永久遗忘的候选；expired 快照的载荷一旦失去全部引用即被回收，不会永久保留全部历史；计划保护是具体的 names.held 与 plans 行，取消/失败后对象真实可回收，不把"输出丢失后重新计算"当成保护。

### 3.8 永久名称约定

names 表永久登记每个业务写路径见过的名字，行永不删除；seed 与 prepare 的登记事务（CS3）发现重名即报 NAME_RETIRED，使"回收后不得把旧名分配给新对象"在业务层可执行。collect 只删载荷、不动 names。基线存储在对象缺失时接受同名异值写入（objects.py:25-28 内存分支；gc_check.py 的 raw 探针演示），cancel_name 样例的 raw_drop/raw_put（gc_observe.py:114-117）正是绕过目录的诊断，不是合法回收协议操作；本设计用登记事务把禁令前移到写路径入口，而不是照抄 raw 探针作为业务端口。

### 3.9 成本核算

| 类别 | 业务操作（每次） | collect（每轮） | 空间（累计） |
|---|---|---|---|
| 目录扫描 | read/delete 各 2 个 O(1) 写事务；prepare 1 个 O(|in|+|out|) 登记事务；publish/cancel/expire O(1) 行变更 | 读 expired O(E)、未到期快照 files 行 O(ΣF)、pins O(P)、活动 plans O(J·(|in|+|out|))、held 名 O(H) | snapshots/files/tombstones 元数据 O(S·F)，可选裁剪已到期且无 pin 快照的文件行（保留 snapshots+expired 行以区分 NO_SNAPSHOT/EXPIRED_SNAPSHOT）；plans O(J)；pins O(P) |
| 对象枚举 | 无 | 1 次清单枚举 O(N_obj)，只列名字 | — |
| 载荷读写 | read |files| 次 get；delete 1 次 get；prepare |in| 次 get + |out| 次 put；均不持目录写锁 | 仅 |doomed| 次 drop，零载荷读取（不以读载荷判断可达性） | 静止后只存可达对象 |
| 永久名称保留 | 每个新对象名 1 行 names | 不增删 | O(历史名字总数)，永不删除——名称不复用约定的固有代价 |

## 4. 五组公开时序：基线实测与方案预测

预测中 protected 取回收者实际使用的已提交保护视图（活动事务期间未提交变更不改变该视图，因为 collect 暂缓）；deleted 为该操作本轮实际回收的对象名。完整逐操作预测见第 6 节证据。

### reader（读间隙中的到期与回收）

- 基线实测：读取在 before_get(b) 暂停，期间完成 prepare/publish，expire 与 collect 仅记录请求（gc_observe.py:101-103）；during 与 after 的 objects 恒为 [a,b,rewrite-r-0,rewrite-r-1]；late 读快照1仍成功。
- 方案预测：读前取得 pin(1)；during 的 collect 看到 pin，删 0 项；原读取仍返回 [7,7,9,11]；读末释放 pin 后 after 实际删除 [a,b]；late 读报 EXPIRED_SNAPSHOT。分歧点：到期读取由成功变拒绝，旧 a/b 在读取结束后真实释放，不永久占空间。

### prepared（部分输出与发布冲突）

- 基线实测：partial 时 objects=[a,b,rewrite-p-0]；conflict 报 PUBLISH_CONFLICT；released 时四对象俱在，collect 全是空转。
- 方案预测：计划登记先于首个输出写入，partial/held 两轮 collect 均删 0 项；冲突失败不取消计划，输出名保持 held；显式 cancel 后 released 实际删除 [rewrite-p-0,rewrite-p-1]；a/b 始终由 head=2 保护。分歧点：取消后输出真实可回收，而不是永久滞留。

### rollback（外层事务回滚）

- 基线实测：事务内 read_inside 读到快照2，tx 回滚后 head 恢复1、objects 不变，随后 publish 得快照2；collect 三次均空转。
- 方案预测：collect_inside 检测 in_transaction 整轮暂缓；回滚自动恢复计划 active 与输出 held，after_rollback 删 0 项（不把回滚前的临时可达性当最终状态）；重新 publish 并 expire(1) 后 after_commit 实际删除 [a,b]；current 读 head=2 得 [7,9,7,11]。分歧点：回滚后计划可再发布且对象最终真实回收。

### cancel_name（取消、原始删除与名称复用）

- 基线实测：cancel/collect 仅记录；raw_remove 直接删掉 rewrite-c-0，raw_reuse 同名异值写入成功——展示存储层限制。
- 方案预测：cancel 后 collect 已合法删除 [rewrite-c-0,rewrite-c-1]，名称永久留在 names 登记；raw_remove 此时找不到对象（诊断报错），raw_reuse 的同名异值写入是绕过协议的探针行为，业务写路径会被 NAME_RETIRED 拦截。两步 raw 诊断在预测中单列解释，不计入协议删除（deleted 均为空）。分歧点：协议删除先于探针发生，名称复用由登记事务禁止。

### shared_empty（共享对象与空文件）

- 基线实测：快照1/2共享 a/b/empty，快照3移除 empty；两轮 collect 空转，最终 objects 仍含 empty；当前读为 [7,9,11]。
- 方案预测：仅 expire_old(1) 后 shared 删 0 项（empty 被快照2共享）；expire_middle(2) 后 empty 轮实际删除 [empty]；零行内容仍有规范字节对象（objects.py:16），存在性按名称清单判定，与内容无关；current 读 head=3 得 [7,9,11]。分歧点：共享对象在两个引用快照都到期后真实删除。

## 5. 回归、核验与人工审查要点

保持不变的回归：`python3 checks.py`（原业务断言）、`python3 replay.py --input examples/move.json`（原时序）、`python3 gc_check.py baseline`（原回归与观察核对）、`python3 gc_observe.py all`（真实观察）。文档核验：`python3 gc_check.py verify` 重放证据、检查预测覆盖与源码行号；.pointwise-runtime.json 的 verify 已接通该命令，demo 不变。

人工审查请逐步核对：读到期排序（reader 的 pin 与 late 拒绝）、部分输出保护（prepared 的 partial/held）、提交/回滚移交窗口（rollback 的暂缓与恢复）、共享对象（shared_empty 的两段到期）、静止后的实际回收（各 collect 的 deleted），以及 cancel_name 中 raw 诊断与协议删除的区分。结构检查通过不代表协议正确。

## 6. 证据

以下 actual 为 `gc_observe.run(cases()[name])` 的完整事件数组（基线实测），predicted 为按真实 end 事件顺序排列的逐操作方案预测。

<!-- gc-evidence -->
~~~json
{"reader":{"actual":[{"event":"begin","id":"seed","op":"seed"},{"event":"before_put","value":"a","in_transaction":false},{"event":"after_put","value":"a","in_transaction":false},{"event":"before_put","value":"b","in_transaction":false},{"event":"after_put","value":"b","in_transaction":false},{"event":"sql","value":"BEGIN IMMEDIATE","in_transaction":false},{"event":"after_commit","value":1,"in_transaction":true},{"event":"sql","value":"COMMIT","in_transaction":true},{"event":"end","id":"seed","op":"seed","result":1,"head":1,"objects":["a","b"],"in_transaction":false},{"event":"begin","id":"reader","op":"read"},{"event":"after_state","value":1,"in_transaction":false},{"event":"before_get","value":"a","in_transaction":false},{"event":"after_get","value":"a","in_transaction":false},{"event":"before_get","value":"b","in_transaction":false},{"event":"begin","id":"p","op":"prepare"},{"event":"after_state","value":1,"in_transaction":false},{"event":"before_get","value":"a","in_transaction":false},{"event":"after_get","value":"a","in_transaction":false},{"event":"before_get","value":"b","in_transaction":false},{"event":"after_get","value":"b","in_transaction":false},{"event":"before_put","value":"rewrite-r-0","in_transaction":false},{"event":"after_put","value":"rewrite-r-0","in_transaction":false},{"event":"before_put","value":"rewrite-r-1","in_transaction":false},{"event":"after_put","value":"rewrite-r-1","in_transaction":false},{"event":"end","id":"p","op":"prepare","result":{"base":1,"inputs":["a","b"],"outputs":["rewrite-r-0","rewrite-r-1"]},"head":1,"objects":["a","b","rewrite-r-0","rewrite-r-1"],"in_transaction":false},{"event":"begin","id":"publish","op":"publish"},{"event":"sql","value":"BEGIN IMMEDIATE","in_transaction":false},{"event":"after_state","value":1,"in_transaction":true},{"event":"after_commit","value":2,"in_transaction":true},{"event":"sql","value":"COMMIT","in_transaction":true},{"event":"end","id":"publish","op":"publish","result":2,"head":2,"objects":["a","b","rewrite-r-0","rewrite-r-1"],"in_transaction":false},{"event":"begin","id":"expire","op":"expire"},{"event":"end","id":"expire","op":"expire","result":{"request_only":{"id":"expire","op":"expire","snapshot":1}},"head":2,"objects":["a","b","rewrite-r-0","rewrite-r-1"],"in_transaction":false},{"event":"begin","id":"during","op":"collect"},{"event":"end","id":"during","op":"collect","result":{"request_only":{"id":"during","op":"collect"}},"head":2,"objects":["a","b","rewrite-r-0","rewrite-r-1"],"in_transaction":false},{"event":"after_get","value":"b","in_transaction":false},{"event":"end","id":"reader","op":"read","result":{"snapshot":1,"rows":[{"file":"a","pos":0,"value":{"reading":7}},{"file":"a","pos":1,"value":{"reading":7}},{"file":"b","pos":0,"value":{"reading":9}},{"file":"b","pos":1,"value":{"reading":11}}]},"head":2,"objects":["a","b","rewrite-r-0","rewrite-r-1"],"in_transaction":false},{"event":"begin","id":"after","op":"collect"},{"event":"end","id":"after","op":"collect","result":{"request_only":{"id":"after","op":"collect"}},"head":2,"objects":["a","b","rewrite-r-0","rewrite-r-1"],"in_transaction":false},{"event":"begin","id":"late","op":"read"},{"event":"after_state","value":1,"in_transaction":false},{"event":"before_get","value":"a","in_transaction":false},{"event":"after_get","value":"a","in_transaction":false},{"event":"before_get","value":"b","in_transaction":false},{"event":"after_get","value":"b","in_transaction":false},{"event":"end","id":"late","op":"read","result":{"snapshot":1,"rows":[{"file":"a","pos":0,"value":{"reading":7}},{"file":"a","pos":1,"value":{"reading":7}},{"file":"b","pos":0,"value":{"reading":9}},{"file":"b","pos":1,"value":{"reading":11}}]},"head":2,"objects":["a","b","rewrite-r-0","rewrite-r-1"],"in_transaction":false}],"predicted":[{"id":"seed","outcome":"发布快照1：先登记名称 a/b（held=1），锁外写载荷，再提交目录并解除 held","protected":["a","b"],"deleted":[],"reason":"head=1 未到期，快照1文件集保护 a/b"},{"id":"p","outcome":"返回计划 {base:1, inputs:[a,b], outputs:[rewrite-r-0,rewrite-r-1]}","protected":["a","b","rewrite-r-0","rewrite-r-1"],"deleted":[],"reason":"登记事务把输入交给活动计划保护、输出名以 held 保护；在途读取的 pin(1) 同时覆盖 a/b"},{"id":"publish","outcome":"发布快照2，计划保护在同一目录事务内移交给快照2，held 解除","protected":["a","b","rewrite-r-0","rewrite-r-1"],"deleted":[],"reason":"快照1（含在途 pin）与快照2共同覆盖全部四个对象"},{"id":"expire","outcome":"快照1登记到期，无返回","protected":["a","b","rewrite-r-0","rewrite-r-1"],"deleted":[],"reason":"在途读取已于到期前取得 pin(1)，a/b 仍受保护；快照2保护 rewrite-r-0/1"},{"id":"during","outcome":"返回 []：本轮无实际删除","protected":["a","b","rewrite-r-0","rewrite-r-1"],"deleted":[],"reason":"已提交视图中 pin(1) 仍持有，a/b 可达；rewrite-r-0/1 属未到期快照2，候选为空"},{"id":"reader","outcome":"在途读取完成，返回快照1的 [7,7,9,11]，读末释放 pin(1)","protected":["rewrite-r-0","rewrite-r-1"],"deleted":[],"reason":"pin 释放且快照1已到期，a/b 自此不再受保护；快照2仍保护两个输出"},{"id":"after","outcome":"返回 [a, b]：实际删除 a、b","protected":["rewrite-r-0","rewrite-r-1"],"deleted":["a","b"],"reason":"快照1已到期、无 pin、无计划引用，a/b 不可达且不会重新可达；删除不影响 head=2"},{"id":"late","outcome":"拒绝：EXPIRED_SNAPSHOT","protected":["rewrite-r-0","rewrite-r-1"],"deleted":[],"reason":"取保护临界区发现快照1已在 expired 集合，到期后新读取被拒绝；与基线 late 仍成功不同"}]},"prepared":{"actual":[{"event":"begin","id":"seed","op":"seed"},{"event":"before_put","value":"a","in_transaction":false},{"event":"after_put","value":"a","in_transaction":false},{"event":"before_put","value":"b","in_transaction":false},{"event":"after_put","value":"b","in_transaction":false},{"event":"sql","value":"BEGIN IMMEDIATE","in_transaction":false},{"event":"after_commit","value":1,"in_transaction":true},{"event":"sql","value":"COMMIT","in_transaction":true},{"event":"end","id":"seed","op":"seed","result":1,"head":1,"objects":["a","b"],"in_transaction":false},{"event":"begin","id":"p","op":"prepare"},{"event":"after_state","value":1,"in_transaction":false},{"event":"before_get","value":"a","in_transaction":false},{"event":"after_get","value":"a","in_transaction":false},{"event":"before_get","value":"b","in_transaction":false},{"event":"after_get","value":"b","in_transaction":false},{"event":"before_put","value":"rewrite-p-0","in_transaction":false},{"event":"after_put","value":"rewrite-p-0","in_transaction":false},{"event":"begin","id":"partial","op":"collect"},{"event":"end","id":"partial","op":"collect","result":{"request_only":{"id":"partial","op":"collect"}},"head":1,"objects":["a","b","rewrite-p-0"],"in_transaction":false},{"event":"before_put","value":"rewrite-p-1","in_transaction":false},{"event":"after_put","value":"rewrite-p-1","in_transaction":false},{"event":"end","id":"p","op":"prepare","result":{"base":1,"inputs":["a","b"],"outputs":["rewrite-p-0","rewrite-p-1"]},"head":1,"objects":["a","b","rewrite-p-0","rewrite-p-1"],"in_transaction":false},{"event":"begin","id":"delete","op":"delete"},{"event":"after_state","value":1,"in_transaction":false},{"event":"before_get","value":"a","in_transaction":false},{"event":"after_get","value":"a","in_transaction":false},{"event":"sql","value":"BEGIN IMMEDIATE","in_transaction":false},{"event":"after_state","value":1,"in_transaction":true},{"event":"after_commit","value":2,"in_transaction":true},{"event":"sql","value":"COMMIT","in_transaction":true},{"event":"end","id":"delete","op":"delete","result":2,"head":2,"objects":["a","b","rewrite-p-0","rewrite-p-1"],"in_transaction":false},{"event":"begin","id":"expire","op":"expire"},{"event":"end","id":"expire","op":"expire","result":{"request_only":{"id":"expire","op":"expire","snapshot":1}},"head":2,"objects":["a","b","rewrite-p-0","rewrite-p-1"],"in_transaction":false},{"event":"begin","id":"conflict","op":"publish"},{"event":"sql","value":"BEGIN IMMEDIATE","in_transaction":false},{"event":"after_state","value":2,"in_transaction":true},{"event":"sql","value":"ROLLBACK","in_transaction":true},{"event":"end","id":"conflict","op":"publish","error":"PUBLISH_CONFLICT","head":2,"objects":["a","b","rewrite-p-0","rewrite-p-1"],"in_transaction":false},{"event":"begin","id":"held","op":"collect"},{"event":"end","id":"held","op":"collect","result":{"request_only":{"id":"held","op":"collect"}},"head":2,"objects":["a","b","rewrite-p-0","rewrite-p-1"],"in_transaction":false},{"event":"begin","id":"cancel","op":"cancel"},{"event":"end","id":"cancel","op":"cancel","result":{"request_only":{"id":"cancel","op":"cancel","job":"p"}},"head":2,"objects":["a","b","rewrite-p-0","rewrite-p-1"],"in_transaction":false},{"event":"begin","id":"released","op":"collect"},{"event":"end","id":"released","op":"collect","result":{"request_only":{"id":"released","op":"collect"}},"head":2,"objects":["a","b","rewrite-p-0","rewrite-p-1"],"in_transaction":false}],"predicted":[{"id":"seed","outcome":"发布快照1：先登记名称 a/b（held=1），锁外写载荷，再提交目录并解除 held","protected":["a","b"],"deleted":[],"reason":"head=1 未到期，快照1文件集保护 a/b"},{"id":"partial","outcome":"返回 []：本轮无实际删除","protected":["a","b","rewrite-p-0","rewrite-p-1"],"deleted":[],"reason":"计划 p 已在写首个输出前的登记事务中注册：输入 a/b 由计划与快照1保护，输出名以 held 保护，已写出的部分输出 rewrite-p-0 不会被误删"},{"id":"p","outcome":"返回计划 {base:1, inputs:[a,b], outputs:[rewrite-p-0,rewrite-p-1]}","protected":["a","b","rewrite-p-0","rewrite-p-1"],"deleted":[],"reason":"活动计划的输入与 held 输出名均在保护集内"},{"id":"delete","outcome":"源快照1未到期，取得 pin(1) 后完成位置验证并提交快照2（墓碑 (a,0)），随后释放 pin","protected":["a","b","rewrite-p-0","rewrite-p-1"],"deleted":[],"reason":"快照1、快照2与活动计划共同保护全部四个对象"},{"id":"expire","outcome":"快照1登记到期","protected":["a","b","rewrite-p-0","rewrite-p-1"],"deleted":[],"reason":"head=2 未到期保护 a/b；计划未结束，输出名仍 held"},{"id":"conflict","outcome":"PUBLISH_CONFLICT：head=2 不等于 base=1，计划保持活动","protected":["a","b","rewrite-p-0","rewrite-p-1"],"deleted":[],"reason":"失败发布不取消计划，输出名继续 held，冲突前后部分输出都不得删除"},{"id":"held","outcome":"返回 []：本轮无实际删除","protected":["a","b","rewrite-p-0","rewrite-p-1"],"deleted":[],"reason":"已提交视图中计划 p 仍活动，rewrite-p-0/1 受 held 保护，候选为空"},{"id":"cancel","outcome":"取消计划 p：状态置 cancelled，输出名 held 解除","protected":["a","b"],"deleted":[],"reason":"取消只释放计划自身保护；a/b 由未到期快照2保护"},{"id":"released","outcome":"返回 [rewrite-p-0, rewrite-p-1]：实际删除两个输出","protected":["a","b"],"deleted":["rewrite-p-0","rewrite-p-1"],"reason":"计划已取消且无其他持有者，两个输出不可达，本轮实际回收；a/b 仍为 head 所用"}]},"rollback":{"actual":[{"event":"begin","id":"seed","op":"seed"},{"event":"before_put","value":"a","in_transaction":false},{"event":"after_put","value":"a","in_transaction":false},{"event":"before_put","value":"b","in_transaction":false},{"event":"after_put","value":"b","in_transaction":false},{"event":"sql","value":"BEGIN IMMEDIATE","in_transaction":false},{"event":"after_commit","value":1,"in_transaction":true},{"event":"sql","value":"COMMIT","in_transaction":true},{"event":"end","id":"seed","op":"seed","result":1,"head":1,"objects":["a","b"],"in_transaction":false},{"event":"begin","id":"p","op":"prepare"},{"event":"after_state","value":1,"in_transaction":false},{"event":"before_get","value":"a","in_transaction":false},{"event":"after_get","value":"a","in_transaction":false},{"event":"before_get","value":"b","in_transaction":false},{"event":"after_get","value":"b","in_transaction":false},{"event":"before_put","value":"rewrite-p-0","in_transaction":false},{"event":"after_put","value":"rewrite-p-0","in_transaction":false},{"event":"before_put","value":"rewrite-p-1","in_transaction":false},{"event":"after_put","value":"rewrite-p-1","in_transaction":false},{"event":"end","id":"p","op":"prepare","result":{"base":1,"inputs":["a","b"],"outputs":["rewrite-p-0","rewrite-p-1"]},"head":1,"objects":["a","b","rewrite-p-0","rewrite-p-1"],"in_transaction":false},{"event":"begin","id":"tx","op":"rollback"},{"event":"sql","value":"BEGIN IMMEDIATE","in_transaction":false},{"event":"begin","id":"publish_inside","op":"publish"},{"event":"after_state","value":1,"in_transaction":true},{"event":"after_commit","value":2,"in_transaction":true},{"event":"end","id":"publish_inside","op":"publish","result":2,"head":2,"objects":["a","b","rewrite-p-0","rewrite-p-1"],"in_transaction":true},{"event":"begin","id":"expire_inside","op":"expire"},{"event":"end","id":"expire_inside","op":"expire","result":{"request_only":{"id":"expire_inside","op":"expire","snapshot":1}},"head":2,"objects":["a","b","rewrite-p-0","rewrite-p-1"],"in_transaction":true},{"event":"begin","id":"collect_inside","op":"collect"},{"event":"end","id":"collect_inside","op":"collect","result":{"request_only":{"id":"collect_inside","op":"collect"}},"head":2,"objects":["a","b","rewrite-p-0","rewrite-p-1"],"in_transaction":true},{"event":"begin","id":"read_inside","op":"read"},{"event":"after_state","value":2,"in_transaction":true},{"event":"before_get","value":"rewrite-p-0","in_transaction":true},{"event":"after_get","value":"rewrite-p-0","in_transaction":true},{"event":"before_get","value":"rewrite-p-1","in_transaction":true},{"event":"after_get","value":"rewrite-p-1","in_transaction":true},{"event":"end","id":"read_inside","op":"read","result":{"snapshot":2,"rows":[{"file":"rewrite-p-0","pos":0,"value":{"reading":7}},{"file":"rewrite-p-0","pos":1,"value":{"reading":9}},{"file":"rewrite-p-1","pos":0,"value":{"reading":7}},{"file":"rewrite-p-1","pos":1,"value":{"reading":11}}]},"head":2,"objects":["a","b","rewrite-p-0","rewrite-p-1"],"in_transaction":true},{"event":"sql","value":"ROLLBACK","in_transaction":true},{"event":"end","id":"tx","op":"rollback","result":"ROLLED_BACK","head":1,"objects":["a","b","rewrite-p-0","rewrite-p-1"],"in_transaction":false},{"event":"begin","id":"after_rollback","op":"collect"},{"event":"end","id":"after_rollback","op":"collect","result":{"request_only":{"id":"after_rollback","op":"collect"}},"head":1,"objects":["a","b","rewrite-p-0","rewrite-p-1"],"in_transaction":false},{"event":"begin","id":"publish","op":"publish"},{"event":"sql","value":"BEGIN IMMEDIATE","in_transaction":false},{"event":"after_state","value":1,"in_transaction":true},{"event":"after_commit","value":2,"in_transaction":true},{"event":"sql","value":"COMMIT","in_transaction":true},{"event":"end","id":"publish","op":"publish","result":2,"head":2,"objects":["a","b","rewrite-p-0","rewrite-p-1"],"in_transaction":false},{"event":"begin","id":"expire","op":"expire"},{"event":"end","id":"expire","op":"expire","result":{"request_only":{"id":"expire","op":"expire","snapshot":1}},"head":2,"objects":["a","b","rewrite-p-0","rewrite-p-1"],"in_transaction":false},{"event":"begin","id":"after_commit","op":"collect"},{"event":"end","id":"after_commit","op":"collect","result":{"request_only":{"id":"after_commit","op":"collect"}},"head":2,"objects":["a","b","rewrite-p-0","rewrite-p-1"],"in_transaction":false},{"event":"begin","id":"current","op":"read"},{"event":"after_state","value":2,"in_transaction":false},{"event":"before_get","value":"rewrite-p-0","in_transaction":false},{"event":"after_get","value":"rewrite-p-0","in_transaction":false},{"event":"before_get","value":"rewrite-p-1","in_transaction":false},{"event":"after_get","value":"rewrite-p-1","in_transaction":false},{"event":"end","id":"current","op":"read","result":{"snapshot":2,"rows":[{"file":"rewrite-p-0","pos":0,"value":{"reading":7}},{"file":"rewrite-p-0","pos":1,"value":{"reading":9}},{"file":"rewrite-p-1","pos":0,"value":{"reading":7}},{"file":"rewrite-p-1","pos":1,"value":{"reading":11}}]},"head":2,"objects":["a","b","rewrite-p-0","rewrite-p-1"],"in_transaction":false}],"predicted":[{"id":"seed","outcome":"发布快照1：先登记名称 a/b（held=1），锁外写载荷，再提交目录并解除 held","protected":["a","b"],"deleted":[],"reason":"head=1 未到期，快照1文件集保护 a/b"},{"id":"p","outcome":"返回计划 {base:1, inputs:[a,b], outputs:[rewrite-p-0,rewrite-p-1]}","protected":["a","b","rewrite-p-0","rewrite-p-1"],"deleted":[],"reason":"登记事务保护输入与输出名"},{"id":"publish_inside","outcome":"外层事务内发布快照2（未提交），计划移交与 held 解除均为未提交态","protected":["a","b","rewrite-p-0","rewrite-p-1"],"deleted":[],"reason":"回收者只使用已提交视图：快照1未到期、计划活动、输出 held，四个对象全保护"},{"id":"expire_inside","outcome":"事务内登记快照1到期（未提交）","protected":["a","b","rewrite-p-0","rewrite-p-1"],"deleted":[],"reason":"已提交视图中快照1仍未到期，保护集不变"},{"id":"collect_inside","outcome":"返回 []：检测到 in_transaction，整轮暂缓","protected":["a","b","rewrite-p-0","rewrite-p-1"],"deleted":[],"reason":"未提交的到期与移交不得驱动删除；暂缓不缓存候选，下一轮重算不会遗忘"},{"id":"read_inside","outcome":"事务内读 head=2，返回 [7,9,7,11]","protected":["a","b","rewrite-p-0","rewrite-p-1"],"deleted":[],"reason":"pin 的插入与释放都并入外层事务，回滚同时撤销两者，不泄漏保护"},{"id":"tx","outcome":"ROLLBACK：head 恢复1，快照2、到期、移交全部撤销","protected":["a","b","rewrite-p-0","rewrite-p-1"],"deleted":[],"reason":"目录回滚自动恢复计划 active 与输出 held；原计划可再发布，四个对象全部保留"},{"id":"after_rollback","outcome":"返回 []：本轮无实际删除","protected":["a","b","rewrite-p-0","rewrite-p-1"],"deleted":[],"reason":"已提交视图：快照1未到期、计划活动、输出 held，候选为空；回滚前的临时可达性未被当成最终状态"},{"id":"publish","outcome":"重新发布成功，快照2提交，计划保护原子移交","protected":["a","b","rewrite-p-0","rewrite-p-1"],"deleted":[],"reason":"快照1与快照2共同覆盖四个对象"},{"id":"expire","outcome":"快照1登记到期","protected":["rewrite-p-0","rewrite-p-1"],"deleted":[],"reason":"仅 head=2 的快照2文件集仍受保护"},{"id":"after_commit","outcome":"返回 [a, b]：实际删除 a、b","protected":["rewrite-p-0","rewrite-p-1"],"deleted":["a","b"],"reason":"快照1已到期、计划已移交、无 pin，a/b 不可达，本轮实际回收"},{"id":"current","outcome":"读 head=2 返回 [7,9,7,11]","protected":["rewrite-p-0","rewrite-p-1"],"deleted":[],"reason":"head 输出对象完好，a/b 已回收不影响当前读"}]},"cancel_name":{"actual":[{"event":"begin","id":"seed","op":"seed"},{"event":"before_put","value":"a","in_transaction":false},{"event":"after_put","value":"a","in_transaction":false},{"event":"before_put","value":"b","in_transaction":false},{"event":"after_put","value":"b","in_transaction":false},{"event":"sql","value":"BEGIN IMMEDIATE","in_transaction":false},{"event":"after_commit","value":1,"in_transaction":true},{"event":"sql","value":"COMMIT","in_transaction":true},{"event":"end","id":"seed","op":"seed","result":1,"head":1,"objects":["a","b"],"in_transaction":false},{"event":"begin","id":"p","op":"prepare"},{"event":"after_state","value":1,"in_transaction":false},{"event":"before_get","value":"a","in_transaction":false},{"event":"after_get","value":"a","in_transaction":false},{"event":"before_get","value":"b","in_transaction":false},{"event":"after_get","value":"b","in_transaction":false},{"event":"before_put","value":"rewrite-c-0","in_transaction":false},{"event":"after_put","value":"rewrite-c-0","in_transaction":false},{"event":"before_put","value":"rewrite-c-1","in_transaction":false},{"event":"after_put","value":"rewrite-c-1","in_transaction":false},{"event":"end","id":"p","op":"prepare","result":{"base":1,"inputs":["a","b"],"outputs":["rewrite-c-0","rewrite-c-1"]},"head":1,"objects":["a","b","rewrite-c-0","rewrite-c-1"],"in_transaction":false},{"event":"begin","id":"cancel","op":"cancel"},{"event":"end","id":"cancel","op":"cancel","result":{"request_only":{"id":"cancel","op":"cancel","job":"c"}},"head":1,"objects":["a","b","rewrite-c-0","rewrite-c-1"],"in_transaction":false},{"event":"begin","id":"collect","op":"collect"},{"event":"end","id":"collect","op":"collect","result":{"request_only":{"id":"collect","op":"collect"}},"head":1,"objects":["a","b","rewrite-c-0","rewrite-c-1"],"in_transaction":false},{"event":"begin","id":"raw_remove","op":"raw_drop"},{"event":"drop","value":"rewrite-c-0","in_transaction":false},{"event":"end","id":"raw_remove","op":"raw_drop","result":null,"head":1,"objects":["a","b","rewrite-c-1"],"in_transaction":false},{"event":"begin","id":"raw_reuse","op":"raw_put"},{"event":"before_put","value":"rewrite-c-0","in_transaction":false},{"event":"after_put","value":"rewrite-c-0","in_transaction":false},{"event":"end","id":"raw_reuse","op":"raw_put","result":null,"head":1,"objects":["a","b","rewrite-c-0","rewrite-c-1"],"in_transaction":false}],"predicted":[{"id":"seed","outcome":"发布快照1：先登记名称 a/b（held=1），锁外写载荷，再提交目录并解除 held","protected":["a","b"],"deleted":[],"reason":"head=1 未到期，快照1文件集保护 a/b"},{"id":"p","outcome":"返回计划 {base:1, inputs:[a,b], outputs:[rewrite-c-0,rewrite-c-1]}","protected":["a","b","rewrite-c-0","rewrite-c-1"],"deleted":[],"reason":"登记事务保护输入与输出名"},{"id":"cancel","outcome":"取消计划 c：状态置 cancelled，输出名 held 解除","protected":["a","b"],"deleted":[],"reason":"取消只释放计划自身保护；a/b 由未到期快照1保护"},{"id":"collect","outcome":"返回 [rewrite-c-0, rewrite-c-1]：实际删除两个输出","protected":["a","b"],"deleted":["rewrite-c-0","rewrite-c-1"],"reason":"计划已取消，两个输出不可达；名称永久留在 names 登记表，删除不等于可重用"},{"id":"raw_remove","outcome":"诊断探针（非协议操作）：对象已被 collect 合法回收，raw_drop 找不到对象","protected":["a","b"],"deleted":[],"reason":"raw_drop（gc_observe.py:21-27）不检查保护，不是合法回收协议操作；协议删除已在 collect 步完成，此处 deleted 为空"},{"id":"raw_reuse","outcome":"诊断探针（非协议操作）：raw 存储在对象缺失时接受同名异值写入，展示存储层限制","protected":["a","b"],"deleted":[],"reason":"协议的 names 永久登记在业务写路径拒绝已退役名（seed/prepare 登记事务报 NAME_RETIRED）；探针绕过登记，正说明不能照抄 raw 探针作为业务端口"}]},"shared_empty":{"actual":[{"event":"begin","id":"seed","op":"seed"},{"event":"before_put","value":"a","in_transaction":false},{"event":"after_put","value":"a","in_transaction":false},{"event":"before_put","value":"b","in_transaction":false},{"event":"after_put","value":"b","in_transaction":false},{"event":"before_put","value":"empty","in_transaction":false},{"event":"after_put","value":"empty","in_transaction":false},{"event":"sql","value":"BEGIN IMMEDIATE","in_transaction":false},{"event":"after_commit","value":1,"in_transaction":true},{"event":"sql","value":"COMMIT","in_transaction":true},{"event":"end","id":"seed","op":"seed","result":1,"head":1,"objects":["a","b","empty"],"in_transaction":false},{"event":"begin","id":"delete","op":"delete"},{"event":"after_state","value":1,"in_transaction":false},{"event":"before_get","value":"a","in_transaction":false},{"event":"after_get","value":"a","in_transaction":false},{"event":"sql","value":"BEGIN IMMEDIATE","in_transaction":false},{"event":"after_state","value":1,"in_transaction":true},{"event":"after_commit","value":2,"in_transaction":true},{"event":"sql","value":"COMMIT","in_transaction":true},{"event":"end","id":"delete","op":"delete","result":2,"head":2,"objects":["a","b","empty"],"in_transaction":false},{"event":"begin","id":"prepare","op":"prepare"},{"event":"after_state","value":2,"in_transaction":false},{"event":"before_get","value":"empty","in_transaction":false},{"event":"after_get","value":"empty","in_transaction":false},{"event":"end","id":"prepare","op":"prepare","result":{"base":2,"inputs":["empty"],"outputs":[]},"head":2,"objects":["a","b","empty"],"in_transaction":false},{"event":"begin","id":"publish","op":"publish"},{"event":"sql","value":"BEGIN IMMEDIATE","in_transaction":false},{"event":"after_state","value":2,"in_transaction":true},{"event":"after_commit","value":3,"in_transaction":true},{"event":"sql","value":"COMMIT","in_transaction":true},{"event":"end","id":"publish","op":"publish","result":3,"head":3,"objects":["a","b","empty"],"in_transaction":false},{"event":"begin","id":"expire_old","op":"expire"},{"event":"end","id":"expire_old","op":"expire","result":{"request_only":{"id":"expire_old","op":"expire","snapshot":1}},"head":3,"objects":["a","b","empty"],"in_transaction":false},{"event":"begin","id":"shared","op":"collect"},{"event":"end","id":"shared","op":"collect","result":{"request_only":{"id":"shared","op":"collect"}},"head":3,"objects":["a","b","empty"],"in_transaction":false},{"event":"begin","id":"expire_middle","op":"expire"},{"event":"end","id":"expire_middle","op":"expire","result":{"request_only":{"id":"expire_middle","op":"expire","snapshot":2}},"head":3,"objects":["a","b","empty"],"in_transaction":false},{"event":"begin","id":"empty","op":"collect"},{"event":"end","id":"empty","op":"collect","result":{"request_only":{"id":"empty","op":"collect"}},"head":3,"objects":["a","b","empty"],"in_transaction":false},{"event":"begin","id":"current","op":"read"},{"event":"after_state","value":3,"in_transaction":false},{"event":"before_get","value":"a","in_transaction":false},{"event":"after_get","value":"a","in_transaction":false},{"event":"before_get","value":"b","in_transaction":false},{"event":"after_get","value":"b","in_transaction":false},{"event":"end","id":"current","op":"read","result":{"snapshot":3,"rows":[{"file":"a","pos":1,"value":{"reading":7}},{"file":"b","pos":0,"value":{"reading":9}},{"file":"b","pos":1,"value":{"reading":11}}]},"head":3,"objects":["a","b","empty"],"in_transaction":false}],"predicted":[{"id":"seed","outcome":"发布快照1（含空文件 empty）：名称登记后写载荷再提交目录","protected":["a","b","empty"],"deleted":[],"reason":"零行内容仍有规范字节对象（objects.py:16），名称登记与快照保护同样适用"},{"id":"delete","outcome":"取得 pin(1) 后完成验证并提交快照2（墓碑 (a,0)）","protected":["a","b","empty"],"deleted":[],"reason":"快照1/2 共同保护 a/b/empty"},{"id":"prepare","outcome":"返回计划 {base:2, inputs:[empty], outputs:[]}：空 layout 合法","protected":["a","b","empty"],"deleted":[],"reason":"计划输入 empty 受活动计划保护；无输出名需登记"},{"id":"publish","outcome":"发布快照3：文件集变为 {a,b}，empty 离开 head","protected":["a","b","empty"],"deleted":[],"reason":"empty 仍被未到期快照1/2引用；移交只解除计划保护（本计划无输出）"},{"id":"expire_old","outcome":"快照1登记到期","protected":["a","b","empty"],"deleted":[],"reason":"快照2未到期并引用 empty，empty 仍受保护"},{"id":"shared","outcome":"返回 []：本轮无实际删除","protected":["a","b","empty"],"deleted":[],"reason":"empty 被快照2共享，仅到期快照1不足以回收"},{"id":"expire_middle","outcome":"快照2登记到期","protected":["a","b"],"deleted":[],"reason":"仅剩 head=3 保护 a/b；empty 不再可达"},{"id":"empty","outcome":"返回 [empty]：实际删除 empty","protected":["a","b"],"deleted":["empty"],"reason":"快照1/2均已到期、无 pin/计划引用，empty 不可达；对象存在性按名称清单判定，与零字节内容无关"},{"id":"current","outcome":"读 head=3 返回 [7,9,11]","protected":["a","b"],"deleted":[],"reason":"head 文件集完好，empty 回收不影响当前读"}]}}
~~~
