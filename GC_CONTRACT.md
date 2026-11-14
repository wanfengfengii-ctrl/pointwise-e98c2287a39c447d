# 历史快照到期与对象回收：讨论材料

## 已提供的工程
Python 标准库与 SQLite。lake.py、catalog.py、objects.py 是原始业务源码，保持不变；checks.py 保留原业务断言，replay.py 与 samples.py 保留原时序，examples/move.json 可直接回放。这里只讨论载荷生命周期，不沿用题源的插值分析目标。
入口为 Lake(Catalog(), Objects())；默认内存存储。Objects(root) 可使用目录，Catalog(path) 可使用数据库文件，调用方创建父目录。快照目录和对象不属于同一事务。
从根目录执行：
- python3 checks.py：原业务回归。
- python3 replay.py --input examples/move.json：原位置删除、重排及历史读取。
- python3 gc_check.py baseline：原回归与本题观察器的业务核对。
- python3 gc_observe.py all：输出全部真实观察；all 可换为单个样例名。
运行清单 .pointwise-runtime.json 初始核验基线并演示观察；交付时只把 verify 改为 ["python3","gc_check.py","verify"]，demo 保持观察入口。setup 是 argv 数组列表，verify、demo 是直接执行的 argv；version=1、kind=cli。不使用 shell 包装。

## 原有接口、格式与兼容边界
Lake.seed(files)：files 为文件名到 JSON 行数组的映射；空库发布快照1，快照0为空。文件名仅字母、数字、减号和下划线。相同名只能写相同规范内容；业务调用者始终使用新 job 和新对象名，不篡改 plan。回收后也不得把旧名分配给新对象，不要求重建已回收对象或重跑旧 job。
Lake.read(snapshot=None)：固定指定快照或一次 head，返回 {"snapshot":整数,"rows":[{"file":文件名,"pos":零起行号,"value":原行}]}，按文件名及行号排序，过滤该快照的位置墓碑。
Lake.delete(snapshot,fid,pos)：先验证源快照，再验证文件和位置，再检查当前布局；NO_SNAPSHOT、BAD_POSITION、STALE_LAYOUT 的顺序、重复删除不增快照及返回值沿原代码。
Lake.prepare(job,inputs,layout)：固定当前 head；inputs 互异、非空且包含于当前文件集。layout 是输出组数组，每组为 [输入文件名,原行号] 数组，须恰好包含所有输入物理位置一次，包括墓碑。输出写到 rewrite-JOB-组号，过滤基准墓碑。空输入文件、空输出组、layout=[] 在覆盖关系成立时合法。返回 {"base":整数,"inputs":[文件名],"outputs":[文件名]}。
Lake.publish(plan)：只在 head 等于 base 且输入有效时发布下一快照，移除输入及其墓碑，加入输出；否则保持 PUBLISH_CONFLICT/BAD_PLAN 语义。失败发布不自动取消计划，调用者可显式 cancel。
Lake.rewrite 封装 prepare/publish；正常串行业务结果保留。提议可缩短其原有锁范围，以满足载荷 I/O 不持目录写锁的要求，不额外要求原封装在交错期间必定成功。
Catalog.write 嵌套调用加入外层事务；最外层失败回滚目录，Objects 的写入不回滚。快照编号规则沿源码。讨论中的显式外层事务在 prepare 返回后组合 publish、expire、cancel 及读取；不要求把一个新 prepare 放进调用者尚未提交的外层事务。
位置删除也会读载荷，不能只保护 Lake.read。已经取得合法源快照的在途 delete 可以完成原有验证及当前布局判断；expire 后新发起的 delete 拒绝该源快照。prepare 的输入读取、部分输出和已返回但未结束的计划均属于活动重写。

## 要设计的接口
这些是方案端口，本轮不实现：
- expire(snapshot) -> None。所有已提交快照默认保留，直到显式到期；head 报 HEAD_SNAPSHOT，未知快照报 NO_SNAPSHOT，重复到期无副作用。到期不可撤销，但外层事务回滚须恢复到期前状态。
- 到期后的新 read(snapshot) 或 delete(snapshot,...) 报 EXPIRED_SNAPSHOT；已在到期前取得保护的操作可完成。取得快照与到期须有明确排序，不得以载荷已经打开作为隐含前提。
- cancel(job) -> None。仅对已返回的 prepare 计划调用；未知 job 报 NO_JOB，重复取消或已发布 job 无副作用。取消使未发布计划不可再发布，后续 publish 报 CANCELLED_JOB；目录事务内的取消随外层回滚恢复。
- collect() -> 按文件名排序的本轮实际删除对象列表。只有一个回收者，可多次调用。允许在活动事务期间暂缓删除，不能因为一次暂缓就永远遗忘候选。
当前 head、所有未到期快照、已取得保护的操作、活动重写所需对象不得误删；多个保护可以共享同一对象。发布必须把计划保护安全交给快照保护，外层提交前不能当成已经提交，回滚后仍须保留原计划所需对象。取消或失败也不能释放仍被其他持有者使用的对象。
单进程内按暂停点交错操作，一个 SQLite 目录连接；不要求线程调度实现、跨进程租约、崩溃恢复或多个回收者。对象读写与删除成功，不注入存储故障。实际载荷 I/O 期间不得持目录写锁；可说明另外需要的短临界区。
操作停止、事务结束且所有相应保护解除后，继续调用 collect 必须最终实际删除所有无引用对象；不能永久保留全部历史或把输出丢失后重新计算当成保护。
不要求增量目录优化，允许枚举元数据和对象名，但不得读取所有行载荷来判断可达性。分别核算目录扫描、对象枚举、载荷读写及永久名称保留的空间成本。不做订阅、迁移或分析索引。

## 可重跑的非答案观察
 gc_observe.run(spec) 接收操作数组。每项含唯一 id、op 及接口参数；prepare/publish 使用 job，read/expire 使用 snapshot，delete 使用 snapshot/file/pos，seed 使用 files。rollback.actions 在真实外层 Catalog.write 中执行，再主动回滚。每项可附 at={"point":点名,"value":匹配值,"actions":[嵌套操作]}，在第一次匹配处运行嵌套操作。point 有 after_state（值为快照号）、before_get/after_get、before_put/after_put（值为文件名）、after_commit（值为未必已提交的快照号）。只允许有限、无循环交错。
python3 gc_observe.py --input PACK.json 读取 {"样例名":操作数组}，每行输出 {"case":名字,"actual":事件数组}。
事件包含 begin/end、I/O 暂停点与实际 BEGIN IMMEDIATE/COMMIT/ROLLBACK。end 含 id、op、result 或 error、head、objects 和 in_transaction。objects 是当时真实载荷清单；trace 不表示数据库页访问数。
expire/cancel/collect 仅返回 request_only 并记录请求，不改变基线。基线 late 读取仍成功并非方案预期。
raw_drop/raw_put 是原始存储诊断，不是 collect 的实现，不检查保护。ProbeObjects.inventory/drop_raw 适配内存及文件存储；内存删除会移除规范字节，文件删除调用 unlink。原存储依赖对象仍存在来核对不可变性，raw 删除后同名异值可重新写入。方案须说明如何在回收后继续遵守永久名称约定，不能照抄 raw 探针作为业务端口。

样例 gc_samples.cases()：
- reader：读完 a、读 b 前暂停，完成重写发布、请求到期快照1及回收，然后继续原读取。原读取仍应返回 [7,7,9,11]；新请求快照1读取应拒绝。读取结束后旧 a/b 不能永久占空间。
- prepared：首个输出写完时回收；随后删除导致发布冲突。冲突前后输出不能因暂未入快照就删除；显式取消后输出应最终可回收，a/b 仍为 head 所用。
- rollback：事务内发布、到期、回收及读取，再回滚。事务内读为快照2，回滚 head 恢复1，原计划可再发布；不能把回滚前的临时可达性当成最终状态。
- cancel_name：取消后回收请求，以及绕过目录的原始删除和同名异值写入。后两步只展示现有存储的限制，预测中须明确这不是合法回收协议操作。
- shared_empty：快照1和2共享 a/b/empty，快照3移除空文件；仅到期快照1不能回收 empty，快照2也到期后 empty 应最终删除。零字节行内容不等于对象不存在；当前读取值为 [7,9,11]。

## 文档交付和核验
交付 DESIGN.md，给出 lake.py、catalog.py、objects.py、gc_observe.py 的有效 文件名:行号 定位，串起状态取得、载荷访问、准备、发布、到期与删除调用链。比较两种可行保护策略，选择一种，说明状态、短临界区、外层事务接入及关键伪代码，解释安全性和最终可回收条件，无需完整形式证明。
用 <!-- gc-evidence --> 后的一个 ~~~json 代码块嵌入证据，顶层为五个公开样例名。每项 actual 为 run(cases()[name]) 的完整事件数组；predicted 为按真实 end 事件顺序排列的逐操作预测数组，每项有 id、outcome（方案下结果或错误的文字说明）、protected（此时方案仍保护的对象名列表）、deleted（本步骤实际回收的对象名列表）、reason（保护或删除依据）。raw 诊断单列解释，不把它算成协议的合法删除。预测可以与基线结果不同，尤其到期读取与回收。
python3 gc_check.py verify 重放基线证据，检查预测覆盖、声明的删除与保护不相交及源码行号；不会替你验证预测集合、事务伪代码或交接窗口。人工审查须逐步核对原调用和方案预测，并检查读到期排序、部分输出保护、提交/回滚移交、共享对象及静止后的实际回收，不能以结构检查通过冒充协议正确。
保留所提供的业务源码、原断言、样例和观察器，只交讨论文档及运行清单 verify 的调整，不开发回收器。
