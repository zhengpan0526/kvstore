# KVStore
一个类 Redis 的 C 语言 KV 引擎，覆盖协议解析、命令执行、存储引擎、持久化、复制与压测链路。

## 项目展示
- 协议与会话：RESP 状态机 + 半包处理 +单连接批量命令处理（`src/network/resp_protocol.c`, `src/network/net_session.c`）。
- 低锁并发组件设计：基于原子变量的环形消息队列（SPSC push + CAS pop），用于复制后端解耦（`src/utils/msg_queue.c`, `src/distributed/sync_backend_tcp.c`）。
- 网络缓冲区抽象：自定义 `netbuf` 统一收发缓冲、扩容、前裁剪，承接 Pipeline 与粘拆包（`src/utils/netbuf.c`, `src/network/net_session.c`）。
- 可切换内存后端：`pool/system/jemalloc` 三后端统一接口，便于性能与内存占用对比（`src/storage/kvs_mempool.c`, `src/storage/jemalloc_wrapper.c`）。
- 写路径一致性边界：写命令执行成功后进入 AOF，再进入主从增量同步，且加载/回放场景做防重复处理（`src/core/kvstore.c`, `src/persistence/kvs_aof.c`, `src/distributed/distributed.c`）。
- 持久化工程：AOF append/load + 后台 rewrite，配合 RDB snapshot/load 提供恢复路径（`src/persistence/kvs_aof.c`, `src/persistence/kvs_aof_rewrite.c`, `src/persistence/kvs_rdb.c`）。
- 复制链路工程：TCP 全量+增量同步，另有 eBPF 抓包转发链路（可选）与批处理发送（`src/distributed/*.c`, `src/eBPF/ebpf_syncd.c`）。
- 性能评估闭环：统一脚本对比 KVStore/Redis，覆盖 set/get、pipeline 与结果汇总（`scripts/bench_all.sh`, `scripts/kvstore_resp_bench.py`, `results/summary.md`）。

## 架构总览
Client 请求进入网络层（Reactor/Proactor/NtyCo），先进入 RESP 解析器做协议对齐与命令切分。  
解析后进入命令分发与参数校验，再路由到 array/rbtree/hash/skiptable 等存储实现。  
写命令成功后按边界进入 AOF 追加，并由主节点编码为复制消息广播到从节点。  
从节点按复制协议回放写操作，并在只读策略下隔离外部写请求。  
读写响应统一走会话发送缓冲，支持 pipeline 场景下的批量应答。


## 设计取舍
- RESP 半包与错误快速失败：`resp_peek_command` 与 `resp_parser` 区分“不完整包(0)”和“协议错误(-1)”，避免状态机污染（`src/network/resp_protocol.c`）。
- 写后 AOF + 复制边界：写命令成功后追加 AOF，再按上下文决定是否复制，减少回放重复与环路同步（`src/core/kvstore.c`）。
- slave 只读与回放隔离：用 `__thread sync_context` 区分普通请求/复制上下文，既保证只读策略又允许回放写入（`src/core/kvstore.c`, `src/network/reactor.c`, `src/network/proactor.c`）。
- AOF rewrite 细节：后台重写线程、rewrite buffer、快照保护与落盘路径分离（`src/persistence/kvs_aof_rewrite.c`）。
- RANGE/SORT 跨结构统一语义：命令层统一参数校验和分发，底层按结构实现 range/sort，便于讨论复杂度与一致性（`src/core/kvs_operation.c`）。
- 队列+批处理发送：复制消息先入队再异步发送；eBPF 路径叠加 `tx_batcher` 降低小包发送开销（`src/utils/msg_queue.c`, `src/utils/tx_batcher.c`, `src/eBPF/ebpf_syncd.c`）。

## 性能说明（简要）
- 在当前样例结果中，hash/rbtree/skiptable 的 set/get 吞吐明显高于 array 路径，瓶颈分布具有结构差异。  
- 压测脚本保证 kvstore 与 Redis 的请求参数口径一致，便于横向比较。  
- 详细过程、参数与结果解释见 `README.dev.md` 的性能章节与 `results/summary.md`。

## 面试阅读指南
- 这是“核心代码节选 + 实验文档”仓库，目标是展示系统工程能力，不承诺开箱即用的完整生产部署环境。
- 建议面试官按以下顺序阅读：
  - `src/core/kvstore.c`（写路径边界、复制与持久化挂接点）
  - `src/network/net_session.c` + `src/network/resp_protocol.c`（协议解析/半包/pipeline）
  - `src/persistence/kvs_aof.c` + `src/persistence/kvs_aof_rewrite.c`（AOF 主链路与重写）
  - `src/distributed/distributed.c` + `src/distributed/sync_backend_tcp.c`（主从同步）
  - `src/storage/kvs_mempool.c` + `src/utils/netbuf.c`（基础组件能力）

## 更多资料
- 详细设计与长文档：`README.dev.md`
