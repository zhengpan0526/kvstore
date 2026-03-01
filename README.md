# KVStore

一个类 Redis 的 C 语言 KV 引擎。覆盖 RESP 协议解析、存储引擎、AOF/RDB 持久化、TCP/eBPF 主从同步与压测对齐。

## 这个项目体现的工程能力
- 端到端工程闭环：协议解析 → 命令分发 → 存储执行 → 持久化/复制。
- 存储抽象能力：统一命令框架下实现 `array/rbtree/hash/skiptable`。
- 一致性边界设计：写命令成功后再进入 AOF 与复制。
- 复制工程能力：`sync_mode=tcp|ebpf` 双后端，覆盖全量+增量同步。
- 健壮性处理：RESP 半包继续收包、非法包快速失败。
- 并发隔离能力：线程局部复制上下文 + slave 只读保护。
- 性能方法论：同口径对齐 Redis + 自定义负载与内存后端对比。

## 架构概览
主链路：`Client -> RESP Parser -> Dispatch -> Storage -> Reply`。  
写命令成功后进入 `AOF append/replay`，再进入复制通道。  
复制层支持 `TCP` 与 `eBPF` 两种同步后端。  
存储层为 `array/rbtree/hash/skiptable` 四种实现。  
内存后端支持 `pool/system/jemalloc` 切换。  
详细架构图与时序见 `README.dev.md` / `docs/`。

## 设计与工程亮点
- 一致性边界：仅在写执行成功后触发 AOF 与同步，避免无效写传播。
- 复制上下文隔离：线程局部上下文区分外部请求与复制回放，保证 slave 只读边界。
- RESP 半包处理：`0` 表示数据不足继续收包，`-1` 表示协议错误快速失败。
- 复制并发保护：slave 连接由互斥锁与 `max_slaves` 控制，防止连接风暴。
- AOF/rewrite：覆盖加载重放与重写路径，平衡恢复正确性和运行期开销。
- eBPF 批处理：聚合同步消息后发送，降低 syscall 并提升同步吞吐。

## 性能说明
- 对齐 Redis 的压测显示：`rbtree/hash/skiptable` 已进入高吞吐区间，差距明显收敛。
- 当前主要瓶颈在 `array` 路径。
- TCP 同步吞吐损耗较小；eBPF 链路在启用批处理后较无批处理有显著提升。
- `pool/system/jemalloc` 在吞吐与内存占用上存在可观测差异。
- 完整压测方法与原始数据见 `README.dev.md`。

## 仓库使用说明
- 本仓库定位为“工程核心代码节选 + 设计文档”。
- 重点体现架构取舍、关键链路和性能方法。
- 面试可现场演示并讲解核心链路（协议解析、写路径、持久化与主从同步）。

## 更多细节
详见 `README.dev.md` / `docs/`。
