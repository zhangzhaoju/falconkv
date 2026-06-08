# FalconKV - 分布式 KV 存储系统全局设计文档

## 1. 系统概述

FalconKV 是一款面向 LLM 推理加速场景（LMCache/Mooncake）的低时延、高吞吐分布式 KV 存储系统。系统以 SSD 为持久化存储介质，通过元数据分离架构实现 KV Cache 块的高效存取，核心设计目标是：

- **低时延**：BatchExist/Put/Get 全链路毫秒级响应
- **高吞吐**：批量操作支持数千 KV 块/秒的并发读写
- **数据亲和**：写入绑定本地 Store，读取支持三级亲和路径
- **弹性伸缩**：支持多 Store 节点横向扩展

### 1.1 设计参考

本设计参考了以下项目的架构经验：

- **FalconFS**：分布式文件系统架构（元数据与数据分离、PostgreSQL 元数据引擎、brpc 通信、DirectIO 存储引擎）
- **LMCache V0.3.12**：LLM KV Cache 管理框架（RemoteConnector 插件体系、MemoryObj 内存管理、CacheEngineKey 键格式）

### 1.2 核心约束

| 约束项 | 说明 |
|--------|------|
| 语言 | C++17 主体，Python 绑定层 |
| 元数据存储 | 纯内存（分片哈希表 + 分片读写锁） |
| 通信协议 | brpc（可扩展） |
| 存储介质 | SSD（DirectIO） |
| 数据块大小 | 固定大小（由 LMCache chunk_size 决定） |
| 上游框架 | LMCache RemoteConnector / Mooncake Store |

## 2. 系统架构

### 2.1 整体架构图

```
┌─────────────────────────────────────────────────────────────┐
│                    LLM Inference Engine                      │
│                     (vLLM / 其他引擎)                        │
└───────────────────────────┬─────────────────────────────────┘
                            │ KV Cache Tensor
                            ▼
┌─────────────────────────────────────────────────────────────┐
│                        LMCache / Mooncake                    │
│                  (KV Cache 管理框架)                          │
└───────────────────────────┬─────────────────────────────────┘
                            │ RemoteConnector API
                            ▼
┌─────────────────────────────────────────────────────────────┐
│                     FalconKV Client                          │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────────┐   │
│  │ Python 绑定  │  │ Key 描述缓存 │  │ NodeLocalAccessor│   │
│  │ (GIL-Free)   │  │ (LRU Cache)  │  │ (同节点直读)     │   │
│  └──────┬───────┘  └──────┬───────┘  └───────┬──────────┘   │
│         │                 │                   │              │
│         └────────────┬────┴───────────────────┘              │
│                      │ Batch RPC                              │
│         ┌────────────┘                                       │
│         │ SchedulerProxy (可选，bypass 机制)                 │
│         │                                                    │
│         │ local_store_ (绑定的本地 FalconKVStore)            │
│         │  - BatchPut: 写入始终走本地 Store                  │
│         │  - BatchGet: Level 0 读取                          │
│         │  - BatchContains: 本地存在性查询                   │
└─────────┼────────────────────────────────────────────────────┘
          │
          │    ┌──────────────────────────────────────────────┐
          │    │ IO Scheduler (节点级，独立进程)               │
          │    │  - IO 调度 (Passthrough → 限流/排队)         │
          │    │  - 带宽/时延统计                             │
          │    │  - 峰值并发检测                              │
          │    └──────────────────────────────────────────────┘
          │                ↕ UDS (可选)
          │
          ├────────────┬────────────┐
          ▼            ▼            ▼
┌──────────────────┐ ┌──────────┐ ┌──────────────┐
│  FalconKV Store  │ │          │ │  FalconKV Store│
│  ┌─────────────┐ │ │          │ │  ┌────────────┐│
│  │SlotAlloc   │ │ │          │ │  │SlotAlloc  ││
│  │MetaIndex    │ │ │          │ │  │MetaIndex   ││
│  │MetaSyncCli  │ │ │          │ │  │MetaSyncCli ││
│  │EvictManager │ │ │          │ │  │EvictManager││
│  └─────────────┘ │ │          │ │  └────────────┘│
│  SSD+DirectIO    │ │          │ │  SSD+DirectIO  │
│  (同进程/同节点)  │ │          │ │  (远程节点)    │
└────────┬─────────┘ │          │ └────────────────┘
         │           │          │
         │  SyncCommit/SyncRemove (异步)
         ▼           ▼          ▼
┌─────────────────────────────────────────────┐
│              Meta 模块 (纯内存)               │
│  ┌─────────┐ ┌─────────┐ ┌─────────┐       │
│  │Shard 0  │ │Shard 1  │ │Shard N  │       │
│  │[rwlock] │ │[rwlock] │ │[rwlock] │       │
│  │hashmap  │ │hashmap  │ │hashmap  │       │
│  └─────────┘ └─────────┘ └─────────┘       │
│  ┌─────────────────────────────────────┐    │
│  │ Store 注册表 (独立读写锁保护)        │    │
│  └─────────────────────────────────────┘    │
└─────────────────────────────────────────────┘
```

### 2.2 模块职责划分

| 模块 | 职责 | 部署位置 | 通信方式 |
|------|------|----------|----------|
| **Client** | 对接 LMCache/Mooncake，提供 BatchGet/Put/Exist 接口；写入绑定本地 Store，读取三级亲和 | 推理节点（同进程） | C++ API / Python 绑定 |
| **Transfer** | 数据传输与元数据通信的抽象层 | 横跨 Client/Meta/Store | brpc |
| **Meta** | 元数据聚合/查询服务，接收 Store 推送的元数据，响应 Client 查询 | 独立进程（纯内存存储） | brpc RPC |
| **Store** | SSD 数据持久化、自管理空间（SlotAllocator）、本地元数据索引（StoreMetaIndex）、驱逐管理、元数据同步（MetaSyncClient） | 独立进程或同进程 | brpc RPC / in-process |
| **Scheduler** | 节点内 IO 调度、带宽/时延统计、峰值检测 | 节点内独立进程 | Unix Domain Socket |

### 2.3 部署模型

```
┌──────────────── Node 0 (推理节点, 8 GPU) ──────────────────┐
│                                                              │
│  ┌──────────┐  ┌──────────┐       ┌──────────┐  ┌──────────┐│
│  │vLLM进程0 │  │vLLM进程1 │  ...  │vLLM进程6 │  │vLLM进程7 ││
│  │+Client 0 │  │+Client 1 │       │+Client 6 │  │+Client 7 ││
│  │+Store 0  │  │+Store 1  │       │+Store 6  │  │+Store 7  ││
│  └─────┬────┘  └─────┬────┘       └─────┬────┘  └─────┬────┘│
│        │             │                  │             │      │
│        │    IO 申请/完成上报 (UDS)      │             │      │
│        └──────┬───────┴────────────────┴──────┬──────┘      │
│               │                                │             │
│               ▼                                │             │
│  ┌──────────────────────────┐                  │             │
│  │ IO Scheduler (独立进程)   │                  │             │
│  │  - IO 调度 (Passthrough) │                  │             │
│  │  - 带宽/时延统计         │                  │             │
│  │  - 峰值并发检测          │                  │             │
│  └──────────────────────────┘                  │             │
│               │                                │             │
│        ┌──────┴───────┬────────────────┬───────┘             │
│        ▼              ▼                ▼                     │
│  ┌───────────┐  ┌───────────┐   ┌───────────┐              │
│  │Store 0    │  │Store 1    │   │Store N    │              │
│  │(GPU 0 SSD)│  │(GPU 1 SSD)│   │(GPU N SSD)│              │
│  │SlotAlloc │  │SlotAlloc │   │SlotAlloc │              │
│  │MetaSync   │  │MetaSync   │   │MetaSync   │              │
│  └───────────┘  └───────────┘   └───────────┘              │
│        │              │                │                     │
│        └──────────────┴────────────────┘                     │
│               │ Store 远程 IO 上报 (UDS)                     │
│               │ Store → Meta SyncCommit/SyncRemove           │
│                                                              │
└──────────────────────────────────────────────────────────────┘

┌──────────────── Node 1 (存储节点) ────────────────┐
│                                                    │
│  ┌──────────────────────────────────────────────┐  │
│  │ FalconKV Store                               │  │
│  │ SlotAllocator + StoreMetaIndex + MetaSyncCli │  │
│  │ ┌──────────────────────────────────────────┐ │  │
│  │ │ SSD: /data/falconkv/kv_data_1            │ │  │
│  │ └──────────────────────────────────────────┘ │  │
│  └──────────────────────────────────────────────┘  │
│                                                    │
└────────────────────────────────────────────────────┘

┌──────────────── Meta Node ─────────────────────────┐
│                                                     │
│  ┌─────────────────────────────────────────────┐   │
│  │ FalconKV Meta (纯内存存储)                    │   │
│  │ ┌──────────┐ ┌──────────┐ ┌──────────┐      │   │
│  │ │ Shard 0  │ │ Shard 1  │ │ Shard N  │      │   │
│  │ │ [rwlock] │ │ [rwlock] │ │ [rwlock] │      │   │
│  │ └──────────┘ └──────────┘ └──────────┘      │   │
│  │ 接收 Store SyncCommit/SyncRemove             │   │
│  │ 响应 Client BatchExist/BatchLookup           │   │
│  └─────────────────────────────────────────────┘   │
│                                                     │
└─────────────────────────────────────────────────────┘
```

## 3. 核心数据流

### 3.1 BatchExist 流程（两步查询，不查缓存）

```
Client               local_store_              Meta
  │                       │                      │
  │── Step 1: BatchContains ──▶│                  │
  │   (keys)              │── 查询 StoreMetaIndex │
  │◀─ 命中的 key_descs ──│                      │
  │   (ACCESS_LOCAL_     │                      │
  │    DIRECT)           │                      │
  │   仍未命中 ↓         │                      │
  │                       │                      │
  │── Step 2: BatchExistRequest ─────────────────▶│
  │   (missing_keys)     │                      │── 查询分片内存表
  │                       │                      │── 匹配 stat=done
  │◀─ BatchExistResponse ────────────────────────│
  │   (hit_count,        │                      │
  │    key_descs[])      │                      │
  │                       │                      │
  │── 缓存 Meta 命中的 key_descs 到 KeyDescCache │
  │   （供后续 BatchGet 使用，BatchExist 本身不查缓存）│
```

> **为什么 BatchExist 不查 KeyDescCache**：Store 驱逐时先通过 SyncRemove 删除 Meta 元数据，但 Client 缓存中可能仍有旧描述。如果 BatchExist 信任缓存，会返回已驱逐的 key 为"命中"，导致后续读到不可预知的数据。因此 BatchExist 必须直接查询本地 Store 和 Meta。KeyDescCache 仅用于 BatchGet 加速。

### 3.2 BatchPut 流程（本地 Store 写入）

```
Client            local_store_                  Meta
  │                    │                          │
  │── BatchPut(keys,   │                          │
  │   data_ptrs, sizes)│                          │
  │                    │                          │
  │                    │── 1. SlotAllocator      │
  │                    │     .AllocChunk() →      │
  │                    │     offset               │
  │                    │                          │
  │                    │── 2. Write(offset, data) │
  │                    │     DirectIO → SSD       │
  │                    │                          │
  │                    │── 3. StoreMetaIndex      │
  │                    │     .Put(key, record)    │
  │                    │                          │
  │                    │── 4. MetaSyncClient      │
  │                    │     .SyncCommit() ─────────▶│
  │                    │     (异步 fire-and-forget)  │── upsert 分片内存表
  │◀─ results ────────│                          │
  │                    │                          │
  │── 更新 KeyDescCache│                          │
  │   (ACCESS_LOCAL_   │                          │
  │    DIRECT)         │                          │
```

### 3.3 BatchGet 流程（三级读取亲和）

```
Client               local_store_    NodeLocalAccessor    Store (远程)
  │                       │                │                  │
  │── 1. 查询 KeyDescCache (含 access_type)│                  │
  │    未命中 → BatchLookup RPC 到 Meta    │                  │
  │                       │                │                  │
  │── 2. 按 access_type 分组读取:          │                  │
  │                       │                │                  │
  │   [Level 0] local_store_->BatchRead(local_reads)          │
  │──────────────────────▶│                │                  │
  │                       │── 查 MetaIndex │                  │
  │                       │── DirectIO 读  │                  │
  │◀─ data ──────────────│                │                  │
  │                       │                │                  │
  │   [Level 1] node_accessor_.Read(store_id, offset, ...)   │
  │───────────────────────────────────────▶│                  │
  │                       │                │── FdCache + pread │
  │◀─ data ───────────────────────────────│                  │
  │                       │                │                  │
  │   [Level 2] StoreRpcClient::BatchRead(offsets, sizes, buffers)│
  │──────────────────────────────────────────────────────────▶│
  │                       │                │                  │── DirectIO 读
  │◀─ data (RPC response)────────────────────────────────────│
  │                       │                │                  │
  │  3. 三级路径按 access_type 分组批量执行│                  │
```

## 4. 关键设计决策

### 4.1 元数据与数据分离

参照 FalconFS 的架构经验，将元数据（纯内存分片哈希表）与数据（SSD）分离部署：

- **Meta 作为聚合服务**：Meta 不负责空间分配和驱逐，只接收 Store 推送的元数据（SyncCommit/SyncRemove）并响应查询（BatchExist/BatchLookup）
- **Store 自管理**：每个 Store 独立管理 SlotAllocator、StoreMetaIndex、驱逐策略，通过 MetaSyncClient 异步同步到 Meta
- **优势**：减少 Meta→Store 的交互延迟，Store 写入路径无需等待 Meta 响应

### 4.2 固定大小块管理

KVCache 场景下数据块大小固定（由 LMCache 的 `chunk_size` 和 `kv_shape` 决定），参照 Linux 内核 Buddy Allocator 管理空间：

- **分配粒度**：以页（Page，通常 4KB）为最小单位
- **数据对齐**：所有 offset 做页面对齐，适配 DirectIO
- **空间回收**：Buddy 合并减少外部碎片
- **位置**：SlotAllocator 位于 `src/common/`，被 Store 和 Meta（测试用）共同使用

### 4.3 三级读取亲和

Client 与本地 Store 共进程启动，同物理节点上的 Store 文件互相可见。写入始终走本地 Store，读取按三级亲和选择路径：

| 层级 | 亲和关系 | IO 方式 | 预估延迟 |
|------|----------|----------|----------|
| **Level 0** | 同进程 Store | `local_store_->BatchRead()` in-process 批量直通 | ~300us |
| **Level 1** | 同节点跨进程 Store | NodeLocalAccessor DirectIO (`FdCache + pread`) | ~500us |
| **Level 2** | 远程节点 Store | StoreRpcClient RPC 到 Store 节点 | ~2ms |

**写入路径**：所有写入只走 Level 0（`local_store_->BatchPut()`），Store 内部完成空间分配、DirectIO 写入、元数据索引更新和 Meta 同步。

**关键收益**：Level 0/1 场景下读写操作完全不经过 RPC，Client 统一通过 DirectIO 操作 SSD 文件，极大降低延迟。

### 4.4 Store 自管理空间与驱逐（先删 Meta + 宽限期回收）

每个 Store 独立管理生命周期。写入流程：

```
Store.Put(key, data, size):
  1. SlotAllocator.AllocChunk() → offset    // 分配 SSD 空间
  2. Write(offset, data, size)                // DirectIO 写入
  3. StoreMetaIndex.Put(key, {offset, size})  // 更新本地索引
  4. MetaSyncClient.SyncCommit(records)       // 异步同步到 Meta
```

驱逐流程遵循**先删元数据、延迟回收空间**的原则，避免其他 Client 在清理期间读到不可预知的数据：

```
Store 驱逐 (后台线程):
  1. 当使用率 > 85%: 按 access_time 找冷数据候选
  2. ★ 先通知 Meta 删除元数据:
     MetaSyncClient.SyncRemove(keys)          // 等待 Meta 确认
     → 此时 BatchExist 不再命中这些 key
  3. 将候选移入 PendingEvictQueue              // 数据仍在 SSD
  4. PendingEvictQueue 后台线程:
     超过 5s 宽限期 → SlotAllocator.FreeChunk  // 真正回收空间
```

**驱逐时序保证**：
```
T0: Store 选取冷数据
T1: Store → Meta SyncRemove(keys)            ← Meta 删除元数据
T2: 候选移入 PendingEvictQueue               ← 数据仍在 SSD，可读
    ← 5s 宽限期 →                            ← 已获取旧描述的 Client 仍可读取
T3: SlotAllocator.FreeChunk + Index.Remove  ← 空间回收
```

### 4.5 节点级 IO 调度

每个推理节点包含 8 个 GPU Worker（即 8 个 Client）和多个 Store，它们共享同一 NVMe SSD 带宽和网络带宽。IO Scheduler 作为节点级独立进程，协调 IO 时序：

- **通道化统计**：IO 按资源消耗分为六类通道（LOCAL_SSD_READ/WRITE、NET_TX_READ/WRITE、NET_RX_READ/WRITE），分别统计 SSD 带宽、网络 TX/RX 带宽
- **按节点分通道**：网络带宽按远程节点地址分通道统计，支持后续按通道流控
- **放通模式起步**：当前阶段所有调度策略为 Passthrough，仅做通道化带宽/时延统计
- **峰值带宽检测**：对并发 IO 带宽需求按通道累加（SSD 带宽和网络带宽分别检测），判断是否打满硬件上限
- **快速 Bypass**：Scheduler 异常时 Client/Store 在微秒级感知并绕过调度，不影响数据路径
- **UDS 通信**：使用 Unix Domain Socket 降低调度 RPC 延迟（< 50us）

```
资源消耗模型:
┌──────────────┬──────────┬──────────┬──────────┬───────────────────┐
│ IOChannel    │ SSD 带宽 │ 网络发送 │ 网络接收 │ 发起者             │
├──────────────┼──────────┼──────────┼──────────┼───────────────────┤
│LOCAL_SSD_READ │   是     │    -     │    -     │ Client (L0/L1)    │
│LOCAL_SSD_WRITE│   是     │    -     │    -     │ Client (L0)       │
│NET_TX_READ    │   -      │   是     │    -     │ Client (→远程)    │
│NET_RX_READ    │   是     │    -     │   是     │ Store (远程→本地)  │
└──────────────┴──────────┴──────────┴──────────┴───────────────────┘
```

### 4.6 GIL 优化与零拷贝

Client 模块需要同时对接 Python（LMCache）和 C++ 内部模块：

- **GIL 优化**：C++ 核心操作释放 GIL，仅在 Python 对象交互时持有 GIL
- **零拷贝**：直接操作 `MemoryObj.byte_array` 的底层 buffer，避免 Python 层数据复制
- **Fire-and-Forget**：Put 操作支持异步提交后立即返回，C++ 后台线程完成 `local_store_->BatchPut()` 写入

## 5. 接口适配

### 5.1 LMCache RemoteConnector 适配

FalconKV 需要实现 LMCache 的 `RemoteConnector` 接口：

```python
# LMCache RemoteConnector 核心接口
class RemoteConnector:
    # 单 key 操作（FalconKV 内部批量化的 fallback）
    async def exists(key: CacheEngineKey) -> bool
    def exists_sync(key: CacheEngineKey) -> bool
    async def get(key: CacheEngineKey) -> Optional[MemoryObj]
    async def put(key: CacheEngineKey, memory_obj: MemoryObj)

    # 批量操作（FalconKV 核心优化路径）
    def support_batched_get() -> bool        # True
    def support_batched_put() -> bool        # True
    def support_batched_contains() -> bool   # True
    def support_batched_async_contains() -> bool  # True
    def support_batched_get_non_blocking() -> bool  # True

    async def batched_get(keys) -> List[Optional[MemoryObj]]
    async def batched_put(keys, memory_objs)
    def batched_contains(keys) -> int
    async def batched_async_contains(lookup_id, keys, pin) -> int
    async def batched_get_non_blocking(lookup_id, keys) -> List[MemoryObj]

    # 生命周期
    async def close()
    async def list() -> List[str]
```

### 5.2 URL Schema 与配置

```python
# LMCache 配置
config.remote_url = "falconkv://meta_host:meta_port"
config.extra_config = {
    "falconkv_workspace": "/usr/local/falconkv",
    "falconkv_config_file": "/usr/local/falconkv/config/falconkv.json",
    "falconkv_cache_capacity": 100000,
    "falconkv_fire_and_forget": True,
}
```

### 5.3 Key 格式映射

LMCache 的 `CacheEngineKey` 格式为 `{model_name}/{world_size}/{worker_id}/{chunk_hash}`，FalconKV 将其转换为内部 key 格式用于元数据查询和空间分配。

## 6. 构建与部署

### 6.1 构建系统

```
falconkv/
├── CMakeLists.txt          # 顶层 CMake
├── src/
│   ├── client/             # Client 模块
│   ├── meta/               # Meta 模块 (纯内存分片存储)
│   ├── store/              # Store 模块 (含 StoreMetaIndex, MetaSyncClient)
│   ├── common/             # 公共模块 (含 SlotAllocator)
│   ├── transfer/           # Transfer 模块
│   └── scheduler/          # IO Scheduler 模块
├── python/                 # Python 绑定
│   └── pyfalconkv/
├── proto/                  # brpc protobuf 定义
├── tests/
│   ├── unit/
│   └── integration/
├── config/                 # 配置模板
└── build.sh                # 构建脚本
```

### 6.2 构建命令

```bash
./build.sh                                    # 完整构建 (Release)
./build.sh build falconkv --debug             # Debug 构建
./build.sh test                               # 运行单元测试
./build.sh install                            # 安装到 /usr/local/falconkv
```

## 7. 性能目标

| 指标 | 目标值 | 说明 |
|------|--------|------|
| BatchExist (100 keys) | < 2ms | 缓存 + 本地 Store + Meta 查询 |
| BatchPut (100 keys) | < 10ms | 本地 Store SlotAlloc + DirectIO 写入 |
| BatchGet (100 keys) | < 5ms | DirectIO 读取 + 网络传输 |
| 单次 Put 延迟 | < 1ms | 本地 Store 写入 |
| 吞吐量 | > 5000 keys/s | 每秒处理的 KV 块数量 |

## 8. 文档索引

| 文档 | 说明 |
|------|------|
| [falconkv_client_design.md](falconkv_client_design.md) | Client 模块详细设计 |
| [falconkv_meta_design.md](falconkv_meta_design.md) | Meta 模块详细设计 |
| [falconkv_transfer_design.md](falconkv_transfer_design.md) | Transfer 模块详细设计 |
| [falconkv_store_design.md](falconkv_store_design.md) | Store 模块详细设计 |
| [falconkv_scheduler_design.md](falconkv_scheduler_design.md) | IO Scheduler 模块详细设计 |
