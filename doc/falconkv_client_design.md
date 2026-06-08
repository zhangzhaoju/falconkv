# FalconKV Client 模块设计文档

## 1. 模块概述

Client 模块是 FalconKV 对外暴露的核心接口层，负责：
- 适配 LMCache V0.3.12 的 `RemoteConnector` 接口
- 提供高性能的 BatchGet / BatchPut / BatchExist 操作
- 管理 Key 描述信息的本地缓存
- **绑定本地 Store**：Client 与本地 Store 共进程，写入始终委托给 local_store_
- **三级读取亲和**：读取按 local_store → NodeLocalAccessor → RPC 三级路径执行

### 1.1 设计目标

| 目标 | 实现手段 |
|------|----------|
| 低时延 | 批量 RPC 聚合、Key 描述本地缓存、零拷贝传输 |
| 高吞吐 | 并发异步操作、Fire-and-Forget 写入模式 |
| GIL 友好 | C++ 核心逻辑释放 GIL、仅 Python 交互时持锁 |
| 零拷贝 | 直接操作 MemoryObj 底层 buffer、避免序列化 |

### 1.2 参考实现

- **LMCache FalconConnector**：基于 pyfalconfs 的文件语义实现，使用 `AsyncPut/AsyncGet/AsyncExists` 批量操作
- **LMCache MooncakestoreConnector**：使用 `batch_put_from/batch_get_into` 的零拷贝实现，直接操作 tensor data_ptr

FalconKV Client 综合两者的优势：采用 Mooncake 的零拷贝 buffer 操作方式，结合 FalconKV 自身的元数据分离架构实现更高效的数据管理。

## 2. 架构设计

### 2.1 整体分层架构

FalconKV 的 Python 集成采用 **四层分离** 架构，`FalconKVClientImpl` 不直接暴露给 Python，而是通过 `FalconKVBridge` 适配层进行隔离。适配层的目的是：当 `FalconKVClientImpl` 的接口发生变更时，只需修改 Bridge 层的转换逻辑，不影响上层 Python 代码。

```
┌─────────────────────────────────────────────────────────────────────┐
│ Layer 4: LMCache 集成层 (纯 Python)                                 │
│                                                                      │
│  ┌──────────────────────────┐    ┌────────────────────────────┐     │
│  │ FalconKVConnectorAdapter │    │ FalconKVConnector          │     │
│  │ (ConnectorAdapter)       │    │ (RemoteConnector)          │     │
│  │ schema: "falconkv://"    │    │                            │     │
│  └──────────┬───────────────┘    │ + batched_get()            │     │
│             │ create_connector()  │ + batched_put()            │     │
│             │                     │ + batched_contains()       │     │
│             ▼                     │ + close() / list()         │     │
│  ┌──────────────────────────┐    └──────────┬─────────────────┘     │
│  │ ConnectorManager         │               │                       │
│  │ (LMCache 自动发现)       │               │ import pyfalconkv     │
│  └──────────────────────────┘               │                       │
└─────────────────────────────────────────────┼───────────────────────┘
                                              │
┌─────────────────────────────────────────────┼───────────────────────┐
│ Layer 3: Python 封装层 (pyfalconkv 包)      │                       │
│                                              ▼                       │
│  ┌──────────────────────────────────────────────────────────┐       │
│  │ Client 类 (pyfalconkv/client.py)                         │       │
│  │  - BatchExist(keys) → int                                 │       │
│  │  - BatchPut(keys, ptrs, sizes) → None                     │       │
│  │  - BatchGet(keys, ptrs, sizes) → List[int]                │       │
│  │  - FireAndForgetPut(keys, ptrs, sizes, py_objs) → None    │       │
│  │  - Close() → None                                         │       │
│  └──────────────────────────┬───────────────────────────────┘       │
│                              │ import _pyfalconkv_internal           │
└──────────────────────────────┼──────────────────────────────────────┘
                               │
┌──────────────────────────────┼──────────────────────────────────────┐
│ Layer 2: C Extension 模块    │                                      │
│ (_pyfalconkv_internal.so)    ▼                                      │
│                                                                      │
│  ┌──────────────────────────────────────────────────────────────┐    │
│  │ Raw Python C API Module                                       │    │
│  │                                                                │    │
│  │ 模块级函数:                                                   │    │
│  │  Init(config_file)               → handle                     │    │
│  │  BatchExistSync(handle, keys)    → int                        │    │
│  │  BatchPutSync(handle, keys,      → None                       │    │
│  │    data_ptrs, sizes)                                          │    │
│  │  BatchGetSync(handle, keys,      → List[int]                  │    │
│  │    data_ptrs, sizes)                                          │    │
│  │  FireAndForgetPut(handle, keys, → None                        │    │
│  │    data_ptrs, sizes, py_objs)                                 │    │
│  │  Close(handle)                  → None                        │    │
│  │                                                                │    │
│  │ 异步支持类型:                                                 │    │
│  │  AsyncState (Python awaitable)                                 │    │
│  │  AsyncTaskThreadPool (优先级线程池)                            │    │
│  └──────────────────────────┬───────────────────────────────────┘    │
│                              │ 调用 C++ Bridge 接口                   │
└──────────────────────────────┼──────────────────────────────────────┘
                               │
┌──────────────────────────────┼──────────────────────────────────────┐
│ Layer 1: C++ 适配层          ▼                                      │
│ (FalconKVBridge)                                                     │
│                                                                      │
│  ┌──────────────────────────────────────────────────────────────┐    │
│  │ FalconKVBridge                                                │    │
│  │  - 持有 FalconKVClientImpl 实例                               │    │
│  │  - 提供 C 风格的稳定接口 (extern "C")                         │    │
│  │  - 管理 Python buffer → C++ BufferInfo 的转换                │    │
│  │  - 管理 AsyncState 生命周期                                   │    │
│  │  - Fire-and-Forget: 管理 PyObject 引用 + 回调                │    │
│  │                                                                │    │
│  │  + Init(config_file)                → bridge_handle           │    │
│  │  + BatchExistSync(keys)             → int                     │    │
│  │  + BatchPutSync(keys, ptrs, sizes)  → status                  │    │
│  │  + BatchGetSync(keys, ptrs, sizes)  → bytes_read[]            │    │
│  │  + FireAndForgetPut(keys, ptrs,     → void                    │    │
│  │    sizes, py_callback)                                        │    │
│  │  + Close()                          → void                    │    │
│  └──────────────────────────┬───────────────────────────────────┘    │
│                              │ 持有                                   │
└──────────────────────────────┼──────────────────────────────────────┘
                               │
┌──────────────────────────────┼──────────────────────────────────────┐
│ Layer 0: C++ 核心层          ▼                                      │
│ (FalconKVClientImpl)                                                  │
│                                                                      │
│  ┌──────────────────────────────────────────────────────────────┐    │
│  │ FalconKVClientImpl (C++ Core)                                │    │
│  │                                                              │    │
│  │  ┌───────────────┐  ┌────────────────────┐                   │    │
│  │  │ KeyDescCache  │  │ StoreRpcClientMgr  │                   │    │
│  │  │ (LRU Cache)   │  │ + NodeLocalAccessor│                   │    │
│  │  └───────────────┘  └────────────────────┘                   │    │
│  │                                                              │    │
│  │  ┌────────────────────────────────────────────────────────┐  │    │
│  │  │ local_store_ (FalconKVStore*, 绑定的本地 Store)         │  │    │
│  │  │  - BatchPut(): 写入始终走本地 Store                    │  │    │
│  │  │  - BatchGet(): 读取本地数据                            │  │    │
│  │  │  - BatchContains(): 查询本地 key 存在性                │  │    │
│  │  └────────────────────────────────────────────────────────┘  │    │
│  │                                                              │    │
│  │  ┌────────────────────────────────────────────────────────┐  │    │
│  │  │ meta_client_ (Meta 查询客户端)                         │  │    │
│  │  │  - BatchExist(): 查询全局 key 存在性                   │  │    │
│  │  │  - BatchLookup(): 查询 key 描述信息                    │  │    │
│  │  │  - 断连时自动返回空结果（降级为本地模式）               │  │    │
│  │  │  - 后台重连循环 (每 5 秒)                               │  │    │
│  │  └────────────────────────────────────────────────────────┘  │    │
│  │                                                              │    │
│  │  ┌────────────────────────────────────────────────────────┐  │    │
│  │  │ SchedulerProxy (IO 调度代理，可选)                     │  │    │
│  │  │  - RequestIO() / ReportIOCompletion()                  │  │    │
│  │  └────────────────────────────────────────────────────────┘  │    │
│  │                                                              │    │
│  │  + BatchExist(keys) → (hit_count, key_descs[])              │    │
│  │  + BatchPut(keys, bufs) → status[]                          │    │
│  │  + BatchGet(key_descs, bufs) → bytes_read[]                 │    │
│  │  + Close()                                                  │    │
│  └──────────────────────────────────────────────────────────────┘    │
└──────────────────────────────────────────────────────────────────────┘
```

#### 2.1.1 为什么需要 FalconKVBridge 适配层

| 原因 | 说明 |
|------|------|
| **接口稳定性** | `FalconKVClientImpl` 的接口（如 `BufferInfo`、`KeyDescriptor`）可能随业务迭代变化。Bridge 层将这些 C++ 类型转换为 C 风格的原始类型（`void*`、`int`、`char**`），使 C Extension 模块不受 C++ 接口变更影响 |
| **GIL 隔离** | Bridge 层统一处理 GIL 释放/获取逻辑，C Extension 只负责 Python 对象转换，不需要关心线程安全 |
| **异步生命周期** | Fire-and-Forget 模式下 Python 对象（buffer、MemoryObj）需要跨线程存活。Bridge 层统一管理 `Py_INCREF/Py_DECREF` 和 `ref_count_down` 回调 |
| **构建解耦** | C Extension 只依赖 Bridge 的头文件，不直接依赖 `FalconKVClientImpl` 及其全部头文件链，减少构建时间暴露 |
| **替代方案灵活性** | 未来如果更换底层实现（如换用不同的存储引擎），只需替换 Bridge 内部的 `FalconKVClientImpl` 调用，上层 Python 代码无需任何修改 |

#### 2.1.2 与 FalconFS pyfalconfs 的对比

| 维度 | FalconFS (pyfalconfs) | FalconKV (pyfalconkv) |
|------|----------------------|----------------------|
| 语义 | 文件系统语义 (Create/Open/Read/Write/Close) | KV 批量操作语义 (BatchExist/Put/Get) |
| Python 绑定方式 | Raw Python C API | Raw Python C API (一致) |
| 模块级 vs 类绑定 | 模块级函数 + 全局状态 | 模块级函数 + handle 参数 |
| 异步机制 | AsyncTaskThreadPool + AsyncState (awaitable) | 相同模式 |
| 适配层 | 无（C API 直接封装文件操作） | FalconKVBridge（隔离 C++ 业务接口） |
| Buffer 传递 | Py_buffer（文件读写） | data_ptr + size（零拷贝直传 tensor） |

### 2.2 数据结构

#### 2.2.1 Key 描述信息

```cpp
struct KeyDescriptor {
    std::string key;           // KV Cache key (LMCache CacheEngineKey.to_string())
    uint32_t store_id;         // Store 节点 ID
    uint64_t offset;           // 数据在 SSD 文件中的偏移（页面对齐）
    uint32_t size;             // 数据大小（字节）
    uint64_t access_time_ms;   // 最后访问时间
    std::string store_addr;    // Store RPC 地址 (host:port，ACCESS_REMOTE_RPC 时使用)

    // 亲和性层级，决定 Client 的 IO 方式
    enum AccessType {
        ACCESS_LOCAL_DIRECT = 0,  // Level 0: local_store_ in-process 直通
        ACCESS_NODE_DIRECT = 1,   // Level 1: NodeLocalAccessor DirectIO
        ACCESS_REMOTE_RPC = 2,    // Level 2: StoreRpcClient RPC
    };
    AccessType access_type = ACCESS_REMOTE_RPC;
};
```

#### 2.2.2 Key 描述缓存

```cpp
// LRU 缓存，缓存最近 BatchExist 命中的 key 描述信息
// 避免后续 BatchGet 再次查询 Meta
class KeyDescCache {
public:
    // 查找单个 key 描述
    std::optional<KeyDescriptor> Lookup(const std::string& key);

    // 批量查找，返回命中数量和命中描述列表
    // 未命中的 key 记录到 missing_keys 中
    int BatchLookup(const std::vector<std::string>& keys,
                    std::vector<KeyDescriptor>& hit_descs,
                    std::vector<std::string>& missing_keys);

    // 批量插入
    void BatchInsert(const std::vector<std::pair<std::string, KeyDescriptor>>& items);

    // 批量失效（当数据被驱逐时）
    void BatchInvalidate(const std::vector<std::string>& keys);

private:
    static constexpr size_t DEFAULT_CAPACITY = 100000;  // 默认容量
    std::mutex mutex_;
    // key -> KeyDescriptor
    std::unordered_map<std::string, KeyDescriptor> cache_;
    // LRU 链表
    std::list<std::string> lru_list_;
    size_t capacity_;
};
```

## 3. 核心接口实现

### 3.1 BatchExist（两步查询，不查缓存）

```
┌─────────────────────────────────────────────────────────────────┐
│ BatchExist(keys: [CacheEngineKey])                              │
│                                                                  │
│  1. Python: keys → key_strings (to_string())                    │
│     释放 GIL                                                     │
│                                                                  │
│  2. C++ Core（两步查询）:                                        │
│                                                                  │
│     Step 1: 查询本地 Store                                       │
│        - local_store_->BatchContains(keys)                       │
│        - 命中的 key: 构造 KeyDescriptor (ACCESS_LOCAL_DIRECT)    │
│        - 仍未命中的 key: 组成 missing_keys                       │
│                                                                  │
│     Step 2: 查询 Meta 服务                                       │
│        - 若 missing_keys 非空:                                   │
│          - 检查 meta_client_.connected()                        │
│          - 若断连: 直接返回空结果（降级为本地模式）              │
│          - 若已连接: 构造 BatchExistRequest protobuf             │
│          - 一次性 RPC 到 Meta 模块                               │
│          - 解析 BatchExistResponse                               │
│          - RPC 失败时标记断连，返回空结果                        │
│          - 将命中的 key_descs 插入 KeyDescCache                  │
│            （供后续 BatchGet 使用）                               │
│                                                                  │
│  3. 汇总结果:                                                    │
│     - hit_count = store_hits + meta_hits                        │
│     - 返回连续命中的 key 数量（前缀匹配语义）                     │
│                                                                  │
│  4. 恢复 GIL，返回 Python int                                    │
└─────────────────────────────────────────────────────────────────┘
```

**为什么 BatchExist 不查 KeyDescCache**：

Store 驱逐时先通过 `SyncRemove` 删除 Meta 元数据，但 Client 的 KeyDescCache 中可能仍有已驱逐 key 的旧描述。如果 BatchExist 信任缓存，会返回已不存在的 key 为"命中"，导致后续 BatchGet 读到不可预知的数据。因此 BatchExist 必须直接查询本地 Store 和 Meta，获取最新的存在性信息。

**KeyDescCache 的用途**：BatchExist 从 Meta 获取的 key_descs 仍会写入 KeyDescCache，供后续 BatchGet 使用。BatchGet 使用缓存描述时，依赖 Store 的 5 秒驱逐宽限期保证数据仍可读取。

**关键优化**：
- **本地 Store 优先**：本地数据无需经过网络即可确认存在性
- **批量 RPC**：所有 key 封装成一个大消息，一次网络往返
- **前缀匹配语义**：LMCache 要求返回连续命中的前缀长度

### 3.2 BatchPut（本地 Store 写入）

```
┌─────────────────────────────────────────────────────────────────┐
│ BatchPut(keys: [CacheEngineKey], memory_objs: [MemoryObj])      │
│                                                                  │
│  架构前提：Client 和本地 Store 共进程，写操作始终写入本地 Store │
│                                                                  │
│  1. Python: 准备 buffer 指针                                     │
│     - 对每个 memory_obj:                                         │
│       ref_count_up() (防止内存池回收)                             │
│       raw_buffer = memory_obj.byte_array                        │
│       data_ptr = buffer 的内存地址                               │
│       data_size = len(buffer)                                    │
│     释放 GIL                                                     │
│                                                                  │
│  2. C++ Core:                                                    │
│     a. 向 Scheduler 申请 IO (可选)                               │
│                                                                  │
│     b. 委托本地 Store 批量写入:                                   │
│        - local_store_->BatchPut(keys, data_ptrs, sizes)          │
│        - Store 内部完成空间分配、SSD 写入、元数据同步            │
│        - Client 不感知 offset 和空间分配细节                     │
│                                                                  │
│     c. 更新本地缓存:                                             │
│        - 将成功写入的 key 构造为 KeyDescriptor                   │
│        - access_type = ACCESS_LOCAL_DIRECT                       │
│        - 插入 KeyDescCache                                       │
│                                                                  │
│     d. 向 Scheduler 上报 IO 完成 (异步)                          │
│                                                                  │
│  3. 恢复 GIL                                                     │
│     - Fire-and-Forget 模式: 异步 ref_count_down                 │
│     - 同步模式: 立即 ref_count_down                              │
└─────────────────────────────────────────────────────────────────┘
```

**关键设计**：
- **Client 不直接操作文件**：所有数据读写统一通过 Store 模块进行
- **单一写入路径**：Client 只调用 `local_store_->BatchPut()`，空间分配和元数据管理由 Store 内部完成
- **Client 不感知 offset**：Store 内部的 SlotAllocator 负责空间分配，Client 不需要 AllocResult
- **Fire-and-Forget**：写入可以异步提交，后台线程完成后释放 Python 引用

### 3.3 BatchGet（三级亲和读取）

```
┌─────────────────────────────────────────────────────────────────┐
│ BatchGet(keys: [CacheEngineKey]) → [Optional[MemoryObj]]        │
│                                                                  │
│  架构前提：Client 与本地 Store 共进程，同节点 Store 文件互可见    │
│                                                                  │
│  1. Python: 为每个 key 分配 MemoryObj                            │
│     - local_cpu_backend.allocate(meta_shapes, meta_dtypes, fmt)  │
│     - 获取 buffer 的 data_ptr 和 size                           │
│     释放 GIL                                                     │
│                                                                  │
│  2. C++ Core:                                                    │
│     a. 查询 KeyDescCache 获取 key 描述                           │
│        - 若缓存未命中: 构造 BatchLookupRequest RPC 到 Meta       │
│        - 若 Meta 断连: BatchLookup 返回空结果（跳过 RPC 超时）   │
│        - 将结果缓存（含 store_addr 和 access_type）              │
│                                                                  │
│     b. 向 Scheduler 申请 IO (可选)                               │
│        - scheduler_proxy_->RequestIO(store_id, total_size)      │
│        - 若 bypass 或 passthrough: 立即返回                      │
│                                                                  │
│     c. 按 access_type 分组读取 — 三级亲和路径:                  │
│                                                                  │
│        [Level 0: ACCESS_LOCAL_DIRECT]                           │
│        → local_store_->BatchRead(local_reads)                   │
│        → FalconKVStore in-process 批量直通，~300us                │
│                                                                  │
│        [Level 1: ACCESS_NODE_DIRECT]                            │
│        → node_accessor_.Read(store_id, offset, buffer, size)   │
│        → NodeLocalAccessor DirectIO 读取同节点 Store 文件       │
│        → NodeLocalAccessor 内部维护 FdCache + DirectIO 对齐     │
│        → ~500us                                                  │
│                                                                  │
│        [Level 2: ACCESS_REMOTE_RPC]                             │
│        → StoreRpcClient::BatchRead(offsets, sizes, buffers, results)│
│        → brpc RPC 到远端 Store 服务，数据写入 buffer             │
│        → BatchRead 内部按 max_body_size 自动拆分为多个子批次     │
│        → ~2ms                                                    │
│                                                                  │
│     d. 并行执行所有分组读取                                      │
│        - Level 0/1: 本地 DirectIO 可 batch 提交                 │
│        - Level 2: 不同远程 Store 节点并行 RPC                    │
│                                                                  │
│     e. 等待所有读取完成                                          │
│                                                                  │
│     f. 向 Scheduler 上报 IO 完成 (异步)                          │
│        - scheduler_proxy_->ReportIOCompletion(                   │
│            start_ts, done_ts, io_size)                           │
│                                                                  │
│  3. 恢复 GIL                                                     │
│     - 对每个结果:                                                 │
│       成功: reshape_partial_chunk (若需要)                        │
│       失败: memory_obj.ref_count_down(), 返回 None              │
└─────────────────────────────────────────────────────────────────┘
```

#### 3.3.1 NodeLocalAccessor（同节点文件直通读写器）

Level 1 场景下，Client 通过 `NodeLocalAccessor` 读取同节点上其他 Store 的数据文件。Client 不直接操作文件，而是调用 `node_accessor_.Read(store_id, ...)`。NodeLocalAccessor 内部封装了 FdCache + DirectIO 对齐逻辑，Client 对此完全不可见。

```cpp
class NodeLocalAccessor {
public:
    // 注册 store_id → data_file 映射（从 KeyDescriptor 或配置中学习）
    void RegisterStoreFile(uint32_t store_id, const std::string& data_file);

    // 读取指定 store 的数据文件（DirectIO，内含对齐处理）
    Status Read(uint32_t store_id, uint64_t offset, void* buffer, uint32_t size);

    void Close();

private:
    int GetFd(uint32_t store_id);
    std::unordered_map<uint32_t, std::string> store_files_;  // store_id → data_file
    FdCache fd_cache_;  // 内部使用，Client 不可见
};
```

**关键设计**：
- **Client 隔离**：Client 只知道 `store_id`，不知道 `data_file` 路径，不持有文件描述符
- **以 store_id 为索引**：Client 调用 `node_accessor_.Read(store_id, offset, ...)` 而非 `pread(fd, ...)`
- **内部 FdCache**：NodeLocalAccessor 内部维护 FdCache，处理文件打开和复用
- **DirectIO 对齐**：对齐处理逻辑封装在 NodeLocalAccessor 内部，Client 无需关心
- **Level 0 分离**：ACCESS_LOCAL_DIRECT 不经过 NodeLocalAccessor，直接调用 `local_store_->BatchRead()`

#### 3.3.2 三级亲和性能对比

| 层级 | IO 路径 | 预估延迟 | 网络开销 |
|------|---------|----------|----------|
| Level 0 | local_store_ in-process 批量直通 (FalconKVStore::BatchRead) | ~300us | 无 |
| Level 1 | NodeLocalAccessor DirectIO (FdCache + pread) | ~500us | 无（文件系统直读） |
| Level 2 | StoreRpcClient BatchRead RPC (brpc, 按大小自动拆分子批次) | ~2ms | 一次网络往返 |

### 3.4 batched_get_non_blocking

```
与 BatchGet 相同的流程，但返回连续前缀：

1. 并发获取所有 keys 的数据
2. 遍历结果，找到第一个 None 的位置
3. 丢弃该位置之后的所有 MemoryObj (ref_count_down)
4. 返回连续前缀的 MemoryObj 列表
```

### 3.5 batched_async_contains

```
异步版本的 contains 检查：

1. 使用 asyncio.gather 并发检查所有 keys
2. 返回连续命中的前缀长度
3. 命中的 key_descs 缓存到 KeyDescCache
```

## 4. Python 绑定层设计

### 4.1 目录结构

```
python/
├── _pyfalconkv_internal/          # Layer 2: C Extension 模块
│   ├── CMakeLists.txt              # 构建 _pyfalconkv_internal.so
│   ├── _pyfalconkv_internal.cpp    # Raw Python C API 绑定
│   └── throw_hook.cpp              # 异常栈追踪调试工具 (可选)
├── pyfalconkv/                     # Layer 3: Python 封装包
│   ├── __init__.py                 # 导出 Client 类
│   ├── client.py                   # Client 类，封装 C Extension
│   ├── connector.py                # FalconKVConnector (LMCache RemoteConnector)
│   └── adapter.py                  # FalconKVConnectorAdapter (LMCache 自动发现)
├── setup.py                        # pip install 构建
└── pyproject.toml
```

### 4.2 Layer 1: FalconKVBridge（C++ 适配层）

FalconKVBridge 位于 C++ 侧，直接持有 `FalconKVClientImpl` 实例。它向 C Extension 层暴露纯 C 风格的接口，屏蔽所有 C++ 类型。

```cpp
// python/_pyfalconkv_internal/falconkv_bridge.h
#pragma once
#include <string>
#include <vector>
#include <cstdint>

namespace falconkv {

// Bridge 层的 buffer 描述，使用原始类型，不暴露 C++ 内部结构
struct BridgeBuffer {
    void* data_ptr;    // tensor.data_ptr()
    uint32_t size;     // tensor.nbytes()
};

class FalconKVBridge {
public:
    struct Config {
        std::string config_file;         // FalconKV 配置文件路径
        size_t cache_capacity = 100000;  // Key 描述缓存容量
        int worker_id = -1;  // -1 表示未设置；>= 0 时自动计算 store_id
    };

    explicit FalconKVBridge(const Config& config);
    ~FalconKVBridge();

    // 禁止拷贝
    FalconKVBridge(const FalconKVBridge&) = delete;
    FalconKVBridge& operator=(const FalconKVBridge&) = delete;

    // ---- 同步批量操作 ----

    // 批量查询 key 存在性，返回连续命中前缀长度
    int BatchExistSync(const std::vector<std::string>& keys);

    // 批量写入（同步），keys[i] 的数据为 buffers[i]
    // 返回 0 表示全部成功，非 0 表示部分失败
    int BatchPutSync(const std::vector<std::string>& keys,
                     const std::vector<BridgeBuffer>& buffers);

    // 批量读取（同步），读取到 buffers[i].data_ptr 指向的内存
    // 返回每个 key 实际读取的字节数，<=0 表示失败
    std::vector<int32_t> BatchGetSync(const std::vector<std::string>& keys,
                                       const std::vector<BridgeBuffer>& buffers);

    // ---- Fire-and-Forget 写入 ----

    // 异步写入：C++ 后台线程完成后调用 py_callback
    // py_callback 签名: void(*)(void* user_data)
    // Bridge 不持有 GIL，由 C Extension 层负责 GIL 管理
    void FireAndForgetPut(const std::vector<std::string>& keys,
                          const std::vector<BridgeBuffer>& buffers,
                          void (*py_callback)(void*),
                          void* user_data);

    // ---- 生命周期 ----

    void Close();

private:
    // 内部持有 FalconKVClientImpl 实例
    // 当 FalconKVClientImpl 接口变更时，只需修改此处的适配代码
    std::unique_ptr<class FalconKVClientImpl> impl_;
};

} // namespace falconkv
```

```cpp
// python/_pyfalconkv_internal/falconkv_bridge.cpp
#include "falconkv_bridge.h"
#include "src/client/falconkv_client_impl.h"
#include "src/common/logging.h"

namespace falconkv {

FalconKVBridge::FalconKVBridge(const Config& config) {
    // 1. 加载 FalconKVConfig
    FalconKVClientImpl::Config impl_config;
    impl_config.config_file = config.config_file;
    impl_config.cache_capacity = config.cache_capacity;

    // 2. 初始化日志
    // ...

    // 3. 设置 brpc max_body_size 从配置
    // brpc::FLAGS_max_body_size = ... (从配置中读取)

    // 4. worker_id >= 0 时计算 store_id 和 listen_port 偏移
    if (config.worker_id >= 0) {
        impl_config.store_id = config.worker_id;  // 或 hash(node_id)*max_gpu + worker_id
        impl_config.listen_port = base_port + config.worker_id;
    }

    // 5. 创建 FalconKVStore 并 Init
    store_ = std::make_unique<FalconKVStore>(store_config);
    store_->Init(meta_addr);

    // 6. 启动 Store RPC server
    store_service_ = std::make_unique<StoreServiceImpl>(store_.get());
    store_rpc_server_ = std::make_unique<brpc::Server>();
    store_rpc_server_->AddService(store_service_.get(),
                                   brpc::SERVER_DOESNT_OWN_SERVICE);
    brpc::ServerOptions options;
    store_rpc_server_->Start(listen_port, &options);

    // 7. 创建 FalconKVClientImpl
    impl_ = std::make_unique<FalconKVClientImpl>(impl_config);

    // 8. 绑定 Store 到 Client
    impl_->BindStore(store_.get());

    // 9. 启动 Fire-and-Forget 工作线程
    for (size_t i = 0; i < kWorkerThreads; ++i) {
        workers_.emplace_back(&FalconKVBridge::WorkerLoop, this);
    }
}

FalconKVBridge::~FalconKVBridge() {
    Close();
}

int FalconKVBridge::BatchExistSync(const std::vector<std::string>& keys) {
    std::vector<KeyDescriptor> descs;
    return impl_->BatchExist(keys, descs);
}

int FalconKVBridge::BatchPutSync(const std::vector<std::string>& keys,
                                  const std::vector<BridgeBuffer>& buffers) {
    // BridgeBuffer → BufferInfo 适配
    std::vector<BufferInfo> buf_infos;
    buf_infos.reserve(buffers.size());
    for (const auto& b : buffers) {
        buf_infos.push_back({b.data_ptr, b.size});
    }
    auto statuses = impl_->BatchPutSync(keys, buf_infos);
    // 检查是否有失败
    for (const auto& s : statuses) {
        if (!s.Ok()) return -1;
    }
    return 0;
}

std::vector<int32_t> FalconKVBridge::BatchGetSync(
    const std::vector<std::string>& keys,
    const std::vector<BridgeBuffer>& buffers) {
    // BridgeBuffer → BufferInfo 适配
    std::vector<BufferInfo> buf_infos;
    buf_infos.reserve(buffers.size());
    for (const auto& b : buffers) {
        buf_infos.push_back({b.data_ptr, b.size});
    }
    return impl_->BatchGetSync(keys, buf_infos);
}

void FalconKVBridge::FireAndForgetPut(
    const std::vector<std::string>& keys,
    const std::vector<BridgeBuffer>& buffers,
    void (*py_callback)(void*),
    void* user_data) {
    // BridgeBuffer → BufferInfo 适配
    std::vector<BufferInfo> buf_infos;
    buf_infos.reserve(buffers.size());
    for (const auto& b : buffers) {
        buf_infos.push_back({b.data_ptr, b.size});
    }

    // 拷贝 keys（异步任务可能在 Python 端释放原始数据后执行）
    auto keys_copy = std::make_shared<std::vector<std::string>>(keys);
    auto bufs_copy = std::make_shared<std::vector<BufferInfo>>(std::move(buf_infos));

    // 提交到 FalconKVClientImpl 的内部线程池
    // 完成后回调 py_callback（C Extension 层会包装为 GIL + ref_count_down）
    impl_->AsyncBatchPut(*keys_copy, *bufs_copy,
        [py_callback, user_data]() {
            py_callback(user_data);
        });
}

void FalconKVBridge::Close() {
    if (impl_) {
        impl_->Close();
        impl_.reset();
    }
}

} // namespace falconkv
```

**Bridge 层的关键设计决策**：

1. **只使用原始类型**：`BridgeBuffer` 只包含 `void*` 和 `uint32_t`，不依赖任何 C++ 内部类型
2. **Bridge 持有实例**：`FalconKVClientImpl` 的生命周期完全由 Bridge 管理
3. **回调参数化**：Fire-and-Forget 的 Python 回调通过函数指针传入，Bridge 不直接操作 Python API
4. **GIL 无关**：Bridge 层代码不包含任何 Python.h 引用，可以在无 GIL 的环境中编译和测试

### 4.3 Layer 2: C Extension 模块（`_pyfalconkv_internal`）

使用 Raw Python C API（参考 FalconFS 的 `_pyfalconfs_internal`），不使用 pybind11。

#### 4.3.1 Handle 管理

与 FalconFS 使用全局初始化不同，FalconKV 支持多实例场景（不同配置的 Client），因此使用 handle 模式：

```cpp
// python/_pyfalconkv_internal/_pyfalconkv_internal.cpp

#include <Python.h>
#include <atomic>
#include <condition_variable>
#include <future>
#include <mutex>
#include <queue>
#include <string>
#include <vector>

#include "falconkv_bridge.h"

// =================== Handle 管理 ===================

// 使用 void* 作为 opaque handle，避免在 C Extension 中暴露 C++ 类型
// 全局 handle 注册表，用于在模块级函数间传递 Bridge 实例
static std::unordered_map<void*, std::unique_ptr<falconkv::FalconKVBridge>> g_bridges;
static std::mutex g_bridges_mutex;
static std::atomic<uintptr_t> g_next_handle{1};

// 将 Bridge 指针转换为 opaque handle
static PyObject* bridge_to_handle(falconkv::FalconKVBridge* bridge) {
    void* handle = reinterpret_cast<void*>(g_next_handle.fetch_add(1));
    std::lock_guard<std::mutex> lock(g_bridges_mutex);
    g_bridges[handle] = std::unique_ptr<falconkv::FalconKVBridge>(bridge);
    return PyLong_FromVoidPtr(handle);
}

// 从 opaque handle 取回 Bridge 指针（不转移所有权）
static falconkv::FalconKVBridge* handle_to_bridge(PyObject* handle_obj) {
    void* handle = PyLong_AsVoidPtr(handle_obj);
    if (handle == nullptr && PyErr_Occurred()) return nullptr;
    std::lock_guard<std::mutex> lock(g_bridges_mutex);
    auto it = g_bridges.find(handle);
    if (it == g_bridges.end()) {
        PyErr_SetString(PyExc_RuntimeError, "Invalid FalconKV handle");
        return nullptr;
    }
    return it->second.get();
}
```

#### 4.3.2 同步操作

```cpp
// =================== Init ===================

static PyObject* PyWrapper_Init(PyObject* self, PyObject* args) {
    const char* config_json = nullptr;
    Py_ssize_t cache_capacity = 100000;
    if (!PyArg_ParseTuple(args, "s|n", &config_json, &cache_capacity))
        return nullptr;

    falconkv::FalconKVBridge::Config config;
    config.config_file = config_json;
    config.cache_capacity = static_cast<size_t>(cache_capacity);

    falconkv::FalconKVBridge* bridge = nullptr;
    try {
        bridge = new falconkv::FalconKVBridge(config);
    } catch (const std::exception& e) {
        PyErr_SetString(PyExc_RuntimeError, e.what());
        return nullptr;
    }

    return bridge_to_handle(bridge);
}

// =================== BatchExistSync ===================

static PyObject* PyWrapper_BatchExistSync(PyObject* self, PyObject* args) {
    PyObject* handle_obj = nullptr;
    PyObject* keys_list = nullptr;
    if (!PyArg_ParseTuple(args, "OO", &handle_obj, &keys_list))
        return nullptr;

    auto* bridge = handle_to_bridge(handle_obj);
    if (!bridge) return nullptr;

    // Python list[str] → vector<string>
    Py_ssize_t n = PyList_Size(keys_list);
    std::vector<std::string> keys;
    keys.reserve(n);
    for (Py_ssize_t i = 0; i < n; ++i) {
        PyObject* item = PyList_GetItem(keys_list, i);  // borrowed ref
        const char* key = PyUnicode_AsUTF8(item);
        if (!key) return nullptr;
        keys.emplace_back(key);
    }

    // 释放 GIL 执行 C++ 操作
    int result = 0;
    {
        Py_BEGIN_ALLOW_THREADS
        result = bridge->BatchExistSync(keys);
        Py_END_ALLOW_THREADS
    }

    return PyLong_FromLong(result);
}

// =================== BatchPutSync ===================

static PyObject* PyWrapper_BatchPutSync(PyObject* self, PyObject* args) {
    PyObject* handle_obj = nullptr;
    PyObject* keys_list = nullptr;
    PyObject* ptrs_list = nullptr;
    PyObject* sizes_list = nullptr;
    if (!PyArg_ParseTuple(args, "OOOO", &handle_obj, &keys_list, &ptrs_list, &sizes_list))
        return nullptr;

    auto* bridge = handle_to_bridge(handle_obj);
    if (!bridge) return nullptr;

    Py_ssize_t n = PyList_Size(keys_list);

    // 解析 keys
    std::vector<std::string> keys;
    keys.reserve(n);
    for (Py_ssize_t i = 0; i < n; ++i) {
        const char* key = PyUnicode_AsUTF8(PyList_GetItem(keys_list, i));
        if (!key) return nullptr;
        keys.emplace_back(key);
    }

    // 解析 data_ptrs (List[int]，每个是 tensor.data_ptr() 的整数值)
    std::vector<falconkv::BridgeBuffer> buffers;
    buffers.reserve(n);
    for (Py_ssize_t i = 0; i < n; ++i) {
        void* ptr = PyLong_AsVoidPtr(PyList_GetItem(ptrs_list, i));
        if (PyErr_Occurred()) return nullptr;
        long size = PyLong_AsLong(PyList_GetItem(sizes_list, i));
        if (PyErr_Occurred()) return nullptr;
        buffers.push_back({ptr, static_cast<uint32_t>(size)});
    }

    // 释放 GIL
    int result = 0;
    {
        Py_BEGIN_ALLOW_THREADS
        result = bridge->BatchPutSync(keys, buffers);
        Py_END_ALLOW_THREADS
    }

    if (result != 0) {
        PyErr_SetString(PyExc_RuntimeError, "BatchPutSync failed");
        return nullptr;
    }
    Py_RETURN_NONE;
}

// =================== BatchGetSync ===================

static PyObject* PyWrapper_BatchGetSync(PyObject* self, PyObject* args) {
    PyObject* handle_obj = nullptr;
    PyObject* keys_list = nullptr;
    PyObject* ptrs_list = nullptr;
    PyObject* sizes_list = nullptr;
    if (!PyArg_ParseTuple(args, "OOOO", &handle_obj, &keys_list, &ptrs_list, &sizes_list))
        return nullptr;

    auto* bridge = handle_to_bridge(handle_obj);
    if (!bridge) return nullptr;

    Py_ssize_t n = PyList_Size(keys_list);

    std::vector<std::string> keys;
    std::vector<falconkv::BridgeBuffer> buffers;
    keys.reserve(n);
    buffers.reserve(n);

    for (Py_ssize_t i = 0; i < n; ++i) {
        const char* key = PyUnicode_AsUTF8(PyList_GetItem(keys_list, i));
        if (!key) return nullptr;
        keys.emplace_back(key);

        void* ptr = PyLong_AsVoidPtr(PyList_GetItem(ptrs_list, i));
        if (PyErr_Occurred()) return nullptr;
        long size = PyLong_AsLong(PyList_GetItem(sizes_list, i));
        if (PyErr_Occurred()) return nullptr;
        buffers.push_back({ptr, static_cast<uint32_t>(size)});
    }

    // 释放 GIL
    std::vector<int32_t> results;
    {
        Py_BEGIN_ALLOW_THREADS
        results = bridge->BatchGetSync(keys, buffers);
        Py_END_ALLOW_THREADS
    }

    // 返回 List[int]
    PyObject* result_list = PyList_New(results.size());
    for (size_t i = 0; i < results.size(); ++i) {
        PyList_SetItem(result_list, i, PyLong_FromLong(results[i]));
    }
    return result_list;
}
```

#### 4.3.3 异步支持（AsyncState + AsyncTaskThreadPool）

参考 FalconFS 的 `AsyncState` 设计，实现 Python async/await 协议：

```cpp
// =================== Async Task Thread Pool ===================

// 优先级常量：值越小优先级越高
constexpr int TASK_PRIORITY_GET = 0;
constexpr int TASK_PRIORITY_PUT = 10;

class AsyncTaskThreadPool {
private:
    struct TaskItem {
        int priority;
        uint64_t sequence;       // 同优先级内 FIFO
        std::function<void()> task;
        bool operator<(const TaskItem& other) const {
            if (priority != other.priority) return priority > other.priority;
            return sequence > other.sequence;
        }
    };

    std::priority_queue<TaskItem> taskQueue_;
    std::mutex queueMutex_;
    std::condition_variable queueCV_;
    std::vector<std::thread> workers_;
    std::atomic<bool> stop_{false};
    std::atomic<uint64_t> sequenceCounter_{0};

    void workerLoop() {
        while (true) {
            TaskItem item;
            {
                std::unique_lock<std::mutex> lock(queueMutex_);
                queueCV_.wait(lock, [this] {
                    return stop_.load() || !taskQueue_.empty();
                });
                if (stop_.load() && taskQueue_.empty()) return;
                item = std::move(taskQueue_.top());
                taskQueue_.pop();
            }
            item.task();
        }
    }

public:
    explicit AsyncTaskThreadPool(size_t numWorkers) {
        for (size_t i = 0; i < numWorkers; ++i) {
            workers_.emplace_back([this] { workerLoop(); });
        }
    }

    ~AsyncTaskThreadPool() { shutdown(); }

    // 带优先级的异步调度，返回 future 用于 await
    template<class F>
    auto DispatchWithPriority(int priority, F&& f) -> std::future<decltype(f())> {
        using R = decltype(f());
        auto task = std::make_shared<std::packaged_task<R()>>(std::forward<F>(f));
        std::future<R> res = task->get_future();
        {
            std::lock_guard<std::mutex> lock(queueMutex_);
            taskQueue_.push(TaskItem{
                priority,
                sequenceCounter_.fetch_add(1),
                [task]() { (*task)(); }
            });
        }
        queueCV_.notify_one();
        return res;
    }

    // Fire-and-Forget：不返回 future
    template<class F>
    void DispatchFireAndForget(int priority, F&& f) {
        {
            std::lock_guard<std::mutex> lock(queueMutex_);
            taskQueue_.push(TaskItem{
                priority,
                sequenceCounter_.fetch_add(1),
                std::forward<F>(f)
            });
        }
        queueCV_.notify_one();
    }

    void shutdown() {
        bool expected = false;
        if (!stop_.compare_exchange_strong(expected, true)) return;
        queueCV_.notify_all();
        for (auto& w : workers_) {
            if (w.joinable()) w.join();
        }
        workers_.clear();
    }
};

// 模块级线程池单例
static std::mutex g_pool_mutex;
static std::shared_ptr<AsyncTaskThreadPool> g_pool;

static AsyncTaskThreadPool* GetPool() {
    std::lock_guard<std::mutex> lock(g_pool_mutex);
    if (!g_pool) {
        const char* env = std::getenv("FALCONKV_ASYNC_WORKERS");
        size_t n = env ? std::stoul(env) : 8;
        g_pool = std::make_shared<AsyncTaskThreadPool>(n);
    }
    return g_pool.get();
}

// =================== AsyncState (Python awaitable) ===================

struct AsyncState {
    PyObject_HEAD
    std::future<std::unique_ptr<falconkv::BridgeBuffer>> future;
};

// AsyncState 支持的异步结果类型
struct AsyncExistResult {
    int hit_count;
};

static PyTypeObject AsyncStateType;  // 前向声明，模块初始化时注册

static PyObject* AsyncState_new(PyTypeObject* type, PyObject*, PyObject*) {
    return type->tp_alloc(type, 0);
}

static void AsyncState_dealloc(AsyncState* self) {
    Py_TYPE(self)->tp_free((PyObject*)self);
}

static PyObject* AsyncState_iter(PyObject* self) {
    Py_INCREF(self);
    return self;
}

static PyObject* AsyncState_iternext(PyObject* self) {
    AsyncState* state = (AsyncState*)self;
    if (state->future.wait_for(std::chrono::seconds(0)) != std::future_status::ready)
        Py_RETURN_NONE;  // 未就绪，Python event loop 会再次 poll

    auto result = state->future.get();
    // 将结果封装为 Python 对象并通过 StopIteration 返回
    // （具体类型取决于操作，此处以 BatchExist 为例）
    PyObject* py_result = PyLong_FromLong(reinterpret_cast<intptr_t>(result.get()));
    PyErr_SetObject(PyExc_StopIteration, py_result);
    Py_DECREF(py_result);
    return nullptr;
}

static PyObject* AsyncState_await(PyObject* self) {
    Py_INCREF(self);
    return self;
}

static PyAsyncMethods AsyncState_as_async = {
    .am_await = AsyncState_await,
    .am_aiter = AsyncState_iter,
    .am_anext = AsyncState_iternext
};
```

#### 4.3.4 Fire-and-Forget Put

这是最复杂的操作：C++ 后台线程完成后需要安全地调用 Python 的 `ref_count_down`。

```cpp
// Fire-and-Forget Put 回调数据
struct FireAndForgetData {
    PyObject** py_objs;      // Python MemoryObj 对象数组
    size_t count;            // 数组长度
};

// C++ 后台线程完成后的回调（Bridge 层调用）
// 此函数在无 GIL 的线程中执行，需要先获取 GIL
static void fire_and_forget_callback(void* user_data) {
    FireAndForgetData* data = static_cast<FireAndForgetData*>(user_data);

    PyGILState_STATE gstate = PyGILState_Ensure();
    for (size_t i = 0; i < data->count; ++i) {
        // 调用 MemoryObj.ref_count_down() 释放内存池引用
        PyObject* r = PyObject_CallMethod(data->py_objs[i], "ref_count_down", nullptr);
        Py_XDECREF(r);
        Py_DECREF(data->py_objs[i]);  // 释放 INCREF 的引用
    }
    PyGILState_Release(gstate);

    delete[] data->py_objs;
    delete data;
}

static PyObject* PyWrapper_FireAndForgetPut(PyObject* self, PyObject* args) {
    PyObject* handle_obj = nullptr;
    PyObject* keys_list = nullptr;
    PyObject* ptrs_list = nullptr;
    PyObject* sizes_list = nullptr;
    PyObject* memobjs_list = nullptr;  // List[MemoryObj]
    if (!PyArg_ParseTuple(args, "OOOOO", &handle_obj, &keys_list,
                          &ptrs_list, &sizes_list, &memobjs_list))
        return nullptr;

    auto* bridge = handle_to_bridge(handle_obj);
    if (!bridge) return nullptr;

    Py_ssize_t n = PyList_Size(keys_list);

    // 解析 keys 和 buffers
    std::vector<std::string> keys;
    std::vector<falconkv::BridgeBuffer> buffers;
    keys.reserve(n);
    buffers.reserve(n);

    for (Py_ssize_t i = 0; i < n; ++i) {
        const char* key = PyUnicode_AsUTF8(PyList_GetItem(keys_list, i));
        if (!key) return nullptr;
        keys.emplace_back(key);

        void* ptr = PyLong_AsVoidPtr(PyList_GetItem(ptrs_list, i));
        if (PyErr_Occurred()) return nullptr;
        long size = PyLong_AsLong(PyList_GetItem(sizes_list, i));
        if (PyErr_Occurred()) return nullptr;
        buffers.push_back({ptr, static_cast<uint32_t>(size)});
    }

    // 准备 Fire-and-Forget 回调数据
    auto* cb_data = new FireAndForgetData();
    cb_data->count = static_cast<size_t>(n);
    cb_data->py_objs = new PyObject*[n];

    for (Py_ssize_t i = 0; i < n; ++i) {
        PyObject* memobj = PyList_GetItem(memobjs_list, i);  // borrowed
        Py_INCREF(memobj);  // 防止 Python GC 在 C++ 写入完成前回收
        cb_data->py_objs[i] = memobj;
    }

    // 提交到 Bridge，C++ 后台线程完成后调用 fire_and_forget_callback
    bridge->FireAndForgetPut(keys, buffers,
                              fire_and_forget_callback, cb_data);

    Py_RETURN_NONE;
}

// =================== Close ===================

static PyObject* PyWrapper_Close(PyObject* self, PyObject* args) {
    PyObject* handle_obj = nullptr;
    if (!PyArg_ParseTuple(args, "O", &handle_obj))
        return nullptr;

    void* handle = PyLong_AsVoidPtr(handle_obj);
    if (PyErr_Occurred()) return nullptr;

    {
        std::lock_guard<std::mutex> lock(g_bridges_mutex);
        auto it = g_bridges.find(handle);
        if (it != g_bridges.end()) {
            // 先释放 GIL，因为 Close 可能等待后台任务完成
            Py_BEGIN_ALLOW_THREADS
            it->second->Close();
            g_bridges.erase(it);
            Py_END_ALLOW_THREADS
        }
    }

    Py_RETURN_NONE;
}
```

#### 4.3.5 模块定义

```cpp
static PyMethodDef PyFalconKVInternalMethods[] = {
    {"Init",             PyWrapper_Init,             METH_VARARGS,
     "Initialize FalconKV bridge.\n"
     "Args: config_file (str), [cache_capacity (int)]\n"
     "Returns: handle (int)"},
    {"BatchExistSync",   PyWrapper_BatchExistSync,   METH_VARARGS,
     "Batch check key existence.\n"
     "Args: handle, keys (List[str])\n"
     "Returns: int (consecutive hit count)"},
    {"BatchPutSync",     PyWrapper_BatchPutSync,     METH_VARARGS,
     "Batch put key-value pairs.\n"
     "Args: handle, keys (List[str]), data_ptrs (List[int]), sizes (List[int])\n"
     "Returns: None"},
    {"BatchGetSync",     PyWrapper_BatchGetSync,     METH_VARARGS,
     "Batch get key-value pairs into pre-allocated buffers.\n"
     "Args: handle, keys (List[str]), data_ptrs (List[int]), sizes (List[int])\n"
     "Returns: List[int] (bytes read per key)"},
    {"FireAndForgetPut", PyWrapper_FireAndForgetPut,  METH_VARARGS,
     "Fire-and-forget batch put with Python ref management.\n"
     "Args: handle, keys, data_ptrs, sizes, memobjs\n"
     "Returns: None"},
    {"Close",            PyWrapper_Close,            METH_VARARGS,
     "Close FalconKV bridge and release resources.\n"
     "Args: handle\n"
     "Returns: None"},
    {nullptr, nullptr, 0, nullptr}
};

static struct PyModuleDef PyFalconKVInternalModule = {
    PyModuleDef_HEAD_INIT,
    "_pyfalconkv_internal",
    "FalconKV C extension module (internal, use pyfalconkv.Client instead)",
    -1,
    PyFalconKVInternalMethods,
    nullptr, nullptr, nullptr, nullptr
};

extern "C" PyMODINIT_FUNC PyInit__pyfalconkv_internal(void) {
    // 注册 AsyncState 类型
    if (PyType_Ready(&AsyncStateType) < 0) return nullptr;

    PyObject* module = PyModule_Create(&PyFalconKVInternalModule);
    if (!module) return nullptr;

    Py_INCREF(&AsyncStateType);
    PyModule_AddObject(module, "AsyncState", (PyObject*)&AsyncStateType);

    return module;
}
```

### 4.4 Layer 3: Python 封装层（`pyfalconkv`）

#### 4.4.1 `pyfalconkv/client.py` — Client 类

封装 C Extension 的模块级函数为面向对象的接口：

```python
"""
pyfalconkv.Client - High-level Python wrapper for FalconKV C Extension.

This module wraps the _pyfalconkv_internal C extension module's
module-level functions into a clean Client class. This is the primary
interface for Python users and for the LMCache FalconKVConnector.
"""

try:
    import _pyfalconkv_internal as _internal
except ImportError:
    _internal = None


class Client:
    """FalconKV Python Client.

    Wraps the C Extension module-level functions into an OO interface.
    Internally manages a handle that identifies the FalconKVBridge instance.
    """

    def __init__(self, config_json: str, cache_capacity: int = 100000):
        if _internal is None:
            raise RuntimeError(
                "_pyfalconkv_internal C extension not found. "
                "Build with: ./build.sh build --with-python"
            )
        self._handle = _internal.Init(config_json, cache_capacity)

    def batch_exist_sync(self, keys: list[str]) -> int:
        """Batch check key existence.

        Args:
            keys: List of key strings.

        Returns:
            Consecutive hit count (prefix match semantics).
        """
        return _internal.BatchExistSync(self._handle, keys)

    def batch_put_sync(self, keys: list[str],
                       data_ptrs: list[int],
                       sizes: list[int]) -> None:
        """Batch put key-value pairs (synchronous).

        Args:
            keys: List of key strings.
            data_ptrs: List of buffer memory addresses (from tensor.data_ptr()).
            sizes: List of buffer sizes in bytes.
        """
        _internal.BatchPutSync(self._handle, keys, data_ptrs, sizes)

    def batch_get_sync(self, keys: list[str],
                       data_ptrs: list[int],
                       sizes: list[int]) -> list[int]:
        """Batch get key-value pairs into pre-allocated buffers.

        Args:
            keys: List of key strings.
            data_ptrs: List of target buffer addresses.
            sizes: List of buffer sizes.

        Returns:
            List of bytes actually read per key. <= 0 means failure.
        """
        return _internal.BatchGetSync(self._handle, keys, data_ptrs, sizes)

    def fire_and_forget_put(self, keys: list[str],
                            data_ptrs: list[int],
                            sizes: list[int],
                            memobjs: list) -> None:
        """Fire-and-forget batch put with automatic Python ref management.

        The C++ backend will call memobj.ref_count_down() on each object
        after the write completes. Callers must call obj.ref_count_up()
        before passing objects here.

        Args:
            keys: List of key strings.
            data_ptrs: List of buffer addresses.
            sizes: List of buffer sizes.
            memobjs: List of Python MemoryObj instances.
        """
        _internal.FireAndForgetPut(self._handle, keys, data_ptrs, sizes, memobjs)

    def close(self) -> None:
        """Close the client and release all resources."""
        if self._handle is not None:
            _internal.Close(self._handle)
            self._handle = None

    def __del__(self):
        self.close()
```

#### 4.4.2 `pyfalconkv/__init__.py`

```python
"""
FalconKV Python bindings.
"""

from pyfalconkv.client import Client

__all__ = ["Client"]
__version__ = "0.1.0"
```

### 4.5 构建系统（CMakeLists.txt）

```cmake
# python/_pyfalconkv_internal/CMakeLists.txt

# ==================== FalconKVBridge 库 ====================
add_library(FalconKVBridge STATIC
    ${CMAKE_CURRENT_SOURCE_DIR}/falconkv_bridge.cpp
)
target_include_directories(FalconKVBridge PUBLIC
    ${CMAKE_SOURCE_DIR}/src          # FalconKVClientImpl 等头文件
    ${Python3_INCLUDE_DIRS}
)
target_link_libraries(FalconKVBridge PUBLIC
    falconkv_client                  # src/client/ 的 CMake target
)

# ==================== _pyfalconkv_internal 模块 ====================
add_library(_pyfalconkv_internal MODULE
    ${CMAKE_CURRENT_SOURCE_DIR}/_pyfalconkv_internal.cpp
)
set_target_properties(_pyfalconkv_internal PROPERTIES
    PREFIX ""
    SUFFIX ".so"
)
# WARNING: NEVER use -static-libstdc++ or -static-libgcc for Python modules!
# Python modules must dynamically link libstdc++ to avoid ABI conflicts with
# the Python interpreter.
target_include_directories(_pyfalconkv_internal PUBLIC
    ${Python3_INCLUDE_DIRS}
    ${CMAKE_CURRENT_SOURCE_DIR}      # falconkv_bridge.h
)
target_link_libraries(_pyfalconkv_internal PUBLIC
    FalconKVBridge
    ${Python3_LIBRARIES}
    dl
)

install(TARGETS _pyfalconkv_internal
    LIBRARY DESTINATION ${CMAKE_SOURCE_DIR}/python/pyfalconkv
)

# ==================== throw_hook (可选调试工具) ====================
add_library(throw_hook MODULE
    ${CMAKE_CURRENT_SOURCE_DIR}/throw_hook.cpp
)
set_target_properties(throw_hook PROPERTIES
    PREFIX ""
    SUFFIX ".so"
)
install(TARGETS throw_hook
    LIBRARY DESTINATION ${CMAKE_SOURCE_DIR}/python/pyfalconkv
)
```

### 4.6 数据流总结

以 `BatchPut`（同步模式）为例，完整的数据流：

```
LMCache (Python)
    │
    │  keys: [CacheEngineKey], memory_objs: [MemoryObj]
    ▼
FalconKVConnector.batched_put()       ← Python, 持有 GIL
    │  提取 tensor.data_ptr() 和 size
    │  keys_strs = [k.to_string() for k in keys]
    ▼
asyncio.to_thread(Client.batch_put_sync)
    │  释放 GIL
    ▼
Client.batch_put_sync()               ← Python, 持有 GIL
    │  调用 _internal.BatchPutSync(handle, keys, ptrs, sizes)
    ▼
PyWrapper_BatchPutSync()              ← C Extension, 持有 GIL
    │  解析 Python 参数 → vector<string> + vector<BridgeBuffer>
    │  Py_BEGIN_ALLOW_THREADS (释放 GIL)
    ▼
FalconKVBridge.BatchPutSync()         ← C++, 无 GIL
    │  BridgeBuffer → BufferInfo 适配
    ▼
FalconKVClientImpl.BatchPutSync()     ← C++ Core, 无 GIL
    │  → local_store_->BatchPut()
    │  → MetaSyncClient 异步同步元数据
    ▼
FalconKVBridge 返回                   ← C++, 无 GIL
    ▼
PyWrapper_BatchPutSync()              ← C Extension, 恢复 GIL
    │  Py_END_ALLOW_THREADS
    ▼
Client.batch_put_sync() 返回          ← Python
    ▼
FalconKVConnector.batched_put()       ← Python, 持有 GIL
    │  memory_obj.ref_count_down()
    ▼
LMCache 继续
```

## 5. LMCache 适配实现

### 5.1 FalconKVConnector

```python
# python/pyfalconkv/connector.py
"""
FalconKVConnector - LMCache RemoteConnector implementation using FalconKV.

Uses pyfalconkv.Client (which wraps the C Extension) for all operations.
The Client internally delegates to FalconKVBridge → FalconKVClientImpl.
"""

import asyncio
from typing import List, Optional

try:
    from pyfalconkv.client import Client as FalconKVClient
except ImportError:
    FalconKVClient = None

from lmcache.logging import init_logger
from lmcache.v1.storage_backend.connector.base_connector import RemoteConnector

logger = init_logger(__name__)


class FalconKVConnector(RemoteConnector):
    """LMCache RemoteConnector implementation using FalconKV."""

    def __init__(
        self,
        config_json: str,
        cache_capacity: int = 100000,
        async_batch_size: int = 16,
        fire_and_forget: bool = True,
        loop=None,
        local_cpu_backend=None,
    ):
        super().__init__(local_cpu_backend.config, local_cpu_backend.metadata)

        if FalconKVClient is None:
            raise RuntimeError(
                "pyfalconkv module not found. "
                "Build with: ./build.sh build --with-python"
            )

        self.loop = loop
        self.local_cpu_backend = local_cpu_backend
        self._fire_and_forget = fire_and_forget

        # 通过 Client → FalconKVBridge → FalconKVClientImpl 初始化
        self._client = FalconKVClient(config_json, cache_capacity)

        # 异步并发控制
        self._async_semaphore = asyncio.Semaphore(async_batch_size)

    # ============ Batch operation support ============

    def support_batched_get(self) -> bool:
        return True

    def support_batched_put(self) -> bool:
        return True

    def support_batched_contains(self) -> bool:
        return True

    def support_batched_async_contains(self) -> bool:
        return True

    def support_batched_get_non_blocking(self) -> bool:
        return True

    # ============ Batch operations (core path) ============

    async def batched_get(self, keys) -> List[Optional[object]]:
        # 1. 为每个 key 分配 MemoryObj
        memory_objs = []
        data_ptrs = []
        sizes = []
        for key in keys:
            obj = self.local_cpu_backend.allocate(
                self.meta_shapes, self.meta_dtypes, self.meta_fmt
            )
            memory_objs.append(obj)
            tensor = obj.tensor
            data_ptrs.append(tensor.data_ptr())
            sizes.append(tensor.numel() * tensor.element_size())

        # 2. 通过 Client → C Extension → Bridge → C++ Core 批量读取
        key_strs = [k.to_string() for k in keys]
        bytes_read_list = await asyncio.to_thread(
            self._client.batch_get_sync, key_strs, data_ptrs, sizes
        )

        # 3. 处理结果
        results = []
        for i, n_read in enumerate(bytes_read_list):
            if n_read <= 0:
                memory_objs[i].ref_count_down()
                results.append(None)
            elif n_read < sizes[i]:
                results.append(self.reshape_partial_chunk(memory_objs[i], n_read))
            else:
                results.append(memory_objs[i])
        return results

    async def batched_put(self, keys, memory_objs):
        key_strs = [k.to_string() for k in keys]
        data_ptrs = []
        sizes = []

        for obj in memory_objs:
            tensor = obj.tensor
            data_ptrs.append(tensor.data_ptr())
            sizes.append(tensor.numel() * tensor.element_size())

        if self._fire_and_forget:
            # Fire-and-Forget: 增加引用计数，C++ 后台线程完成后释放
            for obj in memory_objs:
                obj.ref_count_up()
            self._client.fire_and_forget_put(key_strs, data_ptrs, sizes, memory_objs)
        else:
            # 同步等待写入完成
            await asyncio.to_thread(
                self._client.batch_put_sync, key_strs, data_ptrs, sizes
            )
            for obj in memory_objs:
                obj.ref_count_down()

    def batched_contains(self, keys) -> int:
        key_strs = [k.to_string() for k in keys]
        return self._client.batch_exist_sync(key_strs)

    async def batched_async_contains(self, lookup_id, keys, pin=False) -> int:
        return self.batched_contains(keys)

    async def batched_get_non_blocking(self, lookup_id, keys):
        results = await self.batched_get(keys)
        for i, r in enumerate(results):
            if r is None:
                for j in range(i + 1, len(results)):
                    if results[j] is not None:
                        results[j].ref_count_down()
                return results[:i]
        return results

    # ============ Single key operations (fallback) ============

    async def exists(self, key) -> bool:
        return self._client.batch_exist_sync([key.to_string()]) > 0

    def exists_sync(self, key) -> bool:
        return self._client.batch_exist_sync([key.to_string()]) > 0

    async def get(self, key):
        results = await self.batched_get([key])
        return results[0] if results else None

    async def put(self, key, memory_obj):
        await self.batched_put([key], [memory_obj])

    # ============ Lifecycle ============

    async def close(self):
        if self._client:
            self._client.close()
            self._client = None

    async def list(self) -> List[str]:
        raise NotImplementedError("FalconKV does not support list operation")
```

**与旧设计的差异**：

| 维度 | 旧设计（直接 pybind11 绑定） | 新设计（Bridge + C Extension） |
|------|------------------------------|-------------------------------|
| Client 引用 | `self._impl = FalconKVClientImpl(...)` | `self._client = FalconKVClient(...)` |
| Put 调用 | `self._impl.batch_put_sync(key_strs, buffer_infos)` | `self._client.batch_put_sync(key_strs, data_ptrs, sizes)` |
| Buffer 传递 | `List[Tuple[int, int]]` (Python tuple) | `List[int], List[int]` (分离的 ptrs 和 sizes) |
| Fire-and-Forget | `self._impl.fire_and_forget_put(...)` | `self._client.fire_and_forget_put(...)` |
| LMCache 关心的接口 | 完全相同 | 完全相同 |

### 5.2 FalconKVAdapter

```python
# python/pyfalconkv/adapter.py
"""
FalconKVConnectorAdapter - Adapter for creating FalconKVConnector instances.

Auto-discovered by LMCache ConnectorManager via URL schema "falconkv://".
"""

from urllib.parse import parse_qs, urlparse

from lmcache.logging import init_logger
from lmcache.v1.storage_backend.connector import (
    ConnectorAdapter,
    ConnectorContext,
)
from lmcache.v1.storage_backend.connector.base_connector import RemoteConnector

from .connector import FalconKVConnector

logger = init_logger(__name__)


class FalconKVConnectorAdapter(ConnectorAdapter):
    """FalconKV adapter for LMCache auto-discovery.

    URL format: falconkv://localhost:0

    Configuration via extra_config:
    - falconkv_config_json: FalconKV config JSON string (required)
    - falconkv_cache_capacity: Key descriptor cache capacity (default: 100000)
    - falconkv_async_batch_size: Max concurrent async operations (default: 16)
    - falconkv_fire_and_forget: Enable fire-and-forget put mode (default: True)
    """

    def __init__(self) -> None:
        super().__init__("falconkv://")

    def can_parse(self, url: str) -> bool:
        return url.startswith(self.schema)

    def create_connector(self, context: ConnectorContext) -> RemoteConnector:
        config = context.config
        assert config is not None, "Config is required for FalconKVConnector"

        extra_config = config.extra_config if config.extra_config is not None else {}

        config_json = str(extra_config.get("falconkv_config_json", ""))
        cache_capacity = int(extra_config.get("falconkv_cache_capacity", 100000))
        async_batch_size = int(extra_config.get("falconkv_async_batch_size", 16))
        fire_and_forget = bool(extra_config.get("falconkv_fire_and_forget", True))

        parsed = urlparse(context.url)
        if parsed.query:
            params = parse_qs(parsed.query)
            if "config" in params:
                config_json = params["config"][0]

        logger.info(
            f"Creating FalconKVConnector with config: {config_json[:50]}..."
        )

        return FalconKVConnector(
            config_json=config_json,
            cache_capacity=cache_capacity,
            async_batch_size=async_batch_size,
            fire_and_forget=fire_and_forget,
            loop=context.loop,
            local_cpu_backend=context.local_cpu_backend,
        )
```

## 6. 配置项

Client 配置分为四部分：**Python 层 extra_config**（LMCache 传入）、**common 区共享配置**（自动传播）、**client 区专属配置**、**transfer 区 RPC 配置**。

### 6.1 Python 层 LMCache extra_config

| 配置项 | 类型 | 默认值 | 说明 |
|--------|------|--------|------|
| `falconkv_config_file` | str | 必填 | FalconKV JSON 配置文件路径 |
| `falconkv_cache_capacity` | int | 100000 | Key 描述缓存容量 |
| `falconkv_async_batch_size` | int | 16 | 异步操作并发度 |
| `falconkv_fire_and_forget` | bool | True | 是否启用 Fire-and-Forget Put |

### 6.2 JSON common 区（共享，自动传播）

以下字段位于 `common` section，由 `ConfigLoader` 自动传播到 `client` 配置，`FalconKVClientImpl` 构造时从 `cfg.common.*` 读取：

| JSON 字段 (`common.*`) | 默认值 | 说明 |
|------------------------|--------|------|
| `scheduler_enabled` | true | 是否启用 IO Scheduler 通信 |
| `scheduler_uds_path` | /tmp/falconkv_scheduler.sock | Scheduler UDS 路径 |
| `scheduler_rpc_timeout_us` | 2000 | Scheduler RPC 超时（微秒） |
| `max_consecutive_failures` | 3 | SchedulerProxy 连续失败阈值 |
| `reconnect_interval_sec` | 2 | bypass 后重连探测间隔 |
| `node_id` | 0 | 本节点 ID |
| `meta_addr` | localhost:18900 | Meta 服务器地址 |

### 6.3 JSON client 区（专属）

| JSON 字段 (`client.*`) | 默认值 | 说明 |
|------------------------|--------|------|
| `cache_capacity` | 100000 | Key 描述缓存容量 |
| `async_batch_size` | 16 | 异步操作并发度 |
| `fire_and_forget` | true | 是否启用 Fire-and-Forget Put |

### 6.4 JSON transfer 区

| JSON 字段 (`transfer.*`) | 默认值 | 说明 |
|------------------------|--------|------|
| `protocol` | brpc | 通信协议 |
| `meta_pool_size` | 4 | Meta RPC 连接池大小 |
| `store_pool_size` | 4 | Store RPC 连接池大小 |
| `rpc_timeout_ms` | 5000 | RPC 超时（毫秒） |
| `connect_timeout_ms` | 3000 | 连接超时（毫秒） |
| `max_retry` | 3 | RPC 最大重试次数 |
| `max_body_size_mb` | 512 | BRPC max body size（MB），BatchRead 超过此限制时自动拆分子批次 |

## 7. IO Scheduler 集成

### 7.1 SchedulerProxy 在 Client 中的位置

SchedulerProxy 嵌入在 `FalconKVClientImpl` 中，作为 IO 路径的一个可选拦截点。Client 在向 Scheduler 申请 IO 时，需根据亲和层级映射到对应的 IOChannel：

**配置传播**：`FalconKVClientImpl` 构造时从加载的 JSON 配置中读取 `common.scheduler_enabled` 和 `common.scheduler_uds_path`，优先级高于 `FalconKVClientImpl::Config` 的默认值。这使得通过 JSON 配置文件可以控制 scheduler 行为，无需修改代码。

```
IOChannel 映射规则:
┌──────────┬──────────────────────────────────────────────────────┐
│ 亲和层级 │ IOChannel 映射                                       │
├──────────┼──────────────────────────────────────────────────────┤
│ Level 0  │ LOCAL_SSD_READ (读) / LOCAL_SSD_WRITE (写)          │
│ Level 1  │ LOCAL_SSD_READ (读)                                 │
│ Level 2  │ NET_TX_READ (读)                                    │
│          │ + remote_node_addr (远程 Store 节点地址)             │
└──────────┴──────────────────────────────────────────────────────┘
```

```
BatchPut/BatchGet 调用链:

  BatchPut:
    BatchPut
         │
         ▼
    SchedulerProxy::RequestIO() ← IO 前拦截（申请调度）
         │
         ▼
    local_store_->BatchPut() ← 写入始终走本地 Store
         │
         ▼
    SchedulerProxy::ReportIOCompletion() ← IO 后上报（异步）

  BatchGet:
    BatchGet
         │
         ▼
    KeyDescCache 查询  ← 不经过 Scheduler
         │
         ▼
    SchedulerProxy::RequestIO() ← IO 前拦截（申请调度）
         │
         │  按 access_type 映射 IOChannel:
         │  - L0/L1 读 → IOChannel::LOCAL_SSD_READ
         │  - L2 读    → IOChannel::NET_TX_READ + remote_node_addr
         │
         ▼
    实际 IO 操作 (local_store_ / NodeLocalAccessor / StoreRpcClient)
         │
         ▼
    SchedulerProxy::ReportIOCompletion() ← IO 后上报（异步）
```

### 7.2 Bypass 行为

当 Scheduler 不可用时，Client 必须快速 bypass：

| 阶段 | Bypass 行为 | 对业务的影响 |
|------|------------|-------------|
| IO 申请 | 直接跳过 RequestIO，立即执行 IO | 无，仅丢失统计 |
| IO 完成上报 | 跳过 ReportIOCompletion | 无，仅丢失统计 |
| 恢复检测 | 后台线程定期探测 Scheduler | 自动恢复，无需重启 |

详细设计参见 [falconkv_scheduler_design.md](falconkv_scheduler_design.md) 第 6 节。

## 8. 错误处理

| 场景 | 处理策略 |
|------|----------|
| Meta 不可达 | Connect 失败不阻止启动；BatchExist/BatchLookup 断连早返空结果；后台每 5 秒自动重连 |
| Store 写入失败 | local_store_ 内部重试，空间不足时触发驱逐 |
| Key 不存在 | 返回 None / hit_count=0 |
| Buffer 分配失败 | 返回 None，记录 warning |
| Client 退出 | local_store_ 关闭，停止 Meta 重连线程，Meta 通过心跳超时自动回收 |
| Scheduler 不可达 | 快速 bypass，后台自动重连 |
| Meta 运行中故障 | RPC 失败标记断连，后续请求跳过 RPC 直接返回空结果，后台自动重连 |

## 9. 线程模型

```
┌───────────────────────────────────────────────────┐
│ Python Main Thread (GIL)                          │
│  - LMCache 调度 BatchExist/Get/Put                │
│  - MemoryObj 分配/释放                             │
│  - buffer 指针传递 (data_ptr, size)               │
└───────────────────────┬───────────────────────────┘
                        │ C Extension: Py_BEGIN_ALLOW_THREADS
                        ▼
┌───────────────────────────────────────────────────┐
│ C++ Worker Thread Pool                            │
│  - FalconKVBridge 适配层                          │
│  - local_store_->BatchPut() (本地写入)             │
│  - local_store_->BatchRead() (本地读取)             │
│  - NodeLocalAccessor (同节点读取)                  │
│  - RPC 到远端 Store (brpc)                         │
│  - RPC 到 Meta (brpc)                             │
│  - KeyDescCache 操作                              │
└───────────────────────┬───────────────────────────┘
                        │ brpc async RPC
                        ▼
┌───────────────────────────────────────────────────┐
│ brpc I/O Threads                                  │
│  - 网络收发                                       │
│  - 序列化/反序列化                                 │
│  - 连接管理                                       │
└───────────────────────────────────────────────────┘

Meta 重连线程（后台守护线程）:
  - MetaRpcClient::ReconnectLoop — 每 5 秒检查连接状态
  - 断连时尝试 TryConnect()，成功后标记 connected_
  - FalconKVClientImpl 构造时启动，Close() 时停止
```

Fire-and-Forget 场景下额外的后台线程：
- 从线程池取出任务
- 执行 `local_store_->BatchPut()` 写入
- 完成后通过 Bridge 的回调机制触发 C Extension 的 `fire_and_forget_callback`
- `fire_and_forget_callback` 通过 `PyGILState_Ensure` 获取 GIL，调用 `MemoryObj.ref_count_down()`，然后 `PyGILState_Release` 释放 GIL
