# FalconKV Store 模块设计文档

## 1. 模块概述

Store 模块是 FalconKV 的数据持久化层，负责将 KV Cache 数据块高效地存储到 SSD 上，并提供低时延的读写能力。核心设计要点：

- **自管理空间**：Store 内部维护 SlotAllocator 进行空间分配与回收，独立管理驱逐策略
- **Key-aware API**：提供基于 key 的 Put/Get/Contains/Remove 接口，调用方无需感知 offset
- **元数据同步**：通过 MetaSyncClient 将元数据变更异步同步到 Meta 服务器
- **大文件 + DirectIO**：预分配大文件，使用 DirectIO 直接操作 SSD，绕过 OS 页缓存
- **固定块管理**：KVCache 场景数据块大小固定，简化空间管理
- **同进程部署**：Store 与 Client 在同一进程空间，支持本地高速读写
- **零拷贝传输**：远程数据通过 brpc attachment 零拷贝传输
- **io_uring + 固定线程池**：批量 IO 走 io_uring 异步引擎，不可用时自动降级为固定线程池
- **三阶段 BatchPut**：批量分配 → 并行 IO 写入 → 批量元数据更新，最大化 IO 并行度

### 1.1 设计参考

- **FalconFS FalconStore**：基于 SSD 的存储引擎，使用 DirectIO 读写，文件按 `{inodeId}-large` 格式命名
- **Linux DirectIO**：绕过页缓存，直接操作块设备，需要内存对齐
- **Slot Allocator**：固定槽位的空间管理算法，适配 KVCache 固定块大小场景

### 1.2 核心约束

| 约束项 | 说明 |
|--------|------|
| 存储介质 | NVMe SSD |
| IO 模式 | DirectIO (O_DIRECT) |
| 文件系统 | XFS / ext4 (支持大文件) |
| 对齐要求 | 512 字节对齐（SSD 扇区大小） |
| 数据块大小 | 固定（由 LMCache chunk_size 决定） |
| 部署模式 | 与 Client 同进程或独立进程 |

## 2. 架构设计

### 2.1 整体架构

```
┌──────────────────────────────────────────────────────────────────────┐
│                    FalconKV Store Process                             │
│                                                                       │
│  ┌───────────────────────────────────────────────────────────────┐   │
│  │ StoreServer (封装启动/停止)                                    │   │
│  │  ┌─────────────────────────────────────────────────────────┐  │   │
│  │  │ StoreServiceImpl (FalconKVStoreService RPC 处理)         │  │   │
│  │  │  - Write / Read / BatchWrite / BatchRead / Ping          │  │   │
│  │  │  - 委托给 FalconKVStore 执行实际 IO                      │  │   │
│  │  └───────────────────────────┬─────────────────────────────┘  │   │
│  │                              │ brpc::Server                    │   │
│  └──────────────────────────────┼────────────────────────────────┘   │
│                              │                                       │
│  ┌───────────────────────────┼───────────────────────────────────┐   │
│  │ Store Engine              │                                    │   │
│  │                           ▼                                    │   │
│  │  ┌─────────────────────────────────────────────────────────┐  │   │
│  │  │ FalconKVStore                                            │  │   │
│  │  │  - Key-aware API: Put/Get/Contains/Remove/BatchPut/...   │  │   │
│  │  │  - DirectIO: Write/Read (含对齐处理)                     │  │   │
│  │  │  - BatchWrite/BatchRead (io_uring / 线程池并行)         │  │   │
│  │  │  - BatchPut (三阶段流水线: 分配→写入→元数据)            │  │   │
│  │  │                                                          │  │   │
│  │  │  内部组件:                                                │  │   │
│  │  │  ┌──────────────────┐  ┌──────────────────────────────┐ │  │   │
│  │  │  │ SlotAllocator   │  │ StoreMetaIndex               │ │  │   │
│  │  │  │ (空间分配/回收)  │  │ (本地 key→offset 索引)       │ │  │   │
│  │  │  └──────────────────┘  └──────────────────────────────┘ │  │   │
│  │  │  ┌────────────────────────────────────────────────────┐ │  │   │
│  │  │  │ MetaSyncClient (异步同步元数据到 Meta 服务器)       │ │  │   │
│  │  │  │  - SyncCommit() / SyncRemove()                     │ │  │   │
│  │  │  └────────────────────────────────────────────────────┘ │  │   │
│  │  │  ┌────────────────────────────────────────────────────┐ │  │   │
│  │  │  │ PendingEvictQueue (驱逐宽限期队列, 5s)             │ │  │   │
│  │  │  │  - 先删 Meta 再延迟回收本地空间                     │ │  │   │
│  │  │  └────────────────────────────────────────────────────┘ │  │   │
│  │  │  ┌────────────────────────────────────────────────────┐ │  │   │
│  │  │  │ IO Engine (io_uring 或线程池)                       │ │  │   │
│  │  │  │  - IOUringEngine: liburing 批量 SQE/CQE            │ │  │   │
│  │  │  │  - IOThreadPool:   固定线程池降级路径              │ │  │   │
│  │  │  └────────────────────────────────────────────────────┘ │  │   │
│  │  └─────────────────────────────────────────────────────────┘  │   │
│  └───────────────────────────────────────────────────────────────┘   │
│                              │                                       │
│  ┌───────────────────────────┼───────────────────────────────────┐   │
│  │ Storage Layer             │                                    │   │
│  │                           ▼                                    │   │
│  │  ┌─────────────────────────────────────────────────────────┐  │   │
│  │  │ SSD: /data/falconkv/kv_data_{store_id}                   │  │   │
│  │  └─────────────────────────────────────────────────────────┘  │   │
│  └───────────────────────────────────────────────────────────────┘   │
└──────────────────────────────────────────────────────────────────────┘
```

### 2.2 核心类设计

```cpp
class FalconKVStore {
public:
    struct Config {
        std::string ssd_path;          // SSD 数据根目录
        uint32_t store_id = 0;         // Store 唯一标识（从配置文件读取）
        uint32_t node_id = 0;          // 节点 ID
        uint64_t capacity_bytes = 500ULL * 1024 * 1024 * 1024; // 总容量
        uint32_t page_size = 4096;      // 页大小
        uint32_t io_threads = 4;        // IO 工作线程数
        uint32_t write_queue_size = 1024; // 写队列大小
        bool disable_mtime = true;      // 禁用文件修改时间更新
        std::string scheduler_uds_path; // IO Scheduler UDS 路径（可选）
        bool scheduler_enabled = true;  // 是否启用 Scheduler 上报
        int scheduler_rpc_timeout_us = 2000; // Scheduler RPC 超时（微秒）
        int max_consecutive_failures = 3;    // Scheduler 连续失败阈值
        int reconnect_interval_sec = 2;     // Scheduler 重连间隔
        std::string store_rpc_host = "127.0.0.1"; // Store RPC 主机地址
        uint32_t listen_port = 8901;         // Store RPC 监听端口
        std::string meta_addr;          // Meta 服务器地址
        uint32_t evict_grace_period_ms = 5000;    // 驱逐宽限期
        uint32_t evict_check_interval_sec = 60;   // 驱逐检查间隔
        double evict_high_watermark = 0.85;       // 驱逐高水位
        double evict_low_watermark = 0.70;        // 驱逐低水位
        uint64_t evict_cold_threshold_ms = 300000; // 冷数据阈值
        bool io_uring_enabled = true;   // 是否启用 io_uring
        bool direct_io_enabled = true;  // 是否启用 DirectIO
        uint32_t io_uring_queue_depth = 128; // io_uring 队列深度
        uint32_t slot_size_bytes = 0;  // SlotAllocator 槽位大小（0 = 默认 2MB）

        static Config FromStoreConfig(const StoreConfig& sc);
    };

    FalconKVStore(const Config& config);
    ~FalconKVStore();

    // 初始化：创建数据文件、初始化 SlotAllocator、向 Meta 注册
    Status Init();

    // ===== Key-aware API (供 Client 调用) =====

    // 写入单个 key
    StorePutResult Put(const std::string& key, const void* data, uint32_t size);

    // 读取单个 key
    StoreGetResult Get(const std::string& key, void* buffer, uint32_t size);

    // 查询 key 是否存在
    bool Contains(const std::string& key);

    // 删除 key
    Status Remove(const std::string& key);

    // 批量写入（三阶段流水线：分配→并行IO写入→元数据更新）
    std::vector<StorePutResult> BatchPut(const std::vector<std::string>& keys,
                                  const std::vector<const void*>& data_ptrs,
                                  const std::vector<uint32_t>& sizes);

    // 批量查询存在性
    void BatchContains(const std::vector<std::string>& keys,
                       std::vector<StoreKeyRecord>& hits,
                       std::vector<std::string>& misses);

    // ===== Offset-based API (供 RPC 服务使用) =====

    // 按偏移写入数据（单次 IO，走 pread/pwrite）
    Status Write(uint64_t offset, const void* data, uint32_t size);

    // 按偏移读取数据（单次 IO，走 pread/pwrite）
    Status Read(uint64_t offset, void* buffer, uint32_t size);

    // 批量写入（走 io_uring 或线程池并行）
    Status BatchWrite(const std::vector<WriteItem>& items);

    // 批量读取（走 io_uring 或线程池并行）
    Status BatchRead(const std::vector<ReadItem>& items);

    // 关闭
    void Close();

private:
    Config config_;
    int data_fd_;                       // 数据文件 fd (O_DIRECT)
    uint32_t store_id_;                 // 从 config_.store_id 初始化
    std::string data_file_;             // 完整数据文件路径（含 store_id）

    // IO 引擎（二选一，io_uring 优先，线程池降级）
    std::unique_ptr<IOThreadPool> io_pool_;         // 固定线程池（始终存在）
    std::unique_ptr<IOUringEngine> io_uring_engine_; // io_uring 引擎（可选）
    bool io_uring_enabled_ = false;                 // 运行时是否启用 io_uring

    // 空间管理
    std::unique_ptr<SlotAllocator> allocator_;

    // 本地元数据索引
    std::unique_ptr<StoreMetaIndex> meta_index_;

    // Meta 同步客户端
    std::unique_ptr<MetaSyncClient> meta_sync_client_;

    // 驱逐宽限期队列（先删 Meta 元数据，延迟 5s 后回收本地空间）
    std::unique_ptr<PendingEvictQueue> pending_evict_queue_;

    // 后台驱逐管理器
    std::unique_ptr<EvictManager> evict_manager_;

    // IO Scheduler 代理（可选）
    std::unique_ptr<SchedulerProxy> scheduler_proxy_;
};
```

### 2.3 StoreMetaIndex（本地元数据索引 + LRU 双向链表）

Store 维护本地 key→offset 的 hash 索引，支持快速本地查找，避免每次操作都查询 Meta。同时通过嵌入式双向链表实现 LRU 排序，为驱逐提供 O(k) 的候选选取。

```cpp
struct StoreKeyRecord {
    std::string key;
    uint64_t offset = 0;
    uint32_t size = 0;
    uint32_t alloc_size = 0;
    int stat = 0;             // 状态：0=allocated, 1=committed
    uint64_t access_time_ms = 0;

    // LRU 双向链表指针（由 StoreMetaIndex 管理）
    StoreKeyRecord* lru_prev = nullptr;
    StoreKeyRecord* lru_next = nullptr;
};

struct EvictCandidates {
    std::vector<StoreKeyRecord> entries;
    uint32_t total_alloc_size = 0;
};

class StoreMetaIndex {
public:
    // 插入/更新 key 记录（stat==1 时自动链入 LRU MRU 端）
    void Put(const std::string& key, const StoreKeyRecord& record);

    // 查找已提交的 key（自动 touch：移动到 LRU MRU 端 + 更新 access_time_ms）
    std::optional<StoreKeyRecord> Get(const std::string& key);

    // 批量查询存在性（每个命中 key 自动 touch）
    void BatchContains(const std::vector<std::string>& keys,
                       std::vector<StoreKeyRecord>& hits,
                       std::vector<std::string>& misses);

    // 标记 key 为已提交（stat 0→1），链入 LRU 表
    bool Commit(const std::string& key);

    // 删除 key 记录（从 hash + LRU 链表摘除）
    std::optional<StoreKeyRecord> Remove(const std::string& key);

    // 更新访问时间并移动到 MRU 端
    void Touch(const std::string& key);

    // 获取已提交 entry 数量（O(1)）
    size_t CommittedCount() const;

    // 获取所有已提交记录（用于 Meta 重连后全量重新同步）
    std::vector<StoreKeyRecord> GetAllCommittedEntries() const;

    // 从 LRU 尾部取最多 max_count 个候选（O(k)，供 EvictManager 调用）
    std::vector<StoreKeyRecord> GetLRUCandidates(size_t max_count) const;

    // 按大小累积候选，直到 total_alloc_size >= needed_bytes
    EvictCandidates GetLRUCandidatesBySize(uint32_t needed_bytes,
                                            size_t max_count) const;

private:
    void LruRemove(StoreKeyRecord* rec);
    void LruPushFront(StoreKeyRecord* rec);  // push to MRU end

    mutable std::mutex mutex_;
    // unique_ptr 保证指针跨 rehash 稳定
    std::unordered_map<std::string, std::unique_ptr<StoreKeyRecord>> index_;

    // 哨兵节点：lru_next = MRU 端，lru_prev = LRU 端
    StoreKeyRecord sentinel_;
    size_t committed_count_ = 0;
};
```

**LRU 链表方向**：
- `sentinel_.lru_next` → MRU 端（最近访问，驱逐优先级最低）
- `sentinel_.lru_prev` → LRU 端（最久未访问，驱逐优先级最高）
- 只有 `stat == 1`（已提交）的 entry 才会被链入链表
- 空链表时：`sentinel_.lru_next == sentinel_.lru_prev == &sentinel_`

### 2.4 MetaSyncClient（Meta 同步客户端 + 断连容错）

Store 通过 MetaSyncClient 将元数据变更异步同步到 Meta 服务器。支持 Meta 不可用时的断连容错和自动重连。

```cpp
class MetaSyncClient {
public:
    // 连接 Meta 服务器（失败不阻止 Store 启动）
    Status Connect(const std::string& meta_addr);

    // 设置注册信息（用于重连后重新注册）
    void SetStoreInfo(uint32_t store_id, const std::string& data_file,
                      uint64_t capacity_bytes, uint32_t chunk_size);

    // 设置本地索引指针（用于全量重同步，不拥有所有权）
    void SetMetaIndex(StoreMetaIndex* meta_index);

    // 启动后台重连循环（每 interval_sec 秒检查）
    void StartReconnectLoop(int interval_sec);

    // 停止后台重连循环
    void StopReconnectLoop();

    // 同步已提交的 key 到 Meta（断连时跳过）
    Status SyncCommit(uint32_t store_id,
                       const std::vector<StoreKeyRecord>& records);

    // 同步已删除/驱逐的 key 到 Meta（断连时跳过）
    Status SyncRemove(uint32_t store_id,
                       const std::vector<std::string>& keys);

    // 向 Meta 注册 Store（断连时跳过）
    Status RegisterStore(uint32_t store_id, const std::string& data_file,
                         uint64_t capacity_bytes, uint32_t chunk_size);

    bool connected() const;

private:
    Status TryConnect();       // 内部重连
    void FullResync();         // 重连后全量重新同步
    void ReconnectLoop(int interval_sec);  // 后台重连线程

    std::unique_ptr<brpc::Channel> channel_;
    std::unique_ptr<FalconKVMetaService_Stub> stub_;
    std::atomic<bool> connected_{false};
    std::string meta_addr_;

    // Store 注册信息
    uint32_t store_id_ = 0;
    std::string data_file_;
    uint64_t capacity_bytes_ = 0;
    uint32_t chunk_size_ = 0;
    StoreMetaIndex* meta_index_ = nullptr;  // not owned

    // 重连线程
    std::thread reconnect_thread_;
    std::atomic<bool> reconnect_running_{false};
    std::mutex reconnect_mutex_;
    std::condition_variable reconnect_cv_;
};
```

**断连容错行为**：

| 行为 | 说明 |
|------|------|
| Connect 失败 | 不阻止 Store 启动，日志记录错误 |
| SyncCommit/SyncRemove/RegisterStore 断连 | 检测到 `!connected_` 跳过（返回 OK） |
| RPC 失败 | `cntl.Failed()` 时设置 `connected_ = false`，触发后台重连 |
| 后台重连循环 | 每 5 秒尝试 TryConnect()，成功后执行 FullResync() |
| FullResync | RegisterStore + 分批（256 条/批）SyncCommit 全量 committed key |

**Store.Init() 初始化顺序**：

```cpp
FalconKVStore::Init():
    // ...
    meta_sync_client_ = std::make_unique<MetaSyncClient>();
    meta_sync_client_->Connect(addr);            // 失败也 OK
    meta_sync_client_->SetStoreInfo(store_id_, data_file_, capacity, chunk_size);
    meta_sync_client_->SetMetaIndex(meta_index_.get());
    meta_sync_client_->StartReconnectLoop(5);    // 后台重连
    // ...

FalconKVStore::Close():
    // ...
    if (meta_sync_client_) {
        meta_sync_client_->StopReconnectLoop();  // 先停止重连线程
    }
    meta_sync_client_.reset();
    // ...
```

### 2.5 PendingEvictQueue（驱逐宽限期队列）

驱逐数据时，Store 必须先通知 Meta 删除元数据，然后将待驱逐记录移入 `PendingEvictQueue`，等待宽限期（5 秒）后再回收本地空间。这确保了：

1. **Meta 元数据先行删除**：其他 Client 的 BatchExist 查询不会再命中这些 key
2. **宽限期兜底**：在 Meta 删除到本地回收之间的窗口期，已经获取到旧 key 描述的 Client 仍可成功读取
3. **避免读取不可预知数据**：宽限期过后空间才真正回收，杜绝读到被覆盖的部分数据

```cpp
class PendingEvictQueue {
public:
    struct EvictEntry {
        std::string key;
        uint64_t offset;           // SlotAllocator 中的偏移
        uint32_t alloc_size;       // 实际分配大小（用于 Free）
        uint64_t enqueue_time_ms;  // 入队时间（毫秒时间戳）
    };

    PendingEvictQueue(uint64_t grace_period_ms, SlotAllocator* allocator);

    // 将待驱逐记录加入队列（元数据已通过 SyncRemove 在 Meta 中删除）
    void Enqueue(const std::string& key, uint64_t offset, uint32_t alloc_size);

    // 后台线程：每 100ms 检查超过宽限期的记录，回收空间
    void Start();
    void Stop();

    size_t Size() const;

    // 立即回收已过宽限期的 entry（供 Put 路径 Level 1/2 调用）
    size_t FlushExpired();

    // 无视宽限期立即回收全部 entry（供 Put 路径 Level 3 紧急调用）
    size_t FlushAllForced();

private:
    void EvictLoop();
    void FlushAll();  // Stop/析构时使用

    uint64_t grace_period_ms_;            // 宽限期（默认 5000ms）
    SlotAllocator* allocator_;           // not owned

    mutable std::mutex mutex_;
    std::vector<EvictEntry> entries_;     // 待回收 entry 列表

    std::thread thread_;
    std::atomic<bool> running_{false};
};
```

> **注意**：`PendingEvictQueue` 不持有 `StoreMetaIndex` 指针——索引移除由 `EvictManager` 在 `Enqueue` 之前完成。

### 2.6 SlotAllocator（固定槽位空间管理）

SlotAllocator 位于 `src/common/`，被 Store 和 Meta（测试用）共同使用。每个 Store 实例维护一个独立的 SlotAllocator。

```cpp
class SlotAllocator {
public:
    // slot_size = 0 means use default 2MB
    SlotAllocator(uint64_t total_bytes, uint32_t slot_size = 0);
    ~SlotAllocator() = default;

    // Allocate one slot. Returns byte offset, or -1 on failure.
    // If `out_alloc_size` is non-null, receives slot_size_.
    // `size` is accepted for API compatibility; if size > slot_size_, returns -1.
    int64_t Alloc(uint32_t size, uint32_t* out_alloc_size = nullptr);

    // Free a previously allocated slot.
    void Free(int64_t offset, uint32_t alloc_size);

    double GetUsageRatio() const;
    uint64_t GetTotalBytes() const;
    uint64_t GetUsedBytes() const;
    uint32_t GetPageSize() const { return slot_size_; }
    uint32_t ComputeAllocSize(uint32_t size) const;

private:
    uint32_t slot_size_;
    uint32_t total_slots_;
    uint32_t used_slots_;
    uint64_t total_bytes_;
    std::vector<uint32_t> free_stack_;  // LIFO stack of free slot indices
    mutable std::mutex mutex_;
};
```

#### 分配流程

```
Alloc(size, out_alloc_size):
  1. lock_guard(mutex_)
  2. 若 size > slot_size_ → 返回 -1
  3. 若 free_stack_ 为空 → 返回 -1（空间不足）
  4. slot_index = free_stack_.back(); free_stack_.pop_back()
  5. used_slots_++
  6. *out_alloc_size = slot_size_
  7. 返回 offset = slot_index * slot_size_
```

#### 回收流程

```
Free(offset, alloc_size):
  1. lock_guard(mutex_)
  2. 验证 offset 对齐：offset % slot_size_ == 0
  3. slot_index = offset / slot_size_
  4. free_stack_.push_back(slot_index)
  5. used_slots_--
```

### 2.6 Store RPC Service 层

Client-Store 分离后，Store 模块通过 RPC Service 层提供远程访问能力。

**StoreServiceImpl** — 服务端 RPC 处理器：

```cpp
class StoreServiceImpl : public FalconKVStoreService {
public:
    explicit StoreServiceImpl(FalconKVStore* store);

    void Write(RpcController*, const WriteRequest*, WriteResponse*, Closure*) override;
    void Read(RpcController*, const ReadRequest*, ReadResponse*, Closure*) override;
    void BatchWrite(RpcController*, const BatchWriteRequest*, BatchWriteResponse*, Closure*) override;
    void BatchRead(RpcController*, const BatchReadRequest*, BatchReadResponse*, Closure*) override;
    void Ping(RpcController*, const PingRequest*, PongResponse*, Closure*) override;

private:
    FalconKVStore* store_;  // not owned
};
```

**StoreServer** — 独立 Store 进程封装：

```cpp
class StoreServer {
public:
    StoreServer(const FalconKVStore::Config& store_config, const std::string& listen_addr);
    Status Start();   // Init Store + AddService + brpc::Server::Start
    void Stop();      // brpc::Server::Stop + Store::Close

    FalconKVStore* GetStore();
    const std::string& ListenAddr() const;

private:
    FalconKVStore store_;
    StoreServiceImpl service_impl_;
    brpc::Server server_;
};
```

**StoreRpcClient** — 客户端 RPC 封装：

```cpp
class StoreRpcClient {
public:
    Status Connect(const std::string& addr,
                   uint64_t max_body_size_bytes = 512ULL * 1024 * 1024);
    bool IsConnected() const;
    Status Read(uint64_t offset, void* buffer, uint32_t size,
                uint32_t client_id = 0,
                const std::string& source_node_addr = "");
    Status BatchRead(const std::vector<uint64_t>& offsets,
                     const std::vector<uint32_t>& sizes,
                     const std::vector<void*>& buffers,
                     std::vector<int32_t>& results);
    Status Ping();

private:
    brpc::Channel channel_;
    std::unique_ptr<FalconKVStoreService_Stub> stub_;
    bool connected_ = false;
    uint64_t max_body_size_bytes_ = 512ULL * 1024 * 1024;
};
```

**StoreRpcClientManager** — 按地址缓存的连接池：

```cpp
class StoreRpcClientManager {
public:
    StoreRpcClient* GetOrCreate(const std::string& addr);
    void CloseAll();
    void SetMaxBodySize(uint64_t max_body_size_bytes);

private:
    std::mutex mutex_;
    std::unordered_map<std::string, std::unique_ptr<StoreRpcClient>> clients_;
    uint64_t max_body_size_bytes_ = 512ULL * 1024 * 1024;
};
```

**NodeLocalAccessor** — 同节点文件直通读写器：

```cpp
class NodeLocalAccessor {
public:
    void RegisterStoreFile(uint32_t store_id, const std::string& data_file);
    Status Write(uint32_t store_id, uint64_t offset, const void* data, uint32_t size);
    Status Read(uint32_t store_id, uint64_t offset, void* buffer, uint32_t size);
    void Close();

private:
    int GetFd(uint32_t store_id);
    std::unordered_map<uint32_t, std::string> store_files_;  // store_id → data_file
    FdCache fd_cache_;  // 内部使用
};
```

**StoreRpcClient::BatchRead 拆分逻辑**：

StoreRpcClient::BatchRead 内部实现了按数据大小的自适应拆分，防止单次 RPC 超过 BRPC max body size 限制：

```
BatchRead(offsets, sizes, buffers, results):
  1. 有效限制 = max_body_size_bytes_ * 90%（留 10% 给 protobuf 开销）
  2. 贪心打包：按累加数据大小将 segments 切分为多个子批次
     - 遍历所有 segment，累加 sizes[i]
     - 当累加大小超过有效限制时，切分为一个子批次
  3. 每个子批次独立发送 RPC，结果通过 original_indices 映射回原 results[]
  4. 单个超大 segment（超过有效限制）仍单独发送，不跳过
  5. 汇总所有子批次的结果到 results[] 后返回
```

## 3. Key-aware API 内部流程

### 3.1 Put 内部流程

```
Store.Put(key, data, size):
1. allocator_->AllocChunk() → offset        // SlotAllocator 分配空间
2. Write(offset, data, size)                 // DirectIO 写入 SSD
3. meta_index_->Put(key, {offset, size, stat=1})  // 更新本地索引
4. meta_sync_client_->SyncCommit(store_id, {record}) // 异步同步到 Meta
```

### 3.2 Get 内部流程

```
Store.Get(key, buffer, buffer_size):
1. meta_index_->Get(key) → StoreKeyRecord    // 查询本地索引
2. Read(record.offset, buffer, record.size)  // DirectIO 从 SSD 读取
3. meta_index_->Touch(key)                   // 更新访问时间
```

### 3.3 BatchPut 内部流程（三阶段流水线）

BatchPut 采用三阶段流水线设计，将空间分配、IO 写入和元数据更新解耦，最大化 IO 并行度：

```
Store.BatchPut(keys, data_ptrs, sizes):

┌─ Phase 1: 批量分配空间（顺序） ─────────────────────────────────┐
│ for each key:                                                     │
│   if key exists in meta_index_ → skip                             │
│   allocator_->Alloc(size) → offset                                │
│   if out of space → 三级驱逐重试（FlushExpired/ForceEvict/FlushAll）│
│   results[i] = {offset, alloc_size, Status::OK()}                 │
└───────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─ Phase 2: 并行 IO 写入 ──────────────────────────────────────────┐
│ collect all successfully allocated (offset, data, size)           │
│ if io_uring_enabled_:                                             │
│   → IOUringEngine::BatchWrite(fd, requests)                      │
│     批量提交 SQE → 内核异步处理 → 收割 CQE                       │
│ else:                                                             │
│   → IOThreadPool::SubmitAndWait(tasks)                            │
│     每个任务调用 Write(offset, data, size)                        │
│ if write failed: free all allocated space, return error           │
└───────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─ Phase 3: 批量元数据更新 + Meta 同步 ────────────────────────────┐
│ for each successfully written key:                                │
│   meta_index_->Put(key, {offset, size, stat=1})                  │
│ meta_sync_client_->SyncCommit(store_id, records[])               │
└───────────────────────────────────────────────────────────────────┘
```

**相比旧逐 key 串行流程的收益**：
- Phase 2 中所有 IO 请求并行下发（io_uring 一次 submit 多个 SQE，或线程池并行 pwrite）
- Phase 1 和 Phase 3 均为 CPU 操作，与 IO 无关，不阻塞 SSD

### 3.4 BatchWrite/BatchRead 内部流程

```
Store.BatchWrite(items):
  if items.empty() → return OK
  if io_uring_enabled_:
    构建 UringIORequest 列表（含对齐信息）
    → IOUringEngine::BatchWrite(data_fd_, requests, page_size)
       内部: 对齐处理(fast/slow path) → 分批提交 SQE → 收割 CQE
    检查所有 IOResult，返回最后一个错误
  else:
    构建 tasks 列表，每个 task 调用 Write(offset, data, size)
    → IOThreadPool::SubmitAndWait(tasks)
    检查所有 Status，返回最后一个错误

Store.BatchRead(items):
  同 BatchWrite 模式，UringIORequest 构建、对齐处理、
  io_uring 批量读或线程池并行 pread
```

### 3.5 BatchContains 内部流程

```
Store.BatchContains(keys, hits, misses):
1. 遍历 keys，查询 meta_index_
2. 存在且 stat=1 的 key → hits
3. 不存在或 stat≠1 的 key → misses
4. 返回 hits.size()
```

### 3.6 驱逐流程（先删 Meta + 宽限期回收）

Store 内部管理驱逐策略，由 `EvictManager` 后台线程和 `PendingEvictQueue` 宽限期队列协同完成。驱逐流程遵循**先删 Meta 元数据 → 移除本地索引 → 宽限期后回收 SSD 空间**的三阶段原则。

#### 3.6.1 驱逐组件关系

```
┌─────────────────────────────────────────────────────────────────────┐
│ FalconKVStore                                                        │
│                                                                      │
│  ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐ │
│  │ EvictManager     │    │ PendingEvictQueue│    │ StoreMetaIndex  │ │
│  │ (后台驱逐线程)   │    │ (宽限期队列)     │    │ (LRU 双向链表)  │ │
│  │                  │    │                  │    │                 │ │
│  │ EvictLoop()     │    │ Enqueue()       │    │ GetLRUCandidates│ │
│  │ ForceEvict()    │───▶│ FlushExpired()  │    │ Get()→Touch    │ │
│  │ TryEvictBatchLRU│    │ FlushAllForced()│    │ Remove()       │ │
│  └────────┬────────┘    └────────┬────────┘    └─────────────────┘ │
│           │                      │                                   │
│           ▼                      ▼                                   │
│  ┌─────────────────┐    ┌─────────────────┐                         │
│  │ MetaSyncClient  │    │ SlotAllocator  │                         │
│  │ SyncRemove()    │    │ Free()          │                         │
│  └─────────────────┘    └─────────────────┘                         │
└─────────────────────────────────────────────────────────────────────┘
```

#### 3.6.2 StoreMetaIndex LRU 实现

`StoreMetaIndex` 内部维护一个基于哨兵节点的双向链表，实现 O(1) 的 LRU touch 和 O(k) 的候选选取：

```cpp
struct StoreKeyRecord {
    std::string key;
    uint64_t offset = 0;
    uint32_t size = 0;
    uint32_t alloc_size = 0;
    int stat = 0;             // 0=allocated, 1=committed
    uint64_t access_time_ms = 0;
    StoreKeyRecord* lru_prev = nullptr;  // LRU 双向链表指针
    StoreKeyRecord* lru_next = nullptr;
};
```

**链表方向**：
- `sentinel_.lru_next` → MRU 端（最近访问）
- `sentinel_.lru_prev` → LRU 端（驱逐候选）
- 只有 `stat == 1`（已提交）的 entry 才会被链入 LRU 链表

**自动 touch 触发点**：

| 操作 | LRU 行为 |
|------|----------|
| `Get(key)` | 自动移动到 MRU 端 + 更新 `access_time_ms` |
| `Put(key, record)` | 若 `record.stat == 1`，移动到 MRU 端 |
| `BatchContains(keys)` | 每个命中 key 自动移动到 MRU 端 |

**候选选取方法**：

```cpp
// 从 LRU 尾部取最多 max_count 个候选（O(k)）
std::vector<StoreKeyRecord> GetLRUCandidates(size_t max_count) const;

// 按大小累积：从 LRU 尾部取候选直到 total_alloc_size >= needed_bytes
EvictCandidates GetLRUCandidatesBySize(uint32_t needed_bytes, size_t max_count) const;
```

#### 3.6.3 EvictManager — 后台驱逐管理器

`EvictManager` 是独立的后台线程，定期检查 SSD 使用率，超过水位线时触发 LRU 驱逐：

```cpp
class EvictManager {
public:
    struct Config {
        uint32_t check_interval_sec = 60;    // 检查间隔
        double high_watermark = 0.85;         // 触发驱逐水位
        double low_watermark = 0.70;          // 驱逐目标水位
        uint32_t store_id = 0;
    };

    // 后台线程：周期性检查并驱逐
    void Start();
    void Stop();

    // 同步强制驱逐（供 Put 路径调用）
    uint32_t ForceEvict(uint32_t needed_bytes);

private:
    void EvictLoop();
    bool TryEvictBatchLRU();      // 每批最多驱逐 16 个 LRU 候选
    bool EvictEntries(const std::vector<StoreKeyRecord>& candidates);
};
```

**后台驱逐循环（EvictLoop）**：

```
EvictLoop (后台线程, 每 check_interval_sec 秒检查):
  while (running):
    sleep(check_interval_sec)
    usage = allocator_->GetUsageRatio()
    if usage <= high_watermark (0.85):
      continue
    LOG: usage exceeds high watermark
    while usage > low_watermark (0.70):
      candidates = meta_index_->GetLRUCandidates(16)  // 从 LRU 尾部取 16 个
      if candidates.empty → break
      EvictEntries(candidates)  // 见下方流程
```

**EvictEntries 核心流程**：

```
EvictEntries(candidates):
  // 1. 先通知 Meta 删除元数据（等待确认）
  keys = [rec.key for rec in candidates]
  meta_sync_client_->SyncRemove(store_id, keys)
  if failed → return false  // Meta 同步失败则跳过本批，保持一致性

  // 2. 从本地索引移除 + 加入宽限期队列
  for rec in candidates:
    meta_index_->Remove(rec.key)               // 从 hash + LRU 链表摘除
    pending_queue_->Enqueue(rec.key, rec.offset, rec.alloc_size)  // 宽限期开始

  return true
```

**ForceEvict — 同步强制驱逐（Put 路径）**：

当 Put/BatchPut 空间不足时调用。与后台驱逐共享 `EvictEntries` 逻辑，但按需累积直到满足 `needed_bytes`：

```
ForceEvict(needed_bytes):
  total_evicted = 0
  for round in [0..4]:                         // 最多 5 轮
    if total_evicted >= needed_bytes → break
    remaining = needed_bytes - total_evicted
    candidates = meta_index_->GetLRUCandidatesBySize(remaining, 64)
    if candidates.empty → break
    SyncRemove → Remove from index → Enqueue   // 同 EvictEntries
    total_evicted += candidates.total_alloc_size
  return total_evicted
```

#### 3.6.4 PendingEvictQueue — 宽限期延迟回收

驱逐的 entry 并不立即回收 SSD 空间，而是进入 `PendingEvictQueue` 等待宽限期（默认 5 秒）后再回收。这确保已经获取到旧 key 描述的其他节点 Client 仍可成功读取。

```cpp
class PendingEvictQueue {
public:
    struct EvictEntry {
        std::string key;
        uint64_t offset;            // SSD 偏移
        uint32_t alloc_size;        // 分配大小
        uint64_t enqueue_time_ms;   // 入队时间
    };

    void Enqueue(const std::string& key, uint64_t offset, uint32_t alloc_size);
    void Start();                   // 启动后台线程（每 100ms 扫描）
    void Stop();

    size_t FlushExpired();          // 回收已过宽限期的 entry
    size_t FlushAllForced();        // 立即回收所有 entry（忽略宽限期）
};
```

**后台回收循环**：

```
EvictLoop (后台线程, 每 100ms 扫描):
  while (running):
    sleep(100ms)
    now = GetWallTimeMs()
    // 分离已过期和未过期的 entry
    for entry in entries_:
      if now - entry.enqueue_time_ms >= grace_period_ms_:
        to_free.push(entry)      // 已过宽限期
      else:
        keep.push(entry)         // 仍在宽限期内
    entries_ = keep
    // 在锁外执行 Free（避免持锁期间做 IO）
    for entry in to_free:
      allocator_->Free(entry.offset, entry.alloc_size)
```

**三种 Flush 模式**：

| 方法 | 调用者 | 行为 |
|------|--------|------|
| `FlushExpired()` | Put 路径 Level 1/2 | 回收已过宽限期的 entry，返回回收字节数 |
| `FlushAllForced()` | Put 路径 Level 3 | 立即回收全部 entry，忽略宽限期 |
| `FlushAll()` | 析构 | Stop 时清理残留 entry |

#### 3.6.5 Put/BatchPut 空间不足时的三级驱逐重试

当 SlotAllocator 分配失败时，Put/BatchPut 按以下三级递进策略尝试回收空间：

```
allocator_->Alloc(size) → offset < 0 (空间不足)

Level 1: pending_evict_queue_->FlushExpired()
         → 回收已过宽限期的 entry
         → allocator_->Alloc(size) 重试

Level 2: evict_manager_->ForceEvict(max(size, capacity/20))
         → 强制驱逐 LRU 冷数据（至少驱逐总容量的 5%）
         → pending_evict_queue_->FlushExpired()
         → allocator_->Alloc(size) 重试

Level 3: pending_evict_queue_->FlushAllForced()
         → 无视宽限期，立即回收全部 pending entry
         → allocator_->Alloc(size) 重试

如果三级均失败 → 返回 Status::NoSpace
```

Level 2 中 `ForceEvict` 至少驱逐 `capacity/20`（5% 总容量），是为了摊薄驱逐成本，避免每次 Put 只回收刚够的少量空间导致频繁触发。

#### 3.6.6 驱逐时序保证

```
时间线:
──────────────────────────────────────────────────────────────────────────▶

T0: EvictManager 选取 LRU 候选（最多 16 个/批）
T1: EvictManager → MetaSyncClient::SyncRemove(keys)     ← Meta 删除元数据
    │                                                     BatchExist 不再返回这些 key
T2: EvictManager → StoreMetaIndex::Remove(key)           ← 从本地索引+LRU链表摘除
T3: EvictManager → PendingEvictQueue::Enqueue(key,off,sz) ← 宽限期开始
    │                                                     数据仍在 SSD，可读
    │   ← 宽限期 (5s) →                                  ← 已获取旧描述的 Client 仍可成功读取
    │
T4: PendingEvictQueue 后台线程                           ← 100ms 扫描发现超期
    → SlotAllocator::Free(offset, alloc_size)           ← SSD 空间正式回收，可被新数据覆盖
```

**设计要点**：
- **Meta 先行删除**：防止新的 Client 获取已驱逐数据的 key 描述
- **本地索引同步移除**：`SyncRemove` 成功后才从 `StoreMetaIndex` 移除，保证一致性
- **Meta 同步失败则中止**：`EvictEntries` 若 `SyncRemove` 失败则跳过本批，不删除本地数据
- **5 秒宽限期**：为已经获取到 key 描述的 Client 提供读取窗口
- **空间延迟回收**：宽限期后才 `SlotAllocator::Free()`，确保读取不会遇到不可预知的数据
- **紧急回收**：Put 路径 Level 3 可无视宽限期强制回收，优先保证写入成功

## 4. 数据文件管理

### 4.1 文件创建与初始化

每个 Store 实例使用独立的数据文件，文件名包含 `store_id` 以避免多 Store 部署在同一 SSD 目录时产生冲突。

```
文件命名规则:
  {ssd_path}/kv_data_{store_id}

示例:
  store_id=0 → /data/falconkv/kv_data_0
  store_id=1 → /data/falconkv/kv_data_1
  store_id=7 → /data/falconkv/kv_data_7

同节点多 Store 共存:
  /data/falconkv/
  ├── kv_data_0    (GPU 0 Store, 500GB)
  ├── kv_data_1    (GPU 1 Store, 500GB)
  ├── ...
  └── kv_data_7    (GPU 7 Store, 500GB)

生产环境 store_id 生成规则:
  store_id = hash(node_id) * max_gpu_per_node + gpu_id
  例: node_id=2, gpu_id=3, max_gpu=8 → store_id = 19
```

```cpp
Status FalconKVStore::InitDataFile() {
    // 文件名拼接 store_id，每个 Store 使用独立数据文件
    data_file_ = fmt::format("{}/kv_data_{}", config_.ssd_path, store_id_);

    // 创建文件
    data_fd_ = open(data_file_.c_str(),
                     O_CREAT | O_RDWR | O_DIRECT,  // DirectIO
                     0644);

    if (data_fd_ < 0) {
        return Status::IOError(
            fmt::format("Failed to create data file: {}", data_file_));
    }

    // 预分配空间（fallocate）
    int ret = fallocate(data_fd_,
                        FALLOC_FL_KEEP_SIZE | FALLOC_FL_PUNCH_HOLE,
                        0,
                        config_.capacity_bytes);

    if (ret != 0) {
        return Status::IOError("Failed to fallocate");
    }

    // 禁用文件访问时间更新
    int flags = fcntl(data_fd_, F_GETFL);
    fcntl(data_fd_, F_SETFL, flags | O_NOATIME);

    // 初始化 SlotAllocator
    allocator_ = std::make_unique<SlotAllocator>(
        config_.capacity_bytes, config_.page_size,
        config_.chunk_size / config_.page_size);

    return Status::OK();
}
```

### 4.2 DirectIO 读写

```cpp
// DirectIO 写入
Status FalconKVStore::Write(uint64_t offset, const void* data, uint32_t size) {
    // DirectIO 要求：
    // 1. buffer 地址按 512 字节对齐
    // 2. offset 按 512 字节对齐
    // 3. size 是 512 字节的整数倍

    // 1. 准备对齐 buffer
    uint32_t aligned_size = AlignUp(size, 512);
    void* aligned_buf = nullptr;

    if (IsAligned(data, 512) && aligned_size == size) {
        // 数据已对齐，直接使用
        aligned_buf = const_cast<void*>(data);
    } else {
        // 需要对齐拷贝
        aligned_buf = AlignedAlloc(512, aligned_size);
        memset(aligned_buf, 0, aligned_size);
        memcpy(aligned_buf, data, size);
    }

    // 2. 执行 pwrite (DirectIO)
    ssize_t bytes_written = pwrite(data_fd_, aligned_buf, aligned_size, offset);

    if (aligned_buf != data) {
        AlignedFree(aligned_buf);
    }

    if (bytes_written != static_cast<ssize_t>(aligned_size)) {
        return Status::IOError("pwrite failed");
    }

    return Status::OK();
}

// DirectIO 读取
Status FalconKVStore::Read(uint64_t offset, void* buffer, uint32_t size) {
    uint32_t aligned_size = AlignUp(size, 512);
    void* read_buf = nullptr;

    // 如果用户 buffer 不对齐，使用对齐的中间 buffer
    if (IsAligned(buffer, 512)) {
        read_buf = buffer;
    } else {
        read_buf = AlignedAlloc(512, aligned_size);
    }

    // 执行 pread (DirectIO)
    ssize_t bytes_read = pread(data_fd_, read_buf, aligned_size, offset);

    if (read_buf != buffer) {
        memcpy(buffer, read_buf, size);
        AlignedFree(read_buf);
    }

    if (bytes_read != static_cast<ssize_t>(aligned_size)) {
        return Status::IOError("pread failed");
    }

    return Status::OK();
}
```

### 4.3 批量读写

```cpp
Status FalconKVStore::BatchWrite(const std::vector<WriteItem>& items) {
    if (items.empty()) return Status::OK();

    if (io_uring_enabled_) {
        // io_uring 主路径：批量提交异步写请求
        std::vector<UringIORequest> reqs(items.size());
        for (size_t i = 0; i < items.size(); ++i) {
            reqs[i].user_offset = items[i].offset;
            reqs[i].user_size = items[i].size;
            reqs[i].user_buffer = const_cast<void*>(items[i].data);
        }
        auto results = io_uring_engine_->BatchWrite(data_fd_, reqs, config_.page_size);
        // 检查结果，返回最后一个错误
        Status last_error = Status::OK();
        for (auto& r : results) {
            if (!r.status.ok()) last_error = r.status;
        }
        return last_error;
    }

    // 降级路径：固定线程池并行 pwrite
    std::vector<std::function<Status()>> tasks;
    for (size_t i = 0; i < items.size(); ++i) {
        tasks.push_back([this, &items, i]() {
            return Write(items[i].offset, items[i].data, items[i].size);
        });
    }
    auto statuses = io_pool_->SubmitAndWait(std::move(tasks));
    Status last_error = Status::OK();
    for (auto& s : statuses) {
        if (!s.ok()) last_error = s;
    }
    return last_error;
}

Status FalconKVStore::BatchRead(const std::vector<ReadItem>& items) {
    // 与 BatchWrite 相同的 io_uring / 线程池双路径
    // io_uring: 批量提交异步读请求，收割 CQE 后将数据拷贝到用户 buffer
    // 线程池: 每个任务调用 Read(offset, buffer, size)
}
```

## 5. 内存对齐管理

### 5.1 对齐分配器

```cpp
class AlignedAllocator {
public:
    // 分配按 alignment 对齐的内存
    static void* Allocate(size_t alignment, size_t size) {
        void* ptr = nullptr;
        int ret = posix_memalign(&ptr, alignment, size);
        if (ret != 0) return nullptr;
        return ptr;
    }

    static void Free(void* ptr) {
        free(ptr);
    }

    // 检查地址是否对齐
    static bool IsAligned(const void* ptr, size_t alignment) {
        return (reinterpret_cast<uintptr_t>(ptr) % alignment) == 0;
    }

    // 向上对齐
    static size_t AlignUp(size_t value, size_t alignment) {
        return (value + alignment - 1) & ~(alignment - 1);
    }
};

// 对齐 buffer 池（避免频繁分配）
class AlignedBufferPool {
public:
    AlignedBufferPool(size_t buffer_size, size_t alignment, size_t pool_size)
        : buffer_size_(buffer_size), alignment_(alignment) {
        for (size_t i = 0; i < pool_size; i++) {
            auto* buf = AlignedAllocator::Allocate(alignment, buffer_size);
            if (buf) free_buffers_.push(buf);
        }
    }

    void* Get() {
        std::lock_guard<std::mutex> lock(mutex_);
        if (free_buffers_.empty()) {
            return AlignedAllocator::Allocate(alignment_, buffer_size_);
        }
        auto* buf = free_buffers_.top();
        free_buffers_.pop();
        return buf;
    }

    void Put(void* buf) {
        std::lock_guard<std::mutex> lock(mutex_);
        free_buffers_.push(buf);
    }

    ~AlignedBufferPool() {
        while (!free_buffers_.empty()) {
            AlignedAllocator::Free(free_buffers_.top());
            free_buffers_.pop();
        }
    }

private:
    size_t buffer_size_;
    size_t alignment_;
    std::stack<void*> free_buffers_;
    std::mutex mutex_;
};
```

## 6. Store 注册与心跳

### 6.1 启动流程

```
Store 启动:
1. 读取配置文件（ssd_path, store_id, capacity, page_size, meta_addr）
2. 初始化 store_id_ = config_.store_id
3. 创建/打开数据文件（O_DIRECT）
   - 文件名: {ssd_path}/kv_data_{store_id}
4. fallocate 预分配空间
5. 初始化对齐缓冲池 AlignedBufferPool
6. 初始化 SlotAllocator
7. 初始化 StoreMetaIndex
8. 初始化 IOThreadPool（固定线程池，始终启动）
   - io_pool_ = make_unique<IOThreadPool>(config_.io_threads)
   - io_pool_->Start()
9. 初始化 IOUringEngine（可选，运行时检测）
   - io_uring_engine_ = make_unique<IOUringEngine>()
   - io_uring_enabled_ = io_uring_engine_->Init({queue_depth}, page_size)
   - 失败时自动降级为线程池模式
10. 初始化 MetaSyncClient:
   a. Connect(meta_addr) — 失败不阻止启动，日志记录错误
   b. SetStoreInfo(store_id, data_file, capacity) — 保存注册信息
   c. SetMetaIndex(meta_index_) — 设置本地索引指针
   d. StartReconnectLoop(5) — 启动后台重连线程
   e. 若初始连接成功，则立即 RegisterStore
11. 通过 MetaSyncClient 向 Meta 发送 StoreRegister RPC:
   - node_host, node_port
   - ssd_path
   - data_file（完整路径，供同节点 Client 直读）
   - capacity_bytes
   - store_id
12. 启动 RPC Server（监听 Store 服务端口）
13. 启动心跳线程（定期向 Meta 发送心跳）
14. 启动驱逐线程（EvictManager，定期检查使用率，触发驱逐）
15. 进入服务状态
```

### 6.2 心跳机制

```cpp
class StoreHeartbeat {
public:
    void Start(uint32_t store_id, const std::string& meta_addr) {
        thread_ = std::thread(&StoreHeartbeat::Loop, this,
                              store_id, meta_addr);
    }

    void Stop() {
        running_ = false;
        if (thread_.joinable()) thread_.join();
    }

private:
    void Loop(uint32_t store_id, const std::string& meta_addr) {
        auto channel = TransferChannelFactory::Create("brpc", config_);

        while (running_) {
            std::this_thread::sleep_for(heartbeat_interval_);

            StoreHeartbeatRequest request;
            request.set_store_id(store_id);
            request.set_used_bytes(allocator_->GetUsedBytes());
            request.set_capacity_bytes(allocator_->GetTotalBytes());

            StoreHeartbeatResponse response;
            auto status = channel->SyncCall(request, &response);

            if (!status.OK()) {
                LOG(WARNING) << "Heartbeat to Meta failed: " << status.ToString();
            }
        }
    }

    std::chrono::seconds heartbeat_interval_{5};
    std::thread thread_;
    std::atomic<bool> running_{true};
};
```

## 7. IO 路径优化

### 7.1 IO 引擎架构

FalconKV Store 使用双引擎架构，运行时自动选择最优路径：

```
                   FalconKVStore
                        |
            io_uring_enabled_ ?
              /                  \
        IOUringEngine         IOThreadPool
        (主路径)               (降级路径)
        |                      |
   liburing SQE/CQE       pwrite/pread
   内核环形缓冲区           固定线程池并发
```

### 7.2 IOUringEngine（io_uring 异步 IO 引擎）

```cpp
struct UringIORequest {
    uint64_t offset;              // IO 偏移（对齐后）
    uint32_t size;                // IO 大小（对齐后）
    void* buffer;                 // IO 缓冲区（对齐后，可能是临时分配）
    uint64_t user_offset;         // 用户原始偏移
    uint32_t user_size;           // 用户原始大小
    void* user_buffer;            // 用户原始缓冲区
    bool needs_aligned_copy;      // 是否需要 slow path 对齐拷贝
};

struct UringIOResult {
    int index;                    // 原始请求下标
    Status status;
};

class IOUringEngine {
public:
    struct Config {
        uint32_t queue_depth = 128;
    };

    bool Init(const Config& config, uint32_t page_size);
    void Close();
    bool available() const;

    // 批量写/读，阻塞直到全部完成
    std::vector<UringIOResult> BatchWrite(int fd,
        std::vector<UringIORequest>& requests, uint32_t page_size);
    std::vector<UringIOResult> BatchRead(int fd,
        std::vector<UringIORequest>& requests, uint32_t page_size);
};
```

**O_DIRECT 对齐处理**：

io_uring + O_DIRECT 要求 offset、size、buffer 三者全部按 page_size 对齐。IOUringEngine 内部实现 fast/slow path：

```
Fast path: offset、size、buffer 全部 page 对齐
  → 直接用 user_buffer 提交 SQE，零拷贝

Slow path: 任一不对齐
  → 分配对齐临时缓冲区
  WRITE: pread 原始页 → memcpy 用户数据 → 提交对齐写 SQE
  READ:  提交对齐读 SQE → 完成后 memcpy 到 user_buffer → 释放临时缓冲区
```

**批量提交与收割**：

```
BatchWrite/BatchRead():
  1. 准备所有请求（对齐处理）
  2. 超过 queue_depth 时分批发提交:
     for each sub-batch:
       for each request: sqe = io_uring_get_sqe(ring) → prep_write/prep_read
       io_uring_submit(ring)
       for each cqe: io_uring_wait_cqe(ring, &cqe) → 检查结果 → cqe_seen
  3. 释放 slow path 临时缓冲区
  4. 返回所有 UringIOResult
```

### 7.3 IOThreadPool（固定线程池降级路径）

```cpp
class IOThreadPool {
public:
    explicit IOThreadPool(uint32_t num_threads);
    void Start();
    void Stop();

    // 提交一批任务，阻塞等待全部完成，返回每个任务的 Status
    std::vector<Status> SubmitAndWait(std::vector<std::function<Status()>> tasks);
};
```

**设计要点**：
- 线程数 = `config_.io_threads`（默认 4），Init 时创建、Close 时销毁
- `SubmitAndWait`：将每个 task 包装为 `packaged_task<Status()>` 入队，逐 future.get() 等待
- 相比旧 `std::async` 方案，消除临时线程创建开销，线程生命周期与 Store 一致

### 7.4 降级策略

| 场景 | 行为 |
|------|------|
| 编译时无 liburing | `FALCONKV_HAS_IOURING=FALSE`，IOUringEngine::Init() 直接返回 false，走 IOThreadPool |
| 编译时有 liburing，运行时 ring 初始化失败 | `io_uring_enabled_=false`，走 IOThreadPool |
| io_uring 正常 | 走 IOUringEngine |

### 7.5 单次 IO vs 批量 IO

| 操作 | IO 路径 | 说明 |
|------|---------|------|
| `Write()` / `Read()` | 单次 `pwrite` / `pread` | 单 IO 不值得 io_uring 开销 |
| `BatchWrite()` / `BatchRead()` | io_uring 或线程池 | 批量异步，最大化并行度 |
| `Put()` / `Get()` | 内部调用 Write/Read | 单 key 操作 |
| `BatchPut()` | 三阶段流水线 + BatchWrite | 批量分配→并行IO→批量元数据 |

### 7.6 写入路径优化

```
标准写入路径:
  RPC 接收 → 反序列化 → BatchWrite → io_uring 线程池并行 pwrite → 回复

优化点:
1. io_uring 批量提交: 一次系统调用提交多个 IO 请求
2. 固定线程池: 消除临时线程创建开销
3. 三阶段 BatchPut: 空间分配与 IO 写入解耦，最大化并行度
4. IO 优先级: 读取优先于写入（避免读取被大量写入阻塞）
```

### 7.7 读取路径优化

```
标准读取路径:
  RPC 接收 → 反序列化 → BatchRead → io_uring 批量异步读 → 序列化 → 回复

优化点:
1. io_uring 批量提交: 一次系统调用提交多个读请求
2. 零拷贝: 使用 brpc attachment 传输大数据，避免 protobuf 序列化
3. 同步单次读: 单 key Get 直接 pread（低延迟，不值得 io_uring 开销）
```

## 8. IO Scheduler 上报

### 8.1 Store 上报职责

Store 节点在接收到**远程 Client** 的 RPC 读写请求时，需要向 IO Scheduler 上报 IO 信息。本地 Client 的 IO 由 Client 自行上报，避免重复统计。

```
上报范围:
┌──────────────────────────────────────────────────────────┐
│ 本地 Client IO                                            │
│  Client 0 → Store 0 (同进程)                              │
│  → Client 0 自己向 Scheduler 上报，Store 不重复上报        │
├──────────────────────────────────────────────────────────┤
│ 远程 Client IO                                            │
│  Client X (远程) → RPC → Store 0                          │
│  → Store 0 向 Scheduler 上报（Store 侧视角的 IO 统计）     │
└──────────────────────────────────────────────────────────┘
```

### 8.2 上报实现

Store 在接收到远程 Client 的 RPC 请求时，使用 NET_RX 通道向 Scheduler 上报 IO 信息。

```
Store 上报 IOChannel 映射:
┌───────────────┬──────────────────────────────────────────────────────┐
│ RPC 请求类型  │ IOChannel 映射                                       │
├───────────────┼──────────────────────────────────────────────────────┤
│ 远程 Read RPC │ NET_RX_READ（网络接收请求 + SSD 读取 + 网络回传）     │
└───────────────┴──────────────────────────────────────────────────────┘
```

```cpp
// Store RPC Handler 中嵌入 Scheduler 上报
void FalconKVStore::HandleRPCRead(
    const ReadRequest& request,
    ReadResponse* response) {

    uint64_t start_ts = GetCurrentTimeNs();

    // 1. 执行实际 DirectIO 读取
    auto status = Read(request.offset(), buffer, request.size());

    uint64_t done_ts = GetCurrentTimeNs();

    // 2. 向 Scheduler 上报（仅远程请求）
    if (scheduler_proxy_ && !scheduler_proxy_->IsBypassMode()
        && IsRemoteRequest(request)) {
        StoreIOReport report;
        report.set_store_id(store_id_);
        report.set_io_channel(NET_RX_READ);
        report.set_source_client_id(request.client_id());
        report.set_io_size(request.size());
        report.set_request_ts_ns(start_ts);
        report.set_done_ts_ns(done_ts);
        report.set_source_node_addr(GetRemoteNodeAddr(request));

        // 异步上报，fire-and-forget，不阻塞 RPC 响应
        scheduler_proxy_->StoreReportIOAsync(report);
    }

    // 3. 填充并返回 RPC 响应
    // ...
}
```

### 8.3 Bypass 行为

Store 端的 Scheduler 上报使用与 Client 端相同的 `SchedulerProxy`，具备相同的 bypass 能力：

| 状态 | Store 行为 |
|------|-----------|
| Scheduler 正常 | IO 完成后异步上报，不阻塞响应 |
| Scheduler 不可达 | 连续 3 次上报失败后标记 bypass，不再尝试 |
| bypass 模式中 | 后台线程定期探测 Scheduler，自动恢复 |

### 8.4 上报对 Store 性能的影响

| 操作 | 额外开销 | 说明 |
|------|----------|------|
| Scheduler 上报 RPC | < 50us | UDS 异步 fire-and-forget |
| bypass 检测 | < 1us | 原子变量读取 |
| bypass 恢复探测 | 无 | 后台独立线程 |

## 9. 文件元数据优化

### 9.1 禁用文件时间更新

```cpp
// 方法 1: 挂载文件系统时使用 noatime 选项
// mount -o noatime /dev/nvme0n1 /data

// 方法 2: 使用 fcntl 设置 O_NOATIME
int fd = open(path, O_RDWR | O_DIRECT);
int flags = fcntl(fd, F_GETFL);
fcntl(fd, F_SETFL, flags | O_NOATIME);

// 方法 3: 使用 XFS 的 inode 属性
struct fsxattr fsx;
ioctl(fd, FS_IOC_FSGETXATTR, &fsx);
fsx.fsx_xflags |= FS_XFLAG_NOATIME;
ioctl(fd, FS_IOC_FSSETXATTR, &fsx);
```

### 9.2 减少文件系统元数据操作

```
1. 单一大文件: 避免创建/删除文件的操作
2. O_DIRECT: 绕过页缓存，减少 inode 操作
3. fallocate: 预分配空间，避免动态扩展的元数据更新
4. FALLOC_FL_KEEP_SIZE: 避免更新文件大小
5. 不使用 fdatasync/fsync: KVCache 场景可接受少量数据丢失
```

## 10. 性能优化

### 10.1 关键优化汇总

| 优化项 | 实现手段 | 预期效果 |
|--------|----------|----------|
| DirectIO | O_DIRECT 标志 | 绕过 OS 页缓存，减少 CPU 开销 |
| io_uring 异步 IO | IOUringEngine + liburing | 批量提交/收割，减少系统调用开销 |
| 固定线程池 | IOThreadPool | 消除临时线程创建开销，降级路径 |
| 内存对齐 | posix_memalign + buffer 池 | 满足 DirectIO 对齐要求 |
| 大文件预分配 | fallocate | 避免运行时文件扩展开销 |
| 禁用时间更新 | noatime + O_NOATIME | 减少文件元数据写入 |
| 三阶段 BatchPut | 批量分配→并行IO→批量元数据 | 最大化 IO 并行度 |
| 零拷贝传输 | brpc attachment | 避免 protobuf 序列化 |
| 对齐 buffer 池 | 预分配对齐内存 | 避免频繁内存分配/释放 |
| O_DIRECT 对齐 fast/slow path | IOUringEngine 内部处理 | 对齐数据零拷贝，非对齐数据自动处理 |

### 10.2 性能基准

| 操作 | 目标延迟 | 说明 |
|------|----------|------|
| Write (2MB, 本地 DirectIO) | < 500us | NVMe SSD 写入 |
| Read (2MB, 本地 DirectIO) | < 300us | NVMe SSD 读取 |
| Write (2MB, 远程) | < 3ms | 网络 + SSD 写入 |
| Read (2MB, 远程) | < 2ms | SSD 读取 + 网络传输 |
| BatchWrite (100 * 2MB) | < 10ms | 并行 DirectIO |

## 11. 配置项

Store 配置分为三部分：**common 区共享配置**（自动传播到所有模块）、**store 区专属配置**和 **transfer 区 RPC 配置**。

### 11.1 common 区（共享，自动传播）

| JSON 字段 (`common.*`) | 默认值 | 说明 |
|------------------------|--------|------|
| `meta_addr` | localhost:18900 | Meta 服务器地址，自动传播到 `store.meta_addr` |
| `node_id` | 0 | 节点 ID，自动传播到 `store.node_id` |
| `scheduler_enabled` | true | 是否启用 Scheduler 上报，自动传播到 `store.scheduler_enabled` |
| `scheduler_uds_path` | /tmp/falconkv_scheduler.sock | Scheduler UDS 路径，自动传播到 `store.scheduler_uds_path` |
| `scheduler_rpc_timeout_us` | 2000 | Scheduler RPC 超时（微秒），自动传播到 `store.scheduler_rpc_timeout_us` |
| `max_consecutive_failures` | 3 | Scheduler 连续失败阈值，自动传播到 `store.max_consecutive_failures` |
| `reconnect_interval_sec` | 2 | Scheduler 重连间隔，自动传播到 `store.reconnect_interval_sec` |
| `log_dir` | "" | glog 日志目录，空则输出到 stderr |

### 11.2 store 区（专属）

| JSON 字段 (`store.*`) | 默认值 | 说明 |
|------------------------|--------|------|
| `ssd_path` | /data/falconkv | SSD 数据根目录 |
| `store_id` | 0 | Store 唯一标识 |
| `capacity_gb` | 5 | Store 容量（GB） |
| `page_size` | 4096 | 页大小 |
| `io_threads` | 4 | IO 工作线程数 |
| `alignment` | 512 | DirectIO 对齐大小 |
| `listen_port` | 8901 | Store RPC 监听端口 |
| `heartbeat_sec` | 5 | 心跳间隔（秒） |
| `store_rpc_host` | 127.0.0.1 | Store RPC 监听主机 |
| `evict_grace_period_ms` | 5000 | 驱逐宽限期（Meta 删除后延迟回收本地空间的时间） |
| `evict_check_interval_sec` | 60 | 驱逐扫描间隔（秒） |
| `evict_high_watermark` | 0.85 | 驱逐触发水位 |
| `evict_low_watermark` | 0.70 | 驱逐目标水位 |
| `evict_cold_threshold_ms` | 300000 | 冷数据判定阈值（毫秒） |
| `io_uring_enabled` | true | 是否启用 io_uring（编译时无 liburing 则自动降级） |
| `direct_io_enabled` | true | 是否启用 O_DIRECT 直接 IO（禁用后使用 buffered IO，适用于 tmpfs 等不支持 O_DIRECT 的文件系统） |
| `io_uring_queue_depth` | 128 | io_uring 队列深度 |
| `slot_size_bytes` | 0 | SlotAllocator 槽位大小（0 = 默认 2MB） |

### 11.3 transfer 区

| JSON 字段 (`transfer.*`) | 默认值 | 说明 |
|------------------------|--------|------|
| `max_body_size_mb` | 512 | BRPC max body size（MB），影响 BatchRead 子批次拆分 |
