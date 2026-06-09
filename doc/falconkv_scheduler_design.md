# FalconKV IO Scheduler 模块设计文档

## 1. 模块概述

IO Scheduler 是 FalconKV 在每个计算节点内部部署的 IO 调度与监控模块，负责：

- **IO 调度**：协调同一节点上多个 Client 和多个 Store 之间的 IO 时序，避免 IO 争抢导致带宽浪费
- **带宽统计**：收集本节点所有 IO 的启动时间、完成时间、数据量，计算实时带宽和时延
- **统计输出**：定期打印节点级吞吐和时延统计信息（按 IO 通道分别输出）

### 1.1 问题背景

在典型的 LLM 推理部署中，每个节点配备 8 个 GPU（Worker），每个 GPU 对应一个独立的推理进程和 FalconKV Client。同时节点上存在多个 Store 实例管理本地 SSD。这 8 个 Client 在同一个推理 step 中会瞬时并发发起大量 IO 请求（BatchPut / BatchGet），直接冲击共享的 SSD 带宽。

```
┌──────────────────── Node 内部（无 Scheduler）────────────────────┐
│                                                                   │
│  GPU0 → Client0 ──┐                                              │
│  GPU1 → Client1 ──┤                                              │
│  GPU2 → Client2 ──┤     同时发起 IO     ┌─────────────┐          │
│  GPU3 → Client3 ──┼───────────────────▶│  SSD (NVMe) │          │
│  GPU4 → Client4 ──┤   带宽争抢/拥塞    │  有限带宽    │          │
│  GPU5 → Client5 ──┤                   └─────────────┘          │
│  GPU6 → Client6 ──┤                                              │
│  GPU7 → Client7 ──┘                                              │
│                                                                   │
│  问题：8 个 Client 同时读写同一 SSD，IO 争抢导致延迟抖动         │
└───────────────────────────────────────────────────────────────────┘
```

### 1.2 设计目标

| 目标 | 说明 |
|------|------|
| IO 可观测 | 收集节点内所有 IO 的带宽、时延、并发度数据，按通道分别统计 |
| 调度介入 | 当前阶段为放通模式（Passthrough），后续可扩展为限流/排队策略 |
| 故障快速感知 | Scheduler 异常退出或 hang 时，Client/Store 快速 bypass |
| 低额外开销 | 调度 RPC 延迟 < 50us，不影响正常 IO 路径 |

### 1.3 核心原则

- **可选非必须**：Scheduler 是可选组件，不影响核心数据路径的正确性
- **快速失败**：任何与 Scheduler 的通信异常都会在微秒级感知并 bypass
- **放通优先**：第一阶段所有调度策略为 passthrough，仅做统计

## 2. 架构设计

### 2.1 节点内架构

```
┌──────────────────────── Node (8 GPU) ──────────────────────────────┐
│                                                                     │
│  ┌─────────┐ ┌─────────┐       ┌─────────┐ ┌─────────┐            │
│  │Client 0 │ │Client 1 │  ...  │Client 6 │ │Client 7 │            │
│  │(GPU 0)  │ │(GPU 1)  │       │(GPU 6)  │ │(GPU 7)  │            │
│  └────┬────┘ └────┬────┘       └────┬────┘ └────┬────┘            │
│       │            │                 │            │                  │
│       │  ①IO申请   │                 │            │                  │
│       │  ④IO完成   │                 │            │                  │
│       │  上报      │                 │            │                  │
│       ▼            ▼                 ▼            ▼                  │
│  ┌────────────────────────────────────────────────────────────┐    │
│  │              IO Scheduler (独立进程)                        │    │
│  │                                                             │    │
│  │  ┌──────────────┐ ┌──────────────┐ ┌──────────────────┐   │    │
│  │  │ IO 请求队列  │ │ 统计引擎     │ │ 健康检查/心跳    │   │    │
│  │  │ (Per-Client) │ │ (BW/Latency) │ │ (Watchdog)       │   │    │
│  │  └──────────────┘ └──────────────┘ └──────────────────┘   │    │
│  │                                                             │    │
│  │  ┌──────────────┐                                          │    │
│  │  │ 调度策略     │                                          │    │
│  │  │ (Passthrough)│                                          │    │
│  │  └──────────────┘                                          │    │
│  └─────────────────────────┬──────────────────────────────────┘    │
│                             │ ②③调度决策                          │
│                             │ (当前: 立即放通)                     │
│       ┌─────────────────────┼──────────────────────┐              │
│       ▼                     ▼                      ▼              │
│  ┌─────────┐          ┌─────────┐            ┌─────────┐          │
│  │Store 0  │          │Store 1  │   ...      │Store N  │          │
│  │(SSD)    │          │(SSD)    │            │(SSD)    │          │
│  └─────────┘          └─────────┘            └─────────┘          │
│       │                     │                      │              │
│       │  ⑤Store上报IO信息   │                      │              │
│       └─────────────────────┴──────────────────────┘              │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

### 2.2 通信模型

Scheduler 与 Client/Store 通过 Unix Domain Socket 通信（避免 TCP 栈开销），使用 brpc 的 UDS 支持：

```
通信方式: Unix Domain Socket (UDS)
  - Client/Store → Scheduler: 请求/上报消息
  - Scheduler → Client/Store: 调度决策/响应

路径: /tmp/falconkv_scheduler_{node_id}.sock
```

选择 UDS 的原因：
1. 本机通信无需 TCP/IP 栈，延迟更低（通常 < 20us）
2. 天然具备进程隔离，Scheduler 独立进程不会 crash Client
3. 文件系统可见性便于服务发现和健康检测

## 3. 通信协议

### 3.1 RPC 接口定义

```protobuf
// falconkv_scheduler.proto

service FalconKVSchedulerService {
    // Client 发起 IO 申请
    rpc RequestIO(IORequest) returns (IOResponse);

    // Client 上报 IO 完成信息
    rpc ReportIOCompletion(IOCompletionReport) returns (IOCompletionAck);

    // Store 上报收到的远程 RPC IO 请求
    rpc StoreReportIO(StoreIOReport) returns (StoreIOAck);

    // 心跳 + 健康检查
    rpc Heartbeat(HeartbeatRequest) returns (HeartbeatResponse);
}

// ========== IO 通道定义 ==========

enum IOChannel {
    LOCAL_SSD_READ  = 0;   // 本地 SSD 读取（Client L0/L1）
    LOCAL_SSD_WRITE = 1;   // 本地 SSD 写入（Client L0，写入始终绑定本地 Store）
    NET_TX_READ     = 2;   // 网络发送读取请求（Client → 远程 Store）
    NET_RX_READ     = 3;   // 网络接收 + 本地 SSD 读取（Store 处理远程读请求）
}

message IORequest {
    uint32 client_id = 1;        // Client 标识（worker_id）
    IOChannel io_channel = 2;    // IO 通道类型
    uint32 store_id = 3;         // 目标 Store
    uint64 io_size = 4;          // IO 数据量（字节）
    uint32 priority = 5;         // 优先级（预留）
    uint64 request_ts_ns = 6;    // 请求时间戳（CLOCK_MONOTONIC ns）
    string remote_node_addr = 7; // 远程节点地址（NET_TX 时为目的地址）
}

message IOResponse {
    int32 status = 1;            // 0=放通, 其他值预留
    uint64 permitted_ts_ns = 2;  // 允许发起 IO 的时间点
                                 //   passthrough 模式下 = request_ts_ns
    uint64 ticket = 3;           // IO 票据（用于完成上报时关联）
}

// ========== IO 完成上报 ==========

message IOCompletionReport {
    uint32 client_id = 1;
    uint64 ticket = 2;           // 对应 IORequest 返回的票据
    uint64 io_start_ts_ns = 3;   // 实际 IO 启动时间
    uint64 io_done_ts_ns = 4;    // IO 完成时间
    uint64 io_size = 5;          // 实际 IO 数据量
    IOChannel io_channel = 6;    // IO 通道类型
    int32 io_status = 7;         // 0=成功, <0=错误码
    uint32 store_id = 8;         // 目标 Store ID
    string remote_node_addr = 9; // 远程节点地址（NET_TX 时）
}

message IOCompletionAck {
    int32 status = 1;            // 0=已记录
}

// ========== Store IO 上报 ==========

message StoreIOReport {
    uint32 store_id = 1;
    IOChannel io_channel = 2;    // NET_RX_READ
    uint32 source_client_id = 3; // 发起 IO 的 Client ID
    uint64 io_size = 4;          // IO 数据量
    uint64 request_ts_ns = 5;    // Store 收到请求的时间
    uint64 done_ts_ns = 6;       // IO 处理完成时间
    string source_node_addr = 7; // 远程 Client 来源节点地址
}

message StoreIOAck {
    int32 status = 1;
}

// ========== 心跳 ==========

message HeartbeatRequest {
    uint32 client_id = 1;        // 0=Scheduler 自检
    uint64 timestamp_ns = 2;
}

message HeartbeatResponse {
    int32 status = 1;
    uint64 timestamp_ns = 2;
}
```

### 3.2 通信时序

#### Client 发起 IO 的完整时序

```
Client                    Scheduler              Store
  │                          │                      │
  │ ① IORequest              │                      │
  │ (store_id, io_size)      │                      │
  │─────────────────────────▶│                      │
  │                          │                      │
  │ ② IOResponse (立即)      │                      │
  │ (permitted_ts=t_now)     │                      │
  │◀─────────────────────────│                      │
  │                          │                      │
  │ ③ 执行 IO                │                      │
  │─────────────────────────────────────────────────▶│
  │◀────────────────────────────────────────────────│
  │                          │                      │
  │ ④ IOCompletionReport     │                      │
  │ (start_ts, done_ts,      │                      │
  │  io_size)                │                      │
  │─────────────────────────▶│                      │
  │                          │── 更新带宽/时延统计   │
  │ ⑤ IOCompletionAck        │                      │
  │◀─────────────────────────│                      │
```

#### Store 上报远程读 IO

```
Remote Client            Store                 Scheduler
  │                       │                       │
  │── Read RPC ──────────▶│                       │
  │                       │                       │
  │◀── Read Response ─────│                       │
  │                       │                       │
  │                       │── StoreReportIO ─────▶│
  │                       │   (source_client_id,  │
  │                       │    io_size, ts)        │
  │                       │                       │
  │                       │◀── StoreIOAck ────────│
  │                       │                       │
```

## 4. 统计引擎设计

### 4.1 数据结构

```cpp
// 单次 IO 记录
struct IORecord {
    uint32_t client_id;
    uint32_t store_id;
    IOChannel io_channel;       // IO 通道类型
    uint64_t io_size;           // 字节
    uint64_t request_ts_ns;     // Client 发起请求的时间
    uint64_t start_ts_ns;       // IO 实际启动时间
    uint64_t done_ts_ns;        // IO 完成时间
    int32_t  status;            // 0=成功
    std::string remote_node_addr; // 远程节点地址（NET_TX/NET_RX 时有效）
};

// 单通道统计
struct ChannelStats {
    uint64_t total_bytes = 0;   // 通道总数据量
    uint32_t io_count = 0;      // IO 次数

    // 按通道的延迟百分位统计（从 reservoir 计算）
    double avg_latency_us = 0.0;
    double p50_latency_us = 0.0;
    double p99_latency_us = 0.0;
    double max_latency_us = 0.0;

    double BandwidthMBps(uint64_t window_ns) const {
        if (window_ns == 0) return 0.0;
        return (total_bytes / (1024.0 * 1024.0)) / (window_ns / 1e9);
    }
};

// 单远程节点统计（网络通道负载）
struct NodeAddrStats {
    std::string node_addr;              // 远程节点地址
    ChannelStats net_tx_read;           // → 该节点的网络读发送
    ChannelStats net_rx_read;           // ← 该节点的网络读接收

    double TotalNetTxBandwidthMBps(uint64_t window_ns) const {
        return net_tx_read.BandwidthMBps(window_ns);
    }
    double TotalNetRxBandwidthMBps(uint64_t window_ns) const {
        return net_rx_read.BandwidthMBps(window_ns);
    }
};

// 时间窗口统计
struct TimeWindowStats {
    uint64_t window_start_ns;  // 窗口起始时间
    uint64_t window_end_ns;    // 窗口结束时间

    // 按通道分列统计（每个通道包含独立的带宽和延迟数据）
    ChannelStats local_ssd_read;
    ChannelStats local_ssd_write;
    ChannelStats net_tx_read;
    ChannelStats net_rx_read;

    // 汇总统计
    uint32_t concurrent_peak;  // 窗口内最大并发 IO 数

    // 资源消耗汇总
    // SSD 带宽 = local_ssd_read + local_ssd_write + net_rx_read
    //   写入始终绑定本地 Store，无远程写路径
    //   远程读请求（NET_RX_READ）在 Store 侧消耗本地 SSD 带宽
    double SSDBandwidthMBps() const {
        uint64_t window_ns = window_end_ns - window_start_ns;
        return local_ssd_read.BandwidthMBps(window_ns)
             + local_ssd_write.BandwidthMBps(window_ns)
             + net_rx_read.BandwidthMBps(window_ns);
    }

    // 网络 TX 带宽 = net_tx_read（Client 向远程 Store 发送读请求）
    double NetTxBandwidthMBps() const {
        uint64_t window_ns = window_end_ns - window_start_ns;
        return net_tx_read.BandwidthMBps(window_ns);
    }

    // 网络 RX 带宽 = net_rx_read（Store 接收远程 Client 的读请求）
    double NetRxBandwidthMBps() const {
        uint64_t window_ns = window_end_ns - window_start_ns;
        return net_rx_read.BandwidthMBps(window_ns);
    }
};

// 节点级统计
class NodeStats {
public:
    // 记录一次 IO
    void RecordIO(const IORecord& record);

    // 获取当前时间窗口统计
    TimeWindowStats GetCurrentStats() const;

    // 获取按远程节点地址分列的网络通道统计
    const std::unordered_map<std::string, NodeAddrStats>& GetNodeAddrStats() const;

    // 获取 SSD 带宽利用率
    double GetSSDBandwidthUtilization() const;

    // 打印统计报告
    void PrintReport() const;

private:
    // 滑动时间窗口（环形缓冲区）
    static constexpr int WINDOW_COUNT = 60;    // 60 个窗口
    static constexpr int WINDOW_SIZE_MS = 1000; // 每个窗口 1 秒
    std::array<TimeWindowStats, WINDOW_COUNT> windows_;
    int current_window_idx_ = 0;

    // 按远程节点地址的网络通道统计
    std::unordered_map<std::string, NodeAddrStats> node_addr_stats_;

    // 硬件带宽上限（从配置读取）
    double ssd_bw_limit_mbps_;     // SSD 带宽上限 MB/s
    double net_bw_limit_mbps_;     // 网络带宽上限 MB/s

    // 按通道的延迟采样 reservoir（索引 = IOChannel 枚举值: 0-3）
    std::array<std::vector<uint64_t>, 4> latency_samples_;
    static constexpr size_t MAX_LATENCY_SAMPLES = 10000;
};
```

### 4.2 按通道延迟统计

延迟统计按 IO 通道独立采样和计算，每个通道维护独立的 reservoir，互不影响：

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

说明:
  - 写入始终绑定本地 Store (LOCAL_SSD_WRITE)，不存在远程写路径
  - 远程只有读请求：Client 通过 NET_TX_READ 向远程 Store 发读请求，
    Store 通过 NET_RX_READ 接收远程读请求并读取本地 SSD

SSD 带宽 = LOCAL_SSD_READ + LOCAL_SSD_WRITE + NET_RX_READ
网络 TX  = NET_TX_READ (按 remote_node_addr 分通道)
网络 RX  = NET_RX_READ (按 source_node_addr 分通道)
```

每个 IO 通道的延迟样本写入对应的 reservoir（`latency_samples_[channel]`），使用简单的 reservoir sampling 策略：当 reservoir 未满时直接追加，满后以新样本的延迟值 hash 替换一个已有位置。计算百分位时，从对应通道的 reservoir 排序后计算 avg/p50/p99/max。

### 4.3 统计报告输出

```cpp
// 定期打印统计报告（默认每 5 秒）
void NodeStats::PrintReport() const {
    // 从各通道 reservoir 计算延迟百分位
    auto compute_latency = [](ChannelStats& cs, const std::vector<uint64_t>& samples) {
        if (samples.empty()) return;
        std::vector<uint64_t> sorted = samples;
        std::sort(sorted.begin(), sorted.end());
        double sum = 0.0;
        for (uint64_t v : sorted) sum += static_cast<double>(v);
        cs.avg_latency_us = (sum / sorted.size()) / 1000.0;
        cs.p50_latency_us = sorted[sorted.size() / 2] / 1000.0;
        cs.p99_latency_us = sorted[static_cast<size_t>(sorted.size() * 0.99)] / 1000.0;
        cs.max_latency_us = sorted.back() / 1000.0;
    };
    // 对每个通道独立计算
    compute_latency(cw.local_ssd_read,  latency_samples_[0]);
    compute_latency(cw.local_ssd_write, latency_samples_[1]);
    compute_latency(cw.net_tx_read,     latency_samples_[2]);
    compute_latency(cw.net_rx_read,     latency_samples_[3]);

    // 每个通道一行，输出带宽 + 延迟
    auto print_ch = [&](const char* label, const ChannelStats& cs) {
        LOG(INFO) << "  " << label
                  << "  bytes=" << cs.total_bytes
                  << "  ios=" << cs.io_count
                  << "  bw=" << cs.BandwidthMBps(window_ns) << " MB/s"
                  << "  avg=" << cs.avg_latency_us << " us"
                  << "  p50=" << cs.p50_latency_us << " us"
                  << "  p99=" << cs.p99_latency_us << " us"
                  << "  max=" << cs.max_latency_us << " us";
    };

    LOG(INFO) << "=== Node Stats Report ===";
    print_ch("local_ssd_read ", cw.local_ssd_read);
    print_ch("local_ssd_write", cw.local_ssd_write);
    print_ch("net_tx_read    ", cw.net_tx_read);
    print_ch("net_rx_read    ", cw.net_rx_read);

    // SSD 带宽利用率
    LOG(INFO) << "  SSD BW utilization: " << (util * 100.0) << "%";

    // 按远程节点的网络通道负载
    for (const auto& [addr, ns] : node_addr_stats_) {
        LOG(INFO) << "    " << addr
            << "  tx_read=" << ns.net_tx_read.io_count
            << "  rx_read=" << ns.net_rx_read.io_count;
    }
    LOG(INFO) << "=== End Report ===";

    // 重置 reservoir，下一报告周期重新采样
    ResetPerWindowCounters();
}
```

## 5. 调度策略

### 5.1 当前阶段：放通模式（Passthrough）

所有 IO 请求立即放通，不做任何限流或延迟。调度模块仅负责收集统计信息。

```cpp
class PassthroughPolicy {
public:
    IOResponse Decide(const IORequest& request) {
        IOResponse response;
        response.set_status(0);                           // 放通
        response.set_permitted_ts_ns(request.request_ts_ns()); // 立即允许
        response.set_ticket(NextTicket());
        return response;
    }

private:
    std::atomic<uint64_t> ticket_counter_{0};
    uint64_t NextTicket() { return ticket_counter_.fetch_add(1); }
};
```

### 5.2 未来扩展预留

```cpp
// 调度策略接口
class IOSchedulePolicy {
public:
    virtual ~IOSchedulePolicy() = default;
    virtual IOResponse Decide(const IORequest& request) = 0;
    virtual void OnIOComplete(const IOCompletionReport& report) = 0;
    virtual std::string Name() const = 0;
};

// 工厂创建
std::unique_ptr<IOSchedulePolicy> CreatePolicy(const std::string& name) {
    if (name == "passthrough") {
        return std::make_unique<PassthroughPolicy>();
    }
    // 未来扩展：
    // if (name == "rate_limit") return std::make_unique<RateLimitPolicy>();
    // if (name == "fair_queue") return std::make_unique<FairQueuePolicy>();
    // if (name == "priority")   return std::make_unique<PriorityPolicy>();
    return std::make_unique<PassthroughPolicy>();  // 默认放通
}
```

## 6. 故障感知与 Bypass 机制

### 6.1 故障检测策略

Scheduler 是可选组件，当其异常退出或 hang 时，Client 和 Store 必须快速感知并 bypass。

```
故障检测层次（从快到慢）:

Level 0 - UDS 连接状态（瞬时）
  - RPC 调用返回连接错误 → 立即 bypass

Level 1 - RPC 超时（微秒级）
  - 单次 RPC 超过 scheduler_rpc_timeout_us_（默认 100us）→ bypass 本次

Level 2 - 连续失败计数（毫秒级）
  - 连续 N 次（默认 3 次）RPC 失败 → 标记 Scheduler 为不可用
  - 后续所有 IO 直接 bypass，不再尝试连接

Level 3 - 后台重连探测（秒级）
  - 标记不可用后，后台线程定期（默认 2s）尝试重连
  - 重连成功 → 恢复正常调度模式

Level 4 - UDS 文件存在性（瞬时）
  - Scheduler 正常运行时创建 .sock 文件
  - Scheduler 退出时 .sock 文件被清理
  - Client 启动时若 .sock 文件不存在 → 直接 bypass 模式
```

### 6.2 Client 端 Bypass 实现

SchedulerProxy 通过 brpc Channel/Stub 与 Scheduler 进程通信。当 brpc 不可用时（编译时未开启），自动降级为空操作（始终 bypass）。

```cpp
class SchedulerProxy {
public:
    explicit SchedulerProxy(const std::string& uds_path);
    ~SchedulerProxy();

    IOResponseData RequestIO(const IORequestData& request);
    void ReportIOCompletion(const IOCompletionData& report);
    void StoreReportIOAsync(uint32_t store_id, int io_channel,
                            uint32_t source_client_id, uint64_t io_size,
                            uint64_t request_ts_ns, uint64_t done_ts_ns,
                            const std::string& source_node_addr);
    bool IsBypassMode() const;

private:
    enum class State { CONNECTED, DISCONNECTED, BYPASS };

    IOResponseData MakeBypassResponse(const IORequestData& request);
    void StartReconnectProbe();
    bool ProbeScheduler();

#ifdef FALCONKV_HAS_BRPC
    bool Connect();  // 建立 brpc Channel 到 Scheduler（UDS）
#endif

    std::string uds_path_;
    std::atomic<State> state_;
    std::atomic<int> consecutive_failures_{0};
    std::atomic<bool> stopped_{false};   // 析构时置 true，通知重连线程退出

    static constexpr int MAX_CONSECUTIVE_FAILURES = 3;
    static constexpr int RECONNECT_INTERVAL_SEC = 2;
    static constexpr int RPC_TIMEOUT_MS = 2;  // brpc RPC 超时

    mutable std::mutex reconnect_mutex_;
    bool reconnect_started_ = false;
    std::thread reconnect_thread_;  // joinable，析构时 join

#ifdef FALCONKV_HAS_BRPC
    std::unique_ptr<brpc::Channel> channel_;
    std::unique_ptr<FalconKVSchedulerService_Stub> stub_;
#endif
};
```

#### brpc 通信实现

当 `FALCONKV_HAS_BRPC` 编译宏开启时，SchedulerProxy 使用 brpc 通过 UDS 与 Scheduler 通信：

```cpp
// 建立 UDS 连接
bool SchedulerProxy::Connect() {
    if (!channel_) {
        channel_ = std::make_unique<brpc::Channel>();
    }
    brpc::ChannelOptions options;
    options.timeout_ms = RPC_TIMEOUT_MS;
    options.protocol = brpc::PROTOCOL_BAIDU_STD;
    std::string addr = "unix:" + uds_path_;
    if (channel_->Init(addr.c_str(), &options) != 0) {
        channel_.reset();
        stub_.reset();
        return false;
    }
    stub_ = std::make_unique<FalconKVSchedulerService_Stub>(channel_.get());
    return true;
}

// 通过 Heartbeat RPC 检测 Scheduler 可达性
bool SchedulerProxy::ProbeScheduler() {
    if (!Connect()) return false;
    HeartbeatRequest req;
    req.set_timestamp_ns(NowNanos());
    HeartbeatResponse resp;
    brpc::Controller cntl;
    stub_->Heartbeat(&cntl, &req, &resp, nullptr);
    return !cntl.Failed();
}
```

#### IO 申请与上报

```cpp
IOResponseData SchedulerProxy::RequestIO(const IORequestData& request) {
    State s = state_.load(std::memory_order_acquire);

    // 快速路径：已标记不可用，直接 bypass
    if (s == State::BYPASS) {
        return MakeBypassResponse(request);
    }

    // 尝试连接
    if (s == State::DISCONNECTED) {
        if (ProbeScheduler()) {
            state_.store(State::CONNECTED, std::memory_order_release);
        } else {
            StartReconnectProbe();
            return MakeBypassResponse(request);
        }
    }

    // CONNECTED — 发送真实 RPC
    IORequest io_req;
    io_req.set_client_id(request.client_id);
    io_req.set_io_channel(static_cast<IOChannel>(request.io_channel));
    io_req.set_store_id(request.store_id);
    io_req.set_io_size(request.io_size);
    io_req.set_priority(request.priority);
    io_req.set_request_ts_ns(request.request_ts_ns);
    io_req.set_remote_node_addr(request.remote_node_addr);

    IOResponse io_resp;
    brpc::Controller cntl;
    cntl.set_timeout_ms(RPC_TIMEOUT_MS);
    stub_->RequestIO(&cntl, &io_req, &io_resp, nullptr);

    if (cntl.Failed()) {
        int failures = consecutive_failures_.fetch_add(1) + 1;
        if (failures >= MAX_CONSECUTIVE_FAILURES) {
            state_.store(State::BYPASS, std::memory_order_release);
            StartReconnectProbe();
        }
        return MakeBypassResponse(request);
    }

    consecutive_failures_.store(0);
    IOResponseData resp;
    resp.status = io_resp.status();
    resp.permitted_ts_ns = io_resp.permitted_ts_ns();
    resp.ticket = io_resp.ticket();
    return resp;
}
```

#### 重连探测与生命周期管理

重连线程使用 `stopped_` 原子标志和 joinable 线程（非 detached），确保析构时安全退出：

```cpp
void SchedulerProxy::StartReconnectProbe() {
    std::lock_guard<std::mutex> lock(reconnect_mutex_);
    if (reconnect_started_) return;
    reconnect_started_ = true;

    reconnect_thread_ = std::thread([this]() {
        while (!stopped_.load(std::memory_order_acquire)) {
            std::this_thread::sleep_for(
                std::chrono::seconds(RECONNECT_INTERVAL_SEC));
            if (stopped_.load(std::memory_order_acquire)) return;
            if (ProbeScheduler()) {
                state_.store(State::CONNECTED);
                consecutive_failures_.store(0);
                {
                    std::lock_guard<std::mutex> lk(reconnect_mutex_);
                    reconnect_started_ = false;
                }
                return;
            }
        }
    });
}

SchedulerProxy::~SchedulerProxy() {
    stopped_.store(true, std::memory_order_release);
    std::lock_guard<std::mutex> lock(reconnect_mutex_);
    if (reconnect_thread_.joinable()) {
        reconnect_thread_.join();
    }
}
```

#### 无 brpc 降级

当编译时未开启 brpc（`FALCONKV_HAS_BRPC` 未定义），`ProbeScheduler()` 始终返回 `false`，`RequestIO()` 始终返回 bypass 响应，`ReportIOCompletion()` 和 `StoreReportIOAsync()` 为空操作。

### 6.3 Store 端 Bypass 实现

Store 端的上报逻辑与 Client 类似，使用相同的 `SchedulerProxy`：

```cpp
// Store 处理远程读 RPC 请求时
void FalconKVStore::OnRPCReadRequest(const ReadRequest& req) {
    // ... 执行实际 SSD 读取 ...

    // 异步上报 Scheduler（非阻塞，失败不影响响应）
    if (scheduler_proxy_ && !scheduler_proxy_->IsBypassMode()) {
        StoreIOReport report;
        report.set_store_id(store_id_);
        report.set_io_channel(NET_RX_READ);  // Store 视角：网络接收 + SSD 读取
        report.set_source_client_id(req.client_id());
        report.set_io_size(req.size());
        report.set_request_ts_ns(start_ts);
        report.set_done_ts_ns(done_ts);
        report.set_source_node_addr(req.source_node_addr()); // 远程 Client 来源
        scheduler_proxy_->StoreReportIOAsync(report);  // fire-and-forget
    }
}
```

### 6.4 Bypass 恢复时间要求

| 场景 | 检测时间 | 恢复动作 |
|------|----------|----------|
| Scheduler 进程退出 | < 2ms（brpc RPC 连接失败） | 立即 bypass，连续 3 次后永久 bypass |
| Scheduler hang（不响应） | < 2ms（brpc RPC 超时 `RPC_TIMEOUT_MS`） | 本次 bypass，连续 3 次后永久 bypass |
| Scheduler 启动延迟 | N/A（Client 启动时检测） | 直接 bypass 模式启动 |
| Scheduler 恢复 | < 2s（后台探测间隔） | 自动恢复正常模式 |
| UDS 文件不存在 | 瞬时（启动时检查） | bypass 模式启动 |

## 7. Scheduler 进程设计

### 7.1 进程结构

```cpp
class FalconKVScheduler {
public:
    struct Config {
        std::string uds_path;             // UDS 监听路径
        std::string schedule_policy;       // 调度策略名（"passthrough"）
        double ssd_bw_limit_mbps;         // SSD 带宽上限 MB/s
        double net_bw_limit_mbps;         // 网络带宽上限 MB/s
        int stats_report_interval_sec;    // 统计报告间隔（默认 5）
        int stats_window_ms;              // 统计窗口大小（默认 1000）
        int rpc_timeout_us;               // RPC 超时（默认 100）
    };

    FalconKVScheduler(const Config& config);
    ~FalconKVScheduler();

    // 启动 Scheduler
    Status Start();

    // 停止 Scheduler
    void Stop();

private:
    Config config_;
    std::unique_ptr<IOSchedulePolicy> policy_;
    std::unique_ptr<NodeStats> stats_;

    // brpc Server（UDS）
    std::unique_ptr<brpc::Server> server_;

    // 统计报告线程
    std::thread stats_thread_;
    std::atomic<bool> running_{false};
};
```

### 7.2 启动流程

```
Scheduler 启动:
1. 读取配置文件
2. 创建 UDS 监听路径（创建 .sock 文件）
3. 初始化调度策略（PassthroughPolicy）
4. 初始化统计引擎（NodeStats，含按通道延迟采样 reservoir）
5. 启动 brpc Server（监听 UDS）
6. 启动统计报告线程
7. 进入服务状态

Scheduler 停止:
1. 设置 running_ = false
2. 停止 brpc Server
3. 打印最终统计报告
4. 清理 UDS 文件
5. 退出
```

### 7.3 RPC Handler 实现

```cpp
class SchedulerServiceImpl : public FalconKVSchedulerService {
public:
    SchedulerServiceImpl(IOSchedulePolicy* policy, NodeStats* stats)
        : policy_(policy), stats_(stats) {}

    void RequestIO(google::protobuf::RpcController* controller,
                   const IORequest* request,
                   IOResponse* response,
                   google::protobuf::Closure* done) override {
        brpc::ClosureGuard done_guard(done);

        // 调度决策（passthrough: 直接放通）
        *response = policy_->Decide(*request);
    }

    void ReportIOCompletion(google::protobuf::RpcController* controller,
                            const IOCompletionReport* report,
                            IOCompletionAck* ack,
                            google::protobuf::Closure* done) override {
        brpc::ClosureGuard done_guard(done);

        // 更新统计引擎（含按通道延迟采样）
        IORecord record;
        record.client_id = report->client_id();
        record.store_id = report->store_id();
        record.io_channel = report->io_channel();
        record.io_size = report->io_size();
        record.start_ts_ns = report->io_start_ts_ns();
        record.done_ts_ns = report->io_done_ts_ns();
        record.status = report->io_status();
        record.remote_node_addr = report->remote_node_addr();
        stats_->RecordIO(record);

        // 通知调度策略（未来扩展用）
        policy_->OnIOComplete(*report);

        ack->set_status(0);
    }

    void StoreReportIO(google::protobuf::RpcController* controller,
                       const StoreIOReport* report,
                       StoreIOAck* ack,
                       google::protobuf::Closure* done) override {
        brpc::ClosureGuard done_guard(done);

        // 记录 Store 侧的 IO 信息（NET_RX 通道）
        IORecord record;
        record.client_id = report->source_client_id();
        record.store_id = report->store_id();
        record.io_channel = report->io_channel();
        record.io_size = report->io_size();
        record.start_ts_ns = report->request_ts_ns();
        record.done_ts_ns = report->done_ts_ns();
        record.status = 0;
        record.remote_node_addr = report->source_node_addr();
        stats_->RecordIO(record);

        ack->set_status(0);
    }

    void Heartbeat(google::protobuf::RpcController* controller,
                   const HeartbeatRequest* request,
                   HeartbeatResponse* response,
                   google::protobuf::Closure* done) override {
        brpc::ClosureGuard done_guard(done);
        response->set_status(0);
        response->set_timestamp_ns(GetCurrentTimeNs());
    }

private:
    IOSchedulePolicy* policy_;
    NodeStats* stats_;
};
```

## 8. 线程模型

```
┌──────────────────────────────────────────────────────────────┐
│ IO Scheduler Process                                         │
│                                                               │
│  ┌─────────────────────────────────────────────────────┐     │
│  │ brpc Server (UDS)                                    │     │
│  │  ┌──────────────┐  ┌──────────────────────────────┐ │     │
│  │  │ I/O Thread   │  │ Worker Threads               │ │     │
│  │  │ (UDS 收发)   │  │ (RPC Handler)                │ │     │
│  │  └──────────────┘  │ - RequestIO (放通 + 记录)     │ │     │
│  │                    │ - ReportIOCompletion (统计)    │ │     │
│  │                    │ - StoreReportIO (统计)         │ │     │
│  │                    │ - Heartbeat                    │ │     │
│  │                    └──────────────────────────────┘ │     │
│  └─────────────────────────────────────────────────────┘     │
│                                                               │
│  ┌─────────────────────────────────────────────────────┐     │
│  │ Stats Report Thread (独立线程)                       │     │
│  │  - 定期汇总统计窗口                                  │     │
│  │  - 按通道计算延迟百分位并打印报告                     │     │
│  │  - 重置采样 reservoir                                │     │
│  └─────────────────────────────────────────────────────┘     │
└──────────────────────────────────────────────────────────────┘
```

## 9. 配置项

Scheduler 相关配置分布在 **common 区**（Client/Store 与 Scheduler 通信的参数）和 **scheduler 区**（Scheduler 服务端自身的参数）。

### 9.1 common 区（共享，自动传播到 Client/Store/Scheduler）

| JSON 字段 (`common.*`) | 默认值 | 说明 |
|------------------------|--------|------|
| `scheduler_enabled` | true | 是否启用 Scheduler 通信（false 时 Client/Store 直接 bypass） |
| `scheduler_uds_path` | /tmp/falconkv_scheduler.sock | Scheduler UDS 路径 |
| `scheduler_rpc_timeout_us` | 2000 | Client/Store 到 Scheduler 的 RPC 超时（微秒） |
| `max_consecutive_failures` | 3 | SchedulerProxy 连续 RPC 失败次数阈值（触发 bypass） |
| `reconnect_interval_sec` | 2 | bypass 后重连探测间隔（秒） |

### 9.2 scheduler 区（Scheduler 服务端专属）

| JSON 字段 (`scheduler.*`) | 默认值 | 说明 |
|---------------------------|--------|------|
| `schedule_policy` | passthrough | 调度策略名 |
| `ssd_bw_limit_mbps` | 7000 | SSD 带宽上限 MB/s（NVMe 约 7GB/s） |
| `net_bw_limit_mbps` | 12500 | 网络带宽上限 MB/s（100Gbps 约 12.5GB/s） |
| `stats_report_interval_sec` | 5 | 统计报告打印间隔（秒） |
| `stats_window_ms` | 1000 | 统计时间窗口大小（毫秒） |

## 10. 与其他模块的交互

### 10.1 Client 侧集成点

```
Client BatchPut/BatchGet 流程中的 Scheduler 交互:

1. 获取 key 描述后，执行 IO 前:
   → scheduler_proxy_->RequestIO(request)
   → IORequest.io_channel 按亲和层级映射:
     - Level 0/1 读: LOCAL_SSD_READ
     - Level 0/1 写: LOCAL_SSD_WRITE（写入始终绑定本地 Store）
     - Level 2 读:   NET_TX_READ + remote_node_addr
   → 若 bypass: 直接执行 IO
   → 若正常: 等到 permitted_ts 后执行 IO

2. IO 完成后:
   → scheduler_proxy_->ReportIOCompletion(report)
   → IOCompletionReport.io_channel 与申请时一致
   → 若 bypass: 跳过上报
   → 若正常: 异步上报（fire-and-forget）
```

### 10.2 Store 侧集成点

```
Store 处理远程读 RPC 请求后:

1. 执行完 Read RPC 后:
   → scheduler_proxy_->StoreReportIOAsync(report)
   → StoreIOReport.io_channel 映射:
     - 远程读请求:  NET_RX_READ
   → StoreIOReport.source_node_addr: 远程 Client 来源节点地址
   → 异步上报，不阻塞 RPC 响应

2. 仅上报远程 Client 发起的读 IO:
   → 本地 Client 的 IO 由 Client 自己上报（LOCAL_SSD_READ/WRITE）
   → Store 仅上报收到的远程读 RPC IO（NET_RX_READ）
```
