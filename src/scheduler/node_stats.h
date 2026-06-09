#pragma once
#include <array>
#include <vector>
#include <unordered_map>
#include <mutex>
#include <cstdint>
#include <string>
#include "src/scheduler/passthrough_policy.h"

namespace falconkv {

// ---------------------------------------------------------------------------
// Aggregated counters for a single traffic channel within a time window.
// ---------------------------------------------------------------------------
struct ChannelStats {
    uint64_t total_bytes = 0;
    uint32_t io_count = 0;

    // Per-channel latency percentiles (populated from reservoir)
    double avg_latency_us = 0.0;
    double p50_latency_us = 0.0;
    double p99_latency_us = 0.0;
    double max_latency_us = 0.0;

    double BandwidthMBps(uint64_t window_ns) const {
        if (window_ns == 0) return 0.0;
        return (total_bytes / (1024.0 * 1024.0)) / (window_ns / 1e9);
    }
};

// ---------------------------------------------------------------------------
// Per-remote-node counters (used for TX/RX breakdown).
// ---------------------------------------------------------------------------
struct NodeAddrStats {
    std::string node_addr;
    ChannelStats net_tx_read;
    ChannelStats net_rx_read;
};

// ---------------------------------------------------------------------------
// Statistics for a single sliding time-window.
// ---------------------------------------------------------------------------
struct TimeWindowStats {
    uint64_t window_start_ns = 0;
    uint64_t window_end_ns = 0;

    ChannelStats local_ssd_read;
    ChannelStats local_ssd_write;
    ChannelStats net_tx_read;
    ChannelStats net_rx_read;

    uint32_t concurrent_peak = 0;
};

// ---------------------------------------------------------------------------
// NodeStats -- ring-buffer based statistics engine.
//
// Maintains WINDOW_COUNT adjacent time-windows of WINDOW_SIZE_MS each and
// aggregates IO metrics (throughput, latency percentiles, per-node traffic).
// ---------------------------------------------------------------------------
class NodeStats {
public:
    explicit NodeStats(double ssd_bw_limit_mbps = 7000.0,
                       double net_bw_limit_mbps = 12500.0,
                       int window_size_ms = 1000);

    /// Record a completed IO.
    void RecordIO(const IOCompletionData& record);

    /// Return a snapshot of the current (i.e. most-recently-written) window.
    TimeWindowStats GetCurrentStats() const;

    /// Per-node traffic breakdown.
    const std::unordered_map<std::string, NodeAddrStats>& GetNodeAddrStats() const;

    /// SSD bandwidth utilisation as a fraction of the configured limit.
    double GetSSDBandwidthUtilization() const;

    /// Print a human-readable report and reset per-window counters.
    void PrintReport();

    /// Reset per-window accumulators (node_addr_stats_ and latency_samples_).
    void ResetPerWindowCounters();

    static constexpr int WINDOW_COUNT = 60;

private:
    void UpdateWindow(const IOCompletionData& record);
    ChannelStats& GetChannelStats(TimeWindowStats& ws, int channel);
    uint64_t WindowSizeNs() const;

    std::array<TimeWindowStats, WINDOW_COUNT> windows_;
    int current_window_idx_ = 0;

    std::unordered_map<std::string, NodeAddrStats> node_addr_stats_;

    double ssd_bw_limit_mbps_;
    double net_bw_limit_mbps_;
    const int window_size_ms_;

    // Per-channel latency reservoirs (index = IOChannel enum value: 0-3)
    std::array<std::vector<uint64_t>, 4> latency_samples_;
    static constexpr size_t MAX_LATENCY_SAMPLES = 10000;

    mutable std::mutex mutex_;
};

}  // namespace falconkv
