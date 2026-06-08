#include "src/scheduler/node_stats.h"

#include <chrono>
#include <algorithm>
#include <iomanip>
#include <numeric>

#include "src/common/logging.h"

namespace falconkv {

namespace {

uint64_t NowNanos() {
    auto tp = std::chrono::steady_clock::now().time_since_epoch();
    return static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(tp).count());
}

}  // namespace

// ---------------------------------------------------------------------------
// NodeStats
// ---------------------------------------------------------------------------

NodeStats::NodeStats(double ssd_bw_limit_mbps, double net_bw_limit_mbps,
                     int window_size_ms)
    : ssd_bw_limit_mbps_(ssd_bw_limit_mbps),
      net_bw_limit_mbps_(net_bw_limit_mbps),
      window_size_ms_(window_size_ms) {
    uint64_t base = NowNanos();
    uint64_t ws = WindowSizeNs();
    for (int i = 0; i < WINDOW_COUNT; ++i) {
        windows_[i].window_start_ns = base - static_cast<uint64_t>(WINDOW_COUNT - i) * ws;
        windows_[i].window_end_ns   = base - static_cast<uint64_t>(WINDOW_COUNT - i - 1) * ws;
    }
    current_window_idx_ = WINDOW_COUNT - 1;
    latency_samples_.reserve(MAX_LATENCY_SAMPLES);
}

uint64_t NodeStats::WindowSizeNs() const {
    return static_cast<uint64_t>(window_size_ms_) * 1000000ULL;
}

ChannelStats& NodeStats::GetChannelStats(TimeWindowStats& ws, int channel) {
    // IOChannel enum: LOCAL_SSD_READ=0, LOCAL_SSD_WRITE=1,
    //                 NET_TX_READ=2, NET_RX_READ=3
    switch (channel) {
        case 0: return ws.local_ssd_read;
        case 1: return ws.local_ssd_write;
        case 2: return ws.net_tx_read;
        case 3: return ws.net_rx_read;
        default:
            // Should not happen -- return local_ssd_read as fallback.
            return ws.local_ssd_read;
    }
}

void NodeStats::UpdateWindow(const IOCompletionData& record) {
    uint64_t ws = WindowSizeNs();

    // Find the window that contains record.io_done_ts_ns.
    // If it falls past the current window, advance.
    int idx = current_window_idx_;
    while (record.io_done_ts_ns >= windows_[idx].window_end_ns) {
        int next = (idx + 1) % WINDOW_COUNT;
        // Reset the next window.
        windows_[next].window_start_ns = windows_[idx].window_end_ns;
        windows_[next].window_end_ns   = windows_[idx].window_end_ns + ws;
        windows_[next].local_ssd_read  = {};
        windows_[next].local_ssd_write = {};
        windows_[next].net_tx_read     = {};
        windows_[next].net_rx_read     = {};
        windows_[next].concurrent_peak = 0;
        windows_[next].avg_latency_us  = 0.0;
        windows_[next].p50_latency_us  = 0.0;
        windows_[next].p99_latency_us  = 0.0;
        windows_[next].max_latency_us  = 0.0;
        idx = next;
    }
    current_window_idx_ = idx;

    // Accumulate into the selected window.
    ChannelStats& cs = GetChannelStats(windows_[idx], record.io_channel);
    cs.total_bytes += record.io_size;
    cs.io_count   += 1;
}

void NodeStats::RecordIO(const IOCompletionData& record) {
    std::lock_guard<std::mutex> lock(mutex_);

    // 1. Update the ring buffer.
    UpdateWindow(record);

    // 2. Update per-node-addr counters.
    if (!record.remote_node_addr.empty()) {
        auto& nas = node_addr_stats_[record.remote_node_addr];
        nas.node_addr = record.remote_node_addr;

        // Map channel to the appropriate NodeAddrStats field.
        ChannelStats* target = nullptr;
        switch (record.io_channel) {
            case 2: target = &nas.net_tx_read;  break;
            case 3: target = &nas.net_rx_read;  break;
            default: break;  // local channels ignored for per-node stats
        }
        if (target) {
            target->total_bytes += record.io_size;
            target->io_count   += 1;
        }
    }

    // 3. Collect latency sample.
    if (record.io_done_ts_ns >= record.io_start_ts_ns) {
        uint64_t latency_ns = record.io_done_ts_ns - record.io_start_ts_ns;
        if (latency_samples_.size() >= MAX_LATENCY_SAMPLES) {
            // Simple reservoir: replace a random entry.
            size_t pos = static_cast<size_t>(latency_ns % latency_samples_.size());
            latency_samples_[pos] = latency_ns;
        } else {
            latency_samples_.push_back(latency_ns);
        }
    }
}

TimeWindowStats NodeStats::GetCurrentStats() const {
    std::lock_guard<std::mutex> lock(mutex_);
    TimeWindowStats snap = windows_[current_window_idx_];

    // Compute latency percentiles from the reservoir.
    if (!latency_samples_.empty()) {
        std::vector<uint64_t> sorted = latency_samples_;
        std::sort(sorted.begin(), sorted.end());

        double sum = 0.0;
        for (uint64_t v : sorted) sum += static_cast<double>(v);
        snap.avg_latency_us = (sum / static_cast<double>(sorted.size())) / 1000.0;

        snap.p50_latency_us = static_cast<double>(
            sorted[sorted.size() / 2]) / 1000.0;
        snap.p99_latency_us = static_cast<double>(
            sorted[static_cast<size_t>(sorted.size() * 0.99)]) / 1000.0;
        snap.max_latency_us = static_cast<double>(sorted.back()) / 1000.0;
    }
    return snap;
}

const std::unordered_map<std::string, NodeAddrStats>&
NodeStats::GetNodeAddrStats() const {
    return node_addr_stats_;
}

double NodeStats::GetSSDBandwidthUtilization() const {
    std::lock_guard<std::mutex> lock(mutex_);
    const auto& w = windows_[current_window_idx_];
    uint64_t ssd_bytes = w.local_ssd_read.total_bytes
                       + w.local_ssd_write.total_bytes
                       + w.net_rx_read.total_bytes;
    if (ssd_bw_limit_mbps_ <= 0.0) return 0.0;
    uint64_t window_ns = w.window_end_ns - w.window_start_ns;
    if (window_ns == 0) return 0.0;
    double bw = (static_cast<double>(ssd_bytes) / (1024.0 * 1024.0))
              / (static_cast<double>(window_ns) / 1e9);
    return bw / ssd_bw_limit_mbps_;
}

void NodeStats::PrintReport() {
    std::lock_guard<std::mutex> lock(mutex_);

    const auto& w = windows_[current_window_idx_];
    uint64_t window_ns = w.window_end_ns - w.window_start_ns;

    LOG(INFO) << "=== Node Stats Report (window " << w.window_start_ns
              << "-" << w.window_end_ns << " ns) ===";

    auto print_ch = [&window_ns](const char* label, const ChannelStats& cs) {
        LOG(INFO) << "  " << std::setw(20) << std::left << label
                  << "  bytes=" << cs.total_bytes
                  << "  ios=" << cs.io_count
                  << "  bw=" << std::fixed << std::setprecision(2)
                  << cs.BandwidthMBps(window_ns) << " MB/s";
    };

    print_ch("local_ssd_read",  w.local_ssd_read);
    print_ch("local_ssd_write", w.local_ssd_write);
    print_ch("net_tx_read",     w.net_tx_read);
    print_ch("net_rx_read",     w.net_rx_read);

    if (!latency_samples_.empty()) {
        std::vector<uint64_t> sorted = latency_samples_;
        std::sort(sorted.begin(), sorted.end());
        double sum = 0.0;
        for (uint64_t v : sorted) sum += static_cast<double>(v);
        LOG(INFO) << "  latency: avg=" << std::fixed << std::setprecision(1)
                  << (sum / static_cast<double>(sorted.size()) / 1000.0)
                  << " us  p50="
                  << (static_cast<double>(sorted[sorted.size() / 2]) / 1000.0)
                  << " us  p99="
                  << (static_cast<double>(sorted[static_cast<size_t>(sorted.size() * 0.99)]) / 1000.0)
                  << " us  max="
                  << (static_cast<double>(sorted.back()) / 1000.0)
                  << " us  samples=" << sorted.size();
    }

    // Compute SSD BW utilization inline (mutex_ is already held by
    // PrintReport, so we must NOT call GetSSDBandwidthUtilization() which
    // would try to re-acquire the non-recursive mutex and deadlock).
    {
        uint64_t ssd_bytes = w.local_ssd_read.total_bytes
                           + w.local_ssd_write.total_bytes
                           + w.net_rx_read.total_bytes;
        double util = 0.0;
        if (ssd_bw_limit_mbps_ > 0.0 && window_ns > 0) {
            double bw = (static_cast<double>(ssd_bytes) / (1024.0 * 1024.0))
                      / (static_cast<double>(window_ns) / 1e9);
            util = bw / ssd_bw_limit_mbps_;
        }
        LOG(INFO) << "  SSD BW utilization: " << std::fixed << std::setprecision(1)
                  << (util * 100.0) << "%";
    }

    if (!node_addr_stats_.empty()) {
        LOG(INFO) << "  Per-node stats (" << node_addr_stats_.size() << " nodes):";
        for (const auto& kv : node_addr_stats_) {
            const auto& nas = kv.second;
            LOG(INFO) << "    " << nas.node_addr
                      << "  tx_read=" << nas.net_tx_read.io_count
                      << "  rx_read=" << nas.net_rx_read.io_count;
        }
    }

    LOG(INFO) << "=== End Report ===";

    // Reset per-window counters so the next report shows only the new window's data.
    ResetPerWindowCounters();
}

void NodeStats::ResetPerWindowCounters() {
    // mutex_ is already held by PrintReport().
    node_addr_stats_.clear();
    latency_samples_.clear();
}

}  // namespace falconkv
