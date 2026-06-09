#include <gtest/gtest.h>
#include <vector>
#include <string>
#include <chrono>

#include "src/scheduler/node_stats.h"
#include "src/common/time_util.h"

using namespace falconkv;

namespace {

uint64_t NowNanos() {
    auto tp = std::chrono::steady_clock::now().time_since_epoch();
    return static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(tp).count());
}

} // anonymous namespace

// ---------------------------------------------------------------------------
// Record single IO
// ---------------------------------------------------------------------------
TEST(NodeStats, RecordSingleIO) {
    NodeStats stats(7000.0, 12500.0);

    uint64_t now = NowNanos();

    IOCompletionData record;
    record.client_id = 1;
    record.ticket = 100;
    record.io_start_ts_ns = now - 100000; // 100us ago
    record.io_done_ts_ns = now;
    record.io_size = 4096;
    record.io_channel = 0; // LOCAL_SSD_READ
    record.io_status = 0;
    record.store_id = 1;

    stats.RecordIO(record);

    TimeWindowStats snap = stats.GetCurrentStats();
    // The IO should have been recorded in the current window.
    EXPECT_EQ(snap.local_ssd_read.io_count, 1u);
    EXPECT_EQ(snap.local_ssd_read.total_bytes, 4096u);
}

TEST(NodeStats, RecordMultipleIOsSameChannel) {
    NodeStats stats(7000.0, 12500.0);

    uint64_t now = NowNanos();

    for (int i = 0; i < 10; ++i) {
        IOCompletionData record;
        record.io_start_ts_ns = now - 100000;
        record.io_done_ts_ns = now;
        record.io_size = 4096;
        record.io_channel = 1; // LOCAL_SSD_WRITE
        stats.RecordIO(record);
    }

    TimeWindowStats snap = stats.GetCurrentStats();
    EXPECT_EQ(snap.local_ssd_write.io_count, 10u);
    EXPECT_EQ(snap.local_ssd_write.total_bytes, 40960u);
}

// ---------------------------------------------------------------------------
// Bandwidth calculation
// ---------------------------------------------------------------------------
TEST(NodeStats, BandwidthCalculation) {
    ChannelStats cs;
    cs.total_bytes = 1024 * 1024; // 1 MB
    cs.io_count = 1;

    // 1 MB in 1 second window = 1 MB/s.
    double bw = cs.BandwidthMBps(1000000000ULL);
    EXPECT_NEAR(bw, 1.0, 0.01);
}

TEST(NodeStats, BandwidthZeroWindow) {
    ChannelStats cs;
    cs.total_bytes = 1024 * 1024;
    cs.io_count = 1;

    double bw = cs.BandwidthMBps(0);
    EXPECT_DOUBLE_EQ(bw, 0.0);
}

TEST(NodeStats, BandwidthLargeTransfer) {
    ChannelStats cs;
    cs.total_bytes = 100 * 1024 * 1024; // 100 MB
    cs.io_count = 50;

    // 100 MB in 0.1 second = 1000 MB/s.
    double bw = cs.BandwidthMBps(100000000ULL);
    EXPECT_NEAR(bw, 1000.0, 1.0);
}

// ---------------------------------------------------------------------------
// Latency percentiles
// ---------------------------------------------------------------------------
TEST(NodeStats, LatencyPercentiles) {
    NodeStats stats(7000.0, 12500.0);

    uint64_t base = NowNanos() - 1000000; // 1ms ago

    // Record 100 IOs with increasing latency.
    for (int i = 0; i < 100; ++i) {
        IOCompletionData record;
        record.io_start_ts_ns = base;
        record.io_done_ts_ns = base + static_cast<uint64_t>(i + 1) * 1000; // 1us to 100us
        record.io_size = 4096;
        record.io_channel = 0;
        stats.RecordIO(record);
    }

    TimeWindowStats snap = stats.GetCurrentStats();
    // Average latency should be somewhere in the range.
    EXPECT_GT(snap.local_ssd_read.avg_latency_us, 0.0);
    EXPECT_GT(snap.local_ssd_read.p50_latency_us, 0.0);
    EXPECT_GT(snap.local_ssd_read.p99_latency_us, 0.0);
    EXPECT_GT(snap.local_ssd_read.max_latency_us, 0.0);

    // P50 should be less than P99.
    EXPECT_LE(snap.local_ssd_read.p50_latency_us, snap.local_ssd_read.p99_latency_us);
    // P99 should be less than or equal to max.
    EXPECT_LE(snap.local_ssd_read.p99_latency_us, snap.local_ssd_read.max_latency_us);
}

// ---------------------------------------------------------------------------
// Per-node stats
// ---------------------------------------------------------------------------
TEST(NodeStats, PerNodeAddrStats) {
    NodeStats stats(7000.0, 12500.0);

    uint64_t now = NowNanos();

    IOCompletionData record;
    record.io_start_ts_ns = now - 100000;
    record.io_done_ts_ns = now;
    record.io_size = 8192;
    record.io_channel = 2; // NET_TX_READ
    record.remote_node_addr = "10.0.0.2:8901";

    stats.RecordIO(record);

    const auto& node_stats = stats.GetNodeAddrStats();
    ASSERT_TRUE(node_stats.find("10.0.0.2:8901") != node_stats.end());

    const auto& nas = node_stats.at("10.0.0.2:8901");
    EXPECT_EQ(nas.net_tx_read.io_count, 1u);
    EXPECT_EQ(nas.net_tx_read.total_bytes, 8192u);
}

// ---------------------------------------------------------------------------
// SSD bandwidth utilization
// ---------------------------------------------------------------------------
TEST(NodeStats, SSDBandwidthUtilization) {
    NodeStats stats(7000.0, 12500.0);

    // Without any IOs, utilization should be 0.
    double util = stats.GetSSDBandwidthUtilization();
    EXPECT_DOUBLE_EQ(util, 0.0);
}

// ---------------------------------------------------------------------------
// Per-channel latency isolation
// ---------------------------------------------------------------------------
TEST(NodeStats, PerChannelLatencyStats) {
    NodeStats stats(7000.0, 12500.0);

    uint64_t now = NowNanos();

    // Channel 0 (local_ssd_read): 10 IOs with 100us latency each.
    for (int i = 0; i < 10; ++i) {
        IOCompletionData record;
        record.io_start_ts_ns = now - 100000; // 100 us before now
        record.io_done_ts_ns = now;
        record.io_size = 4096;
        record.io_channel = 0;
        stats.RecordIO(record);
    }

    // Channel 1 (local_ssd_write): 10 IOs with 500us latency each.
    for (int i = 0; i < 10; ++i) {
        IOCompletionData record;
        record.io_start_ts_ns = now - 500000; // 500 us before now
        record.io_done_ts_ns = now;
        record.io_size = 8192;
        record.io_channel = 1;
        stats.RecordIO(record);
    }

    // Channel 2 (net_tx_read): 10 IOs with 1000us latency each.
    for (int i = 0; i < 10; ++i) {
        IOCompletionData record;
        record.io_start_ts_ns = now - 1000000; // 1000 us before now
        record.io_done_ts_ns = now;
        record.io_size = 4096;
        record.io_channel = 2;
        stats.RecordIO(record);
    }

    // Channel 3 (net_rx_read): 10 IOs with 2000us latency each.
    for (int i = 0; i < 10; ++i) {
        IOCompletionData record;
        record.io_start_ts_ns = now - 2000000; // 2000 us before now
        record.io_done_ts_ns = now;
        record.io_size = 4096;
        record.io_channel = 3;
        stats.RecordIO(record);
    }

    TimeWindowStats snap = stats.GetCurrentStats();

    // Each channel should have independent latency stats.
    // Channel 0: ~100 us
    EXPECT_NEAR(snap.local_ssd_read.avg_latency_us, 100.0, 1.0);
    EXPECT_NEAR(snap.local_ssd_read.max_latency_us, 100.0, 1.0);

    // Channel 1: ~500 us
    EXPECT_NEAR(snap.local_ssd_write.avg_latency_us, 500.0, 1.0);
    EXPECT_NEAR(snap.local_ssd_write.max_latency_us, 500.0, 1.0);

    // Channel 2: ~1000 us
    EXPECT_NEAR(snap.net_tx_read.avg_latency_us, 1000.0, 1.0);
    EXPECT_NEAR(snap.net_tx_read.max_latency_us, 1000.0, 1.0);

    // Channel 3: ~2000 us
    EXPECT_NEAR(snap.net_rx_read.avg_latency_us, 2000.0, 1.0);
    EXPECT_NEAR(snap.net_rx_read.max_latency_us, 2000.0, 1.0);

    // Latencies are strictly increasing across channels.
    EXPECT_LT(snap.local_ssd_read.avg_latency_us, snap.local_ssd_write.avg_latency_us);
    EXPECT_LT(snap.local_ssd_write.avg_latency_us, snap.net_tx_read.avg_latency_us);
    EXPECT_LT(snap.net_tx_read.avg_latency_us, snap.net_rx_read.avg_latency_us);

    // IO counts are correct per channel.
    EXPECT_EQ(snap.local_ssd_read.io_count, 10u);
    EXPECT_EQ(snap.local_ssd_write.io_count, 10u);
    EXPECT_EQ(snap.net_tx_read.io_count, 10u);
    EXPECT_EQ(snap.net_rx_read.io_count, 10u);
}
