#pragma once
#include <string>
#include <memory>
#include <thread>
#include <atomic>
#include "src/common/status.h"
#include "src/common/config.h"
#include "src/scheduler/passthrough_policy.h"
#include "src/scheduler/node_stats.h"

namespace falconkv {

// ---------------------------------------------------------------------------
// FalconKVScheduler
//
// The central scheduling daemon.  It owns a scheduling policy and a statistics
// engine.  Clients/stores can call HandleRequestIO / HandleIOCompletion
// directly (in-process) or via RPC.
// ---------------------------------------------------------------------------
class FalconKVScheduler {
public:
    explicit FalconKVScheduler(const SchedulerConfig& config);
    ~FalconKVScheduler();

    /// Start the scheduler (including the background stats-reporting thread).
    Status Start();

    /// Gracefully shut down.
    void Stop();

    /// Direct in-process API -- bypasses RPC.
    IOResponseData HandleRequestIO(const IORequestData& request);
    void HandleIOCompletion(const IOCompletionData& report);

private:
    /// Background loop that periodically prints statistics.
    void StatsReportLoop();

    SchedulerConfig config_;
    std::unique_ptr<IOSchedulePolicy> policy_;
    std::unique_ptr<NodeStats> stats_;
    std::thread stats_thread_;
    std::atomic<bool> running_{false};
};

}  // namespace falconkv
