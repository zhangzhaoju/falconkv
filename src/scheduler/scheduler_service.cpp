#include "src/scheduler/scheduler_service.h"

#include <chrono>

#include "src/common/logging.h"

namespace falconkv {

// ---------------------------------------------------------------------------
// FalconKVScheduler
// ---------------------------------------------------------------------------

FalconKVScheduler::FalconKVScheduler(const SchedulerConfig& config)
    : config_(config) {
    // Instantiate the scheduling policy.
    if (config_.schedule_policy == "passthrough") {
        policy_ = std::make_unique<PassthroughPolicy>();
    } else {
        // Default to passthrough for any unknown policy name.
        LOG(WARNING) << "FalconKVScheduler: unknown policy '" << config_.schedule_policy
                     << "', falling back to passthrough";
        policy_ = std::make_unique<PassthroughPolicy>();
    }

    stats_ = std::make_unique<NodeStats>(config_.ssd_bw_limit_mbps,
                                         config_.net_bw_limit_mbps,
                                         config_.stats_report_interval_sec * 1000);
}

FalconKVScheduler::~FalconKVScheduler() {
    Stop();
}

Status FalconKVScheduler::Start() {
    if (running_.load(std::memory_order_acquire)) {
        return Status::OK();  // already running
    }

    running_.store(true, std::memory_order_release);

    // Start the background stats-reporting thread.
    stats_thread_ = std::thread(&FalconKVScheduler::StatsReportLoop, this);

    LOG(INFO) << "FalconKVScheduler: started with policy='" << config_.schedule_policy
              << "' uds='" << config_.uds_path << "'";

    // RPC server startup is handled externally by SchedulerServer.
    return Status::OK();
}

void FalconKVScheduler::Stop() {
    if (!running_.load(std::memory_order_acquire)) {
        return;
    }

    running_.store(false, std::memory_order_release);

    if (stats_thread_.joinable()) {
        stats_thread_.join();
    }

    LOG(INFO) << "FalconKVScheduler: stopped";
}

IOResponseData FalconKVScheduler::HandleRequestIO(const IORequestData& request) {
    // Delegate to the policy.
    return policy_->Decide(request);
}

void FalconKVScheduler::HandleIOCompletion(const IOCompletionData& report) {
    // Feed the statistics engine.
    stats_->RecordIO(report);
}

void FalconKVScheduler::StatsReportLoop() {
    while (running_.load(std::memory_order_acquire)) {
        // Sleep for the configured interval, but wake up early if stopped.
        for (int i = 0;
             i < config_.stats_report_interval_sec && running_.load();
             ++i) {
            std::this_thread::sleep_for(std::chrono::seconds(1));
        }

        if (!running_.load(std::memory_order_acquire)) break;

        // Print a periodic report.
        stats_->PrintReport();
    }
}

}  // namespace falconkv
