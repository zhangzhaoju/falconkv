#pragma once

#include <string>
#include <unordered_map>
#include <mutex>
#include <memory>

#include "src/store/store_rpc_client.h"

namespace falconkv {

/// Connection pool for StoreRpcClient instances, cached by address.
class StoreRpcClientManager {
public:
    StoreRpcClientManager() = default;
    ~StoreRpcClientManager();

    // Non-copyable
    StoreRpcClientManager(const StoreRpcClientManager&) = delete;
    StoreRpcClientManager& operator=(const StoreRpcClientManager&) = delete;

    /// Get or create a StoreRpcClient for the given address.
    /// Returns nullptr if connection fails.
    StoreRpcClient* GetOrCreate(const std::string& addr);

    /// Close all cached clients.
    void CloseAll();

    /// Set the BRPC max body size (in bytes) for all future connections.
    void SetMaxBodySize(uint64_t max_body_size_bytes);

private:
    std::mutex mutex_;
    std::unordered_map<std::string, std::unique_ptr<StoreRpcClient>> clients_;
    uint64_t max_body_size_bytes_ = 512ULL * 1024 * 1024;
};

} // namespace falconkv
