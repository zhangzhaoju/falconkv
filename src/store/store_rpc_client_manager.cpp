#include "src/store/store_rpc_client_manager.h"

#include "src/common/logging.h"

namespace falconkv {

StoreRpcClientManager::~StoreRpcClientManager() {
    CloseAll();
}

StoreRpcClient* StoreRpcClientManager::GetOrCreate(const std::string& addr) {
    if (addr.empty()) {
        return nullptr;
    }

    std::lock_guard<std::mutex> lock(mutex_);

    auto it = clients_.find(addr);
    if (it != clients_.end() && it->second->IsConnected()) {
        return it->second.get();
    }

    // Create a new client
    auto client = std::make_unique<StoreRpcClient>();
    Status s = client->Connect(addr, max_body_size_bytes_);
    if (!s.ok()) {
        return nullptr;
    }

    auto* raw = client.get();
    LOG(INFO) << "[StoreRpcClientManager] Created new StoreRpcClient for " << addr;
    clients_[addr] = std::move(client);
    return raw;
}

void StoreRpcClientManager::SetMaxBodySize(uint64_t max_body_size_bytes) {
    max_body_size_bytes_ = max_body_size_bytes;
}

void StoreRpcClientManager::CloseAll() {
    std::lock_guard<std::mutex> lock(mutex_);
    clients_.clear();
}

} // namespace falconkv
