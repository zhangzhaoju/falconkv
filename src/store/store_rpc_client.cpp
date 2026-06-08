#include "src/store/store_rpc_client.h"

#include <brpc/controller.h>

#include "src/common/logging.h"

namespace falconkv {

StoreRpcClient::StoreRpcClient() = default;

StoreRpcClient::~StoreRpcClient() = default;

Status StoreRpcClient::Connect(const std::string& addr,
                               uint64_t max_body_size_bytes) {
    if (addr.empty()) {
        LOG(ERROR) << "[StoreRpcClient] Connect: empty store address";
        return Status::InvalidArg("empty store address");
    }

    max_body_size_bytes_ = max_body_size_bytes;

    brpc::ChannelOptions options;
    options.connect_timeout_ms = 3000;
    options.timeout_ms = 30000;       // 30s for large batch reads
    options.max_retry = 1;

    int rc = channel_.Init(addr.c_str(), &options);
    if (rc != 0) {
        LOG(ERROR) << "[StoreRpcClient] Connect: failed to init brpc channel to store at "
                   << addr << ", rc=" << rc;
        return Status::RpcError("failed to init brpc channel to store at " +
                                addr);
    }

    stub_ = std::make_unique<FalconKVStoreService_Stub>(&channel_);
    connected_ = true;
    LOG(INFO) << "[StoreRpcClient] Connected to store at " << addr;
    return Status::OK();
}

Status StoreRpcClient::Read(uint64_t offset, void* buffer, uint32_t size,
                             uint32_t client_id,
                             const std::string& source_node_addr) {
    if (!connected_) {
        LOG(ERROR) << "[StoreRpcClient] Read: not connected";
        return Status::RpcError("not connected");
    }

    ReadRequest request;
    request.set_offset(offset);
    request.set_size(size);
    request.set_client_id(client_id);
    request.set_source_node_addr(source_node_addr);

    ReadResponse response;
    brpc::Controller cntl;

    stub_->Read(&cntl, &request, &response, nullptr);

    if (cntl.Failed()) {
        LOG(ERROR) << "[StoreRpcClient] Read RPC failed at offset " << offset
                   << ", size=" << size << ": " << cntl.ErrorText();
        return Status::RpcError("Store Read RPC failed: " +
                                std::string(cntl.ErrorText()));
    }

    if (response.status() != 0) {
        LOG(ERROR) << "[StoreRpcClient] Read failed at offset " << offset
                   << " with status=" << response.status();
        return Status::IoError("Store Read failed with status: " +
                               std::to_string(response.status()));
    }

    uint32_t bytes_read = response.bytes_read();
    if (bytes_read > size) {
        bytes_read = size;
    }

    // Read data from brpc attachment (zero-copy path from server)
    if (bytes_read > 0) {
        cntl.response_attachment().copy_to(buffer, bytes_read);
    }
    return Status::OK();
}

Status StoreRpcClient::BatchRead(const std::vector<uint64_t>& offsets,
                                 const std::vector<uint32_t>& sizes,
                                 const std::vector<void*>& buffers,
                                 std::vector<int32_t>& results,
                                 const std::string& source_node_addr) {
    if (!connected_) {
        LOG(ERROR) << "[StoreRpcClient] BatchRead: not connected";
        return Status::RpcError("not connected");
    }

    size_t n = offsets.size();
    if (n != sizes.size() || n != buffers.size()) {
        LOG(ERROR) << "[StoreRpcClient] BatchRead: size mismatch, offsets="
                   << n << " sizes=" << sizes.size()
                   << " buffers=" << buffers.size();
        return Status::InvalidArg("BatchRead parameter size mismatch");
    }

    results.assign(n, 0);

    if (n == 0) {
        return Status::OK();
    }

    // Effective limit: reserve 10% for protobuf overhead
    const uint64_t effective_limit =
        max_body_size_bytes_ * 9 / 10;

    // Greedy packing: split segments into sub-batches by accumulated data size
    struct SubBatch {
        std::vector<size_t> original_indices;
        uint64_t total_data_size = 0;
    };

    std::vector<SubBatch> sub_batches;
    SubBatch current;
    for (size_t i = 0; i < n; ++i) {
        uint64_t seg_size = sizes[i];
        // If adding this segment exceeds the limit and current batch is non-empty,
        // finalize current batch and start a new one.
        if (!current.original_indices.empty() &&
            current.total_data_size + seg_size > effective_limit) {
            sub_batches.push_back(std::move(current));
            current = SubBatch();
        }
        // Single oversized segment still goes into its own batch (may fail)
        current.original_indices.push_back(i);
        current.total_data_size += seg_size;
    }
    if (!current.original_indices.empty()) {
        sub_batches.push_back(std::move(current));
    }

    if (sub_batches.size() > 1) {
        LOG(INFO) << "[StoreRpcClient] BatchRead: splitting " << n
                  << " segments into " << sub_batches.size()
                  << " sub-batches (effective_limit="
                  << effective_limit << " bytes)";
    }

    // Execute each sub-batch as an independent RPC
    bool any_rpc_failed = false;
    for (const auto& batch : sub_batches) {
        const auto& indices = batch.original_indices;
        size_t batch_n = indices.size();

        BatchReadRequest request;
        for (size_t j = 0; j < batch_n; ++j) {
            auto* seg = request.add_segments();
            seg->set_offset(offsets[indices[j]]);
            seg->set_size(sizes[indices[j]]);
        }
        if (!source_node_addr.empty()) {
            request.set_source_node_addr(source_node_addr);
        }

        BatchReadResponse response;
        brpc::Controller cntl;

        stub_->BatchRead(&cntl, &request, &response, nullptr);

        if (cntl.Failed()) {
            LOG(ERROR) << "[StoreRpcClient] BatchRead RPC failed (sub-batch of "
                       << batch_n << " segments): " << cntl.ErrorText();
            for (size_t j = 0; j < batch_n; ++j) {
                results[indices[j]] = -1;
            }
            any_rpc_failed = true;
            continue;
        }

        if (response.status() != 0) {
            LOG(ERROR) << "[StoreRpcClient] BatchRead failed with status="
                       << response.status();
            for (size_t j = 0; j < batch_n; ++j) {
                results[indices[j]] = -1;
            }
            any_rpc_failed = true;
            continue;
        }

        int seg_count = response.bytes_read_size();
        if (static_cast<size_t>(seg_count) != batch_n) {
            LOG(ERROR) << "[StoreRpcClient] BatchRead: sub-batch expected "
                       << batch_n << " segments, got " << seg_count;
            for (size_t j = 0; j < batch_n; ++j) {
                results[indices[j]] = -1;
            }
            any_rpc_failed = true;
            continue;
        }

        // Read data from brpc attachment
        size_t att_offset = 0;
        for (int j = 0; j < seg_count; ++j) {
            size_t orig_idx = indices[j];
            uint32_t bytes_read = response.bytes_read(j);
            if (bytes_read == 0) {
                results[orig_idx] = -1;
                continue;
            }
            uint32_t to_copy = std::min(bytes_read, sizes[orig_idx]);
            cntl.response_attachment().copy_to(buffers[orig_idx], to_copy,
                                               att_offset);
            att_offset += bytes_read;
            results[orig_idx] = static_cast<int32_t>(to_copy);
        }
    }

    if (any_rpc_failed) {
        return Status::RpcError("Store BatchRead: one or more sub-batches failed");
    }

    return Status::OK();
}

Status StoreRpcClient::Ping() {
    if (!connected_) {
        LOG(ERROR) << "[StoreRpcClient] Ping: not connected";
        return Status::RpcError("not connected");
    }

    PingRequest request;
    PongResponse response;
    brpc::Controller cntl;

    stub_->Ping(&cntl, &request, &response, nullptr);

    if (cntl.Failed()) {
        LOG(ERROR) << "[StoreRpcClient] Ping RPC failed: " << cntl.ErrorText();
        return Status::RpcError("Store Ping RPC failed: " +
                                std::string(cntl.ErrorText()));
    }

    return Status::OK();
}

} // namespace falconkv
