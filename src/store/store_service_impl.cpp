#include "src/store/store_service_impl.h"
#include "src/store/store_meta_index.h"

#include <brpc/closure_guard.h>
#include <brpc/controller.h>

#include <cstring>
#include <unordered_map>
#include "src/common/time_util.h"

namespace falconkv {

StoreServiceImpl::StoreServiceImpl(FalconKVStore* store)
    : store_(store) {}

// -----------------------------------------------------------------
// Read (offset-based, for remote client backward compatibility)
// -----------------------------------------------------------------
void StoreServiceImpl::Read(::google::protobuf::RpcController* controller,
                             const ReadRequest* request,
                             ReadResponse* response,
                             ::google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);

    uint64_t offset = request->offset();
    uint32_t size = request->size();

    // Allocate aligned buffer for DirectIO read
    void* buffer = AlignedAllocator::Allocate(512, size);
    if (!buffer) {
        response->set_status(-1);
        return;
    }

    uint64_t request_ts_ns = GetCurrentTimeNs();
    Status s = store_->Read(offset, buffer, size);
    uint64_t done_ts_ns = GetCurrentTimeNs();

    if (!s.ok()) {
        AlignedAllocator::Free(buffer);
        response->set_status(static_cast<int>(s.code()));
        return;
    }

    // Report to Scheduler (Store perspective: NET_RX_READ)
    if (store_->scheduler_proxy()) {
        store_->scheduler_proxy()->StoreReportIOAsync(
            store_->store_id(),
            3,  // NET_RX_READ
            request->client_id(),
            size,
            request_ts_ns,
            done_ts_ns,
            request->source_node_addr());
    }

    response->set_status(0);
    response->set_bytes_read(size);

    // Zero-copy: transfer buffer ownership to brpc attachment.
    // The deleter frees the buffer after brpc finishes sending.
    auto* cntl = static_cast<brpc::Controller*>(controller);
    cntl->response_attachment().append_user_data(
        buffer, size, AlignedAllocator::Free);
}

// -----------------------------------------------------------------
// BatchRead (offset-based)
// -----------------------------------------------------------------
void StoreServiceImpl::BatchRead(::google::protobuf::RpcController* controller,
                                  const BatchReadRequest* request,
                                  BatchReadResponse* response,
                                  ::google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);

    std::vector<ReadItem> items;
    std::vector<std::unique_ptr<void, void(*)(void*)>> buffers;

    items.reserve(request->segments_size());

    for (int i = 0; i < request->segments_size(); ++i) {
        const auto& seg = request->segments(i);
        uint32_t size = seg.size();

        void* buf = AlignedAllocator::Allocate(512, size);
        if (!buf) {
            response->set_status(-1);
            return;
        }
        buffers.emplace_back(buf, &AlignedAllocator::Free);
        items.push_back({seg.offset(), buf, size});
    }

    uint64_t total_io_size = 0;
    for (const auto& item : items) total_io_size += item.size;

    uint64_t request_ts_ns = GetCurrentTimeNs();
    Status s = store_->BatchRead(items);
    uint64_t done_ts_ns = GetCurrentTimeNs();

    // Report to Scheduler (Store perspective: NET_RX_READ)
    if (store_->scheduler_proxy() && s.ok()) {
        store_->scheduler_proxy()->StoreReportIOAsync(
            store_->store_id(),
            3,  // NET_RX_READ
            0,  // client_id not available in BatchReadRequest
            total_io_size,
            request_ts_ns,
            done_ts_ns,
            request->source_node_addr());
    }

    response->set_status(s.ok() ? 0 : static_cast<int>(s.code()));
    if (!s.ok()) {
        for (size_t i = 0; i < items.size(); ++i) {
            response->add_bytes_read(0);
        }
        return;
    }

    // Fill bytes_read metadata.
    for (size_t i = 0; i < items.size(); ++i) {
        response->add_bytes_read(items[i].size);
    }

    // Zero-copy: transfer each buffer's ownership to brpc attachment.
    // Layout: [seg0_data][seg1_data]...[segN_data] concatenated sequentially.
    // Client uses bytes_read[] to locate each segment's offset.
    auto* cntl = static_cast<brpc::Controller*>(controller);
    for (size_t i = 0; i < items.size(); ++i) {
        cntl->response_attachment().append_user_data(
            buffers[i].release(), items[i].size, AlignedAllocator::Free);
    }
}

// -----------------------------------------------------------------
// GetByKey (key-based read)
// -----------------------------------------------------------------
void StoreServiceImpl::GetByKey(::google::protobuf::RpcController*,
                                 const GetByKeyRequest* request,
                                 GetByKeyResponse* response,
                                 ::google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);

    const std::string& key = request->key();

    // First look up the key to get its actual size.
    std::vector<StoreKeyRecord> hits;
    std::vector<std::string> misses;
    store_->BatchContains({key}, hits, misses);
    if (hits.empty()) {
        response->set_status(static_cast<int>(Status::kNotFound));
        return;
    }

    uint32_t data_size = hits[0].size;
    void* buffer = AlignedAllocator::Allocate(512, data_size);
    if (!buffer) {
        response->set_status(-1);
        return;
    }

    uint64_t request_ts_ns = GetCurrentTimeNs();
    auto result = store_->Get(key, buffer, data_size);
    uint64_t done_ts_ns = GetCurrentTimeNs();

    if (!result.status.ok()) {
        AlignedAllocator::Free(buffer);
        response->set_status(static_cast<int>(result.status.code()));
        return;
    }

    // Report to Scheduler (Store perspective: NET_RX_READ)
    if (store_->scheduler_proxy()) {
        store_->scheduler_proxy()->StoreReportIOAsync(
            store_->store_id(),
            3,  // NET_RX_READ
            request->client_id(),
            result.size,
            request_ts_ns,
            done_ts_ns,
            request->source_node_addr());
    }

    response->set_status(0);
    response->set_data(buffer, result.size);
    response->set_size(result.size);
    AlignedAllocator::Free(buffer);
}

// -----------------------------------------------------------------
// BatchGetByKey (key-based batch read)
// -----------------------------------------------------------------
void StoreServiceImpl::BatchGetByKey(::google::protobuf::RpcController*,
                                      const BatchGetByKeyRequest* request,
                                      BatchGetByKeyResponse* response,
                                      ::google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);

    // Batch look up all keys to get their actual sizes.
    std::vector<std::string> keys;
    keys.reserve(request->keys_size());
    for (int i = 0; i < request->keys_size(); ++i) {
        keys.push_back(request->keys(i));
    }

    std::vector<StoreKeyRecord> hits;
    std::vector<std::string> misses;
    store_->BatchContains(keys, hits, misses);

    // Build a set of found keys with their sizes for quick lookup.
    std::unordered_map<std::string, uint32_t> key_sizes;
    for (const auto& rec : hits) {
        key_sizes[rec.key] = rec.size;
    }

    bool all_ok = true;

    for (int i = 0; i < request->keys_size(); ++i) {
        const std::string& key = request->keys(i);

        auto it = key_sizes.find(key);
        if (it == key_sizes.end()) {
            response->add_data_segments();
            response->add_sizes(0);
            response->add_statuses(static_cast<int>(Status::kNotFound));
            all_ok = false;
            continue;
        }

        uint32_t data_size = it->second;
        void* buffer = AlignedAllocator::Allocate(512, data_size);
        if (!buffer) {
            response->add_data_segments();
            response->add_sizes(0);
            response->add_statuses(-1);
            all_ok = false;
            continue;
        }

        uint64_t request_ts_ns = GetCurrentTimeNs();
        auto result = store_->Get(key, buffer, data_size);
        uint64_t done_ts_ns = GetCurrentTimeNs();

        if (!result.status.ok()) {
            AlignedAllocator::Free(buffer);
            response->add_data_segments();
            response->add_sizes(0);
            response->add_statuses(static_cast<int>(result.status.code()));
            all_ok = false;
            continue;
        }

        // Report to Scheduler (Store perspective: NET_RX_READ)
        if (store_->scheduler_proxy()) {
            store_->scheduler_proxy()->StoreReportIOAsync(
                store_->store_id(),
                3,  // NET_RX_READ
                request->client_id(),
                result.size,
                request_ts_ns,
                done_ts_ns,
                request->source_node_addr());
        }

        response->add_data_segments(buffer, result.size);
        response->add_sizes(result.size);
        response->add_statuses(0);
        AlignedAllocator::Free(buffer);
    }

    response->set_status(all_ok ? 0 : 1);
}

// -----------------------------------------------------------------
// Ping
// -----------------------------------------------------------------
void StoreServiceImpl::Ping(::google::protobuf::RpcController*,
                             const PingRequest* request,
                             PongResponse* response,
                             ::google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);

    response->set_status(0);
    response->set_timestamp_ns(request->timestamp_ns());
}

} // namespace falconkv
