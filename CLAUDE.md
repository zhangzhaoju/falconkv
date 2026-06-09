# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

FalconKV is a distributed KV storage system designed for LLM inference acceleration. It provides persistent KV cache storage for LMCache/Mooncake frameworks, offering low-latency batch operations backed by SSD. The system is implemented in C++17 with planned Python bindings for LMCache integration.

Data flow:
- **Write**: `vLLM → LMCache/Mooncake → FalconKV Client → local_store_.BatchPut() → SSD (DirectIO) + MetaSyncClient → Meta`
- **Read**: `vLLM → LMCache/Mooncake → FalconKV Client → local_store_ / NodeLocalAccessor / StoreRpcClient → SSD`

## Build & Development Commands

```bash
./build.sh                                    # Full Release build
./build.sh build falconkv --debug             # Debug build
./build.sh build --with-python                # Build with Python bindings
./build.sh test                               # Run all unit tests (via ctest)
./build.sh install                            # Install to /usr/local/falconkv
./build.sh clean                              # Clean build directory

# Run a specific test
cd build && ctest --output-on-failure -R test_slot_allocator

# Manual build
mkdir -p build && cd build
cmake -G Ninja -DCMAKE_BUILD_TYPE=Release ..
ninja -j$(nproc)
```

Build outputs go to `build/`. GTest is auto-fetched via FetchContent if not installed. `compile_commands.json` is generated in the build directory.

## Architecture

### Module Layout

```
src/
├── client/     Client API: FalconKVClientImpl with batch Exist/Put/Get (binds to local_store_)
├── common/     Status codes, config loader (FalconKVConfig), aligned allocator, SlotAllocator, logging
├── meta/       Metadata aggregation service: MetaManager, sharded in-memory store with per-shard read-write locks, standalone `falconkv_master` process
├── store/      SSD storage: FalconKVStore with DirectIO, StoreMetaIndex, MetaSyncClient, EvictManager
├── transfer/   RPC layer: TransferManager, TransferChannel (abstract), BrpcChannel
└── scheduler/  IO scheduling: FalconKVScheduler, IOSchedulePolicy, NodeStats (per-channel latency)
```

### Batch Operation Flows

**BatchExist** (two-step query, no cache): Client queries `local_store_->BatchContains()` → Client queries Meta via `BatchExist` RPC for missing keys. KeyDescCache is NOT queried during BatchExist (to avoid stale entries from evicted data), but Meta results are cached for subsequent BatchGet.

**BatchPut** (local store write): Client calls `local_store_->BatchPut()` which internally allocates space via SlotAllocator, writes data via DirectIO, records metadata in StoreMetaIndex, and asynchronously syncs to Meta via MetaSyncClient. Client is not aware of offset or space allocation.

**BatchGet** (three-level read affinity): Client uses cached `KeyDescriptor`s → reads data based on `AccessType`: local store in-process (Level 0), NodeLocalAccessor DirectIO for same-node stores (Level 1), or StoreRpcClient for remote stores (Level 2).

### Three-Level Read Affinity

`AccessType` enum in `src/common/types.h` determines how data is read:

- `ACCESS_LOCAL_DIRECT` (0): Same-process Store, `local_store_->Get()` in-process call
- `ACCESS_NODE_DIRECT` (1): Same-node Store, NodeLocalAccessor DirectIO via FdCache
- `ACCESS_REMOTE_RPC` (2): Remote Store, StoreRpcClient RPC via TransferChannel

Writes always go to the bound local store (`ACCESS_LOCAL_DIRECT`). The three-level affinity is only for reads.

### Store Self-Management

Each Store manages its own space and eviction:
- **SlotAllocator** (`src/common/slot_allocator.h`): Allocates/frees SSD space in fixed-size chunks
- **StoreMetaIndex** (`src/store/store_meta_index.h`): Local key→offset hash index for fast lookups
- **MetaSyncClient** (`src/store/meta_sync_client.h`): Syncs metadata changes (SyncCommit/SyncRemove) to Meta with automatic reconnection and full resync on reconnect
- **MetaRpcClient** (`src/meta/meta_rpc_client.h`): Client-side Meta RPC wrapper with disconnect tolerance — returns empty results when Meta is unreachable, auto-reconnects in background
- **EvictManager**: Store-internal eviction based on access time when usage exceeds high watermark
- **PendingEvictQueue**: Grace period queue (5s) — evicts metadata from Meta first, then delays local space reclamation to prevent stale reads from other nodes

### Data State Machine

`StoreKeyRecord.stat` in StoreMetaIndex: `0 (writing)` → `1 (committed)`. Evicted records are removed from the index and synced to Meta via `SyncRemove`.

### Scheduler

`FalconKVScheduler` coordinates IO within a node. Clients request IO permission and report completion. `PassthroughPolicy` currently admits all requests immediately. The scheduler collects per-channel bandwidth and latency statistics via `NodeStats`. If the scheduler becomes unavailable, clients bypass it (configurable via `scheduler_enabled`). Communication is via Unix domain socket (`/tmp/falconkv_scheduler.sock`).

### Transfer Layer

`TransferChannel` is the abstract RPC interface (`transfer_channel.h`). `BrpcChannel` is the brpc implementation. `TransferManager` maintains connection pools: one channel to meta server, per-store-id channels for data transfer.

### Configuration

Single JSON config file (`config/falconkv.json`) loaded by `ConfigLoader`. Top-level sections map to `MetaConfig`, `StoreConfig`, `SchedulerConfig`, `ClientConfig`, `TransferConfig` structs in `src/common/config.h`.

### Protobuf Services

`proto/falconkv_meta.proto`: `FalconKVMetaService` — BatchExist, BatchLookup, SyncCommit, SyncRemove, StoreRegister, ClientHeartbeat.

`proto/falconkv_store.proto`: Store data read/write RPCs.

`proto/falconkv_scheduler.proto`: Scheduler IO request/completion RPCs.

## Code Style

- C++17, `-Wall -Wno-shadow`
- Header guards: `#pragma once`
- Namespace: `falconkv`
- Classes: PascalCase; methods: PascalCase; variables: snake_case
- Status returned as `Status` object (not exceptions)
- Tests use GTest, located in `tests/unit/{client,meta,store,transfer,scheduler}/`

## Key Types

- `Status` (`src/common/status.h`): Error codes — kOk, kIOError, kNotFound, kRpcError, kTimeout, kNoSpace, etc.
- `KeyDescriptor` (`src/client/key_desc_cache.h`): key metadata for cached lookups (store_id, offset, size, access_type)
- `StoreKeyRecord` (`src/store/store_meta_index.h`): Store-internal key record (offset, size, stat, access_time)
- `SlotAllocator` (`src/common/slot_allocator.h`): SSD space management for fixed-size chunks
- `FalconKVConfig` (`src/common/config.h`): aggregated config from all modules
