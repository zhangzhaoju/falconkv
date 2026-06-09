# FalconKV 测试设计文档

## 1. 概述

本文档描述 FalconKV 项目的测试体系。项目采用三级测试：**单元测试（UT）**、**模块测试**、**集成测试**，覆盖 Client / Meta / Store / Transfer / IO Scheduler 五大模块。

### 1.1 测试层次定义

| 层次 | 范围 | 依赖 | 运行环境 |
|------|------|------|----------|
| **单元测试 (UT)** | 单个类/函数，逻辑验证 | 仅被测单元，无外部进程 | 开发机，CI |
| **模块测试** | 单模块多组件协同，或跨模块 RPC 交互 | 真实 brpc Server（进程内） | 开发机，CI |
| **集成测试** | 多进程端到端协同，全链路验证 | 启动 Meta / Scheduler / Store 进程 | 开发机，CI |

### 1.2 测试矩阵总览

| 模块 | UT | 模块测试 | 集成测试 |
|------|----|----------|----------|
| Client | 22 | 6 | 10 (LMCache Connector) + 12 (Functional) |
| Meta | 24 | 5 | 参与全链路 |
| Store | 30 | 5 | 参与全链路 |
| Transfer | 14 | 5 | 参与全链路 |
| Scheduler | 25 | 4 | 7 |
| Bypass | 10 | 3 | 2 |
| 服务启动 | - | - | 6 |
| LMCache 适配 | - | 4 | - |
| **合计** | **125** | **32** | **37** |

---

## 2. Client 模块测试

### 2.1 单元测试

#### 2.1.1 KeyDescCache (`tests/unit/client/test_key_desc_cache.cpp`)

| 编号 | 测试项 | 前置条件 | 测试步骤 | 预期结果 |
|------|--------|----------|----------|----------|
| UT-C-001 | 单 key 查找命中 | 缓存中已插入 key1 的描述 | `Lookup("key1")` | 返回正确的 KeyDescriptor |
| UT-C-002 | 单 key 查找未命中 | 缓存为空 | `Lookup("key_not_exist")` | 返回 `std::nullopt` |
| UT-C-003 | 批量查找部分命中 | 缓存中含 key1, key2 | `BatchLookup([key1, key2, key3])` | hit_count=2, missing_keys=[key3] |
| UT-C-004 | 批量查找全部命中 | 缓存中含所有 key | `BatchLookup` | hit_descs 包含全部 key |
| UT-C-005 | 批量查找全部未命中 | 缓存为空 | `BatchLookup` | hit_descs 为空 |
| UT-C-006 | 批量插入与验证 | 插入多个 key | `BatchInsert` 后逐个 `Lookup` | 全部可查到 |
| UT-C-007 | 容量淘汰（LRU） | 缓存容量=3 | 插入 5 个 key | 仅保留最新 3 个，最早的 2 个被淘汰 |
| UT-C-008 | LRU 更新（Lookup 提升） | 容量=3，已满 | Lookup 最早 key 后插入新 key | 最早 key 保留，中间 key 被淘汰 |
| UT-C-009 | 批量失效 | 缓存中含 key1~key5 | `BatchInvalidate([key2, key4])` | key2/key4 查找返回 nullopt，其余正常 |
| UT-C-010 | 失效不存在的 key | 缓存中无该 key | `Invalidate` | 无异常 |
| UT-C-011 | 并发读写安全 | 多线程 | 线程 A 持续插入，线程 B 持续查找 | 无 crash，无数据竞争 |
| UT-C-012 | Clear 清空 | 缓存中有多个 key | `Clear()` | `Size()` 返回 0 |

#### 2.1.2 FdCache (`tests/unit/store/test_fd_cache.cpp`)

| 编号 | 测试项 | 前置条件 | 测试步骤 | 预期结果 |
|------|--------|----------|----------|----------|
| UT-C-013 | GetFd 创建新 fd | 文件存在 | 首次调用 `GetFd` | 返回有效 fd (>0) |
| UT-C-014 | GetFd 复用已有 fd | 已调用过一次 | 再次调用 `GetFd` 同一路径 | 返回相同 fd |
| UT-C-015 | 不同路径不同 fd | 两个不同文件路径 | 分别调用 `GetFd` | 返回不同 fd |
| UT-C-016 | 空闲 fd 淘汰 | fd 空闲超过阈值 | 调用 `EvictIdle` | 长时间未访问的 fd 被 close |
| UT-C-017 | 活跃 fd 不被淘汰 | fd 刚被访问 | 调用 `EvictIdle` | 活跃 fd 未被 close |
| UT-C-018 | CloseAll 清理 | 缓存中有多个 fd | `CloseAll()` | 所有 fd 被 close |
| UT-C-019 | 不存在文件返回无效 | 文件路径不存在 | `GetFd` | 返回 -1 |

#### 2.1.3 AllocPolicy (`tests/unit/client/test_alloc_policy.cpp`)

| 编号 | 测试项 | 前置条件 | 测试步骤 | 预期结果 |
|------|--------|----------|----------|----------|
| UT-C-020 | AccessType 枚举值 | - | 检查枚举值 | LOCAL=0, NODE=1, REMOTE=2 |
| UT-C-021 | Level 0 本地分配 | local_store 使用率 50% | Allocate | access_type=ACCESS_LOCAL_DIRECT |
| UT-C-022 | Level 1 同节点分配 | local 满，node_store 使用率低 | Allocate | access_type=ACCESS_NODE_DIRECT |
| UT-C-023 | Level 2 远程分配 | 所有同节点 Store 满 | Allocate | access_type=ACCESS_REMOTE_RPC |
| UT-C-024 | 全部 Store 满载 | 所有 Store 使用率 > 70% | Allocate | 返回 ENOSPC |

#### 2.1.4 KeyDescriptor / BufferInfo 数据结构 (`tests/unit/client/test_data_structures.cpp`)

| 编号 | 测试项 | 前置条件 | 测试步骤 | 预期结果 |
|------|--------|----------|----------|----------|
| UT-C-025 | AccessType 枚举正确性 | - | 检查值 | 符合设计 |
| UT-C-026 | KeyDescriptor 默认 access_type | - | 默认构造 | ACCESS_REMOTE_RPC |
| UT-C-027 | KeyDescriptor 字段赋值 | - | 构造并赋值各字段 | 所有字段正确可读 |
| UT-C-028 | AllocResult 默认值 | - | 默认构造 | 各字段为默认初始值 |
| UT-C-029 | AllocResult AccessType 枚举 | - | 赋值检查 | 枚举值正确 |
| UT-C-030 | AllocResult 字段赋值 | - | 赋值后读取 | 一致 |
| UT-C-031 | BufferInfo 字段 | - | 赋值 data_ptr/size | 一致 |
| UT-C-032 | BufferInfo 默认初始化 | - | 默认构造 | data_ptr=nullptr, size=0 |

### 2.2 模块测试 (`tests/module/test_client_batch_ops.cpp`)

| 编号 | 测试项 | 前置条件 | 测试步骤 | 预期结果 |
|------|--------|----------|----------|----------|
| MT-C-001 | BatchExist 缓存优先 | KeyDescCache 含部分 key | 调用 BatchExist | 缓存命中不发 RPC |
| MT-C-002 | BatchPut L0 本地写入 | 启动真实 Store + Meta | BatchPut | 数据写入本地 SSD |
| MT-C-003 | BatchGet L0 本地读取 | 数据已写入本地 | BatchGet | 读出数据与写入一致 |
| MT-C-004 | BatchGet NodeDirect | 跨 Store 同节点读取 | BatchGet via ACCESS_NODE_DIRECT | 通过 DirectIO 成功读取 |
| MT-C-005 | BatchPut 空间不足 | capacity=极小值 | 写入超出容量 | 返回错误，已有数据不受影响 |
| MT-C-006 | BatchGet RPC 降级 | 远程 Store 连接 | BatchGet via ACCESS_REMOTE_RPC | 通过 Store RPC 成功读取 |

---

## 3. Meta 模块测试

### 3.1 单元测试

#### 3.1.1 SlotAllocator (`tests/unit/meta/test_slot_allocator.cpp`)

| 编号 | 测试项 | 前置条件 | 测试步骤 | 预期结果 |
|------|--------|----------|----------|----------|
| UT-M-001 | 分配返回有效 offset | 初始化 1GB 空间 | `AllocChunk()` | 返回有效 offset |
| UT-M-002 | 分配页面对齐 | - | `AllocChunk()` | offset 按 page_size 对齐 |
| UT-M-003 | 分配耗尽 | 所有空间已分配 | `AllocChunk()` | 返回 -1（ENOSPC） |
| UT-M-004 | 释放后可重新分配 | 分配后释放 | `FreeChunk` → `AllocChunk` | 重新分配成功 |
| UT-M-005 | 释放后可重分配 | 分配两个 slot | 释放两个 slot | 重分配成功 |
| UT-M-006 | 使用率计算 | 1GB 总量，分配 512MB | `GetUsageRatio()` | 返回 0.5 |
| UT-M-007 | 反复分配释放无泄漏 | 初始化 1GB | 循环 Alloc+Free 10000 次 | used_pages 最终归零 |
| UT-M-008 | 全部分配释放后使用率为零 | 分配全部后释放 | `GetUsageRatio()` | 返回 0.0 |
| UT-M-009 | GetTotalBytes | - | 调用 | 返回配置的总量 |
| UT-M-010 | GetUsedBytes | 分配 N 个 chunk | 调用 | 返回已使用字节数 |
| UT-M-011 | 释放无效 offset | offset=-1 | `FreeChunk` | 无异常 |
| UT-M-012 | chunk_pages 和 page_size | - | `GetChunkPages` / `GetPageSize` | 与配置一致 |

#### 3.1.2 MetaSyncCommit (`tests/unit/meta/test_meta_sync_commit.cpp`)

| 编号 | 测试项 | 前置条件 | 测试步骤 | 预期结果 |
|------|--------|----------|----------|----------|
| UT-M-013 | SyncCommit 插入记录 | MetaManager 初始化 | SyncCommit 一条记录 | BatchExist 可查到 |
| UT-M-014 | SyncCommit 更新已有记录 | 已有一条记录 | SyncCommit 同 key 新 offset | offset 更新 |
| UT-M-015 | SyncCommit 更新使用率 | Store 注册 | SyncCommit 后查使用率 | 使用率正确 |
| UT-M-016 | SyncRemove 删除记录 | 已有记录 | SyncRemove | BatchExist 查不到 |
| UT-M-017 | SyncRemove 不存在的 key | 无该 key | SyncRemove | 无异常 |
| UT-M-018 | SyncCommit 跳过空 key | key 为空 | SyncCommit | 不插入 |

#### 3.1.3 MetaServiceImpl (`tests/unit/meta/test_meta_service_impl.cpp`)

| 编号 | 测试项 | 前置条件 | 测试步骤 | 预期结果 |
|------|--------|----------|----------|----------|
| UT-M-019 | StoreRegister 成功 | Meta 服务运行 | 发送 StoreRegister RPC | 注册成功 |
| UT-M-020 | SyncCommit 后 BatchExist | 已注册 Store | SyncCommit → BatchExist | 返回正确 key |
| UT-M-021 | BatchLookup | 已有数据 | BatchLookup | 返回完整描述 |
| UT-M-022 | SyncRemove | 已有数据 | SyncRemove → BatchExist | 查不到已删除 key |

#### 3.1.4 MetaRpcClient (`tests/unit/meta/test_meta_rpc_client.cpp`)

| 编号 | 测试项 | 前置条件 | 测试步骤 | 预期结果 |
|------|--------|----------|----------|----------|
| UT-M-023 | BatchExist 空列表 | - | BatchExist([]) | 返回空结果 |
| UT-M-024 | BatchExist SyncCommit 后 | SyncCommit 已提交 | BatchExist | 返回正确 key |
| UT-M-025 | BatchLookup | 已有数据 | BatchLookup | 返回完整描述 |
| UT-M-026 | BatchExist SyncRemove 后 | SyncRemove 已删除 | BatchExist | 查不到 |

#### 3.1.5 MetaSyncClient (`tests/unit/store/test_meta_sync_client.cpp`)

| 编号 | 测试项 | 前置条件 | 测试步骤 | 预期结果 |
|------|--------|----------|----------|----------|
| UT-M-027 | 空地址连接 | addr 为空 | Connect | 不 crash |
| UT-M-028 | 未连接时跳过 SyncCommit | connected=false | SyncCommit | 不发送 RPC |
| UT-M-029 | 未连接时跳过 SyncRemove | connected=false | SyncRemove | 不发送 RPC |
| UT-M-030 | 未连接时跳过 RegisterStore | connected=false | RegisterStore | 不发送 RPC |

### 3.2 模块测试 (`tests/module/test_meta_rpc_service.cpp`)

| 编号 | 测试项 | 前置条件 | 测试步骤 | 预期结果 |
|------|--------|----------|----------|----------|
| MT-M-001 | StoreRegister | 启动真实 Meta brpc Server | RPC 注册 | 成功 |
| MT-M-002 | BatchExist 批量查询 | 已插入多条记录 | BatchExist RPC | 返回正确 hit 列表 |
| MT-M-003 | SyncCommit 使数据可见 | SyncCommit → BatchExist | 数据可查到 |
| MT-M-004 | BatchLookup 返回所有未驱逐 | 含 stat=1 和 stat=2 的记录 | BatchLookup | 仅返回 stat=1 |
| MT-M-005 | SyncRemove 删除 key | SyncRemove → BatchExist | 查不到已删除 key |

---

## 4. Store 模块测试

### 4.1 单元测试

#### 4.1.1 DirectIO (`tests/unit/store/test_direct_io.cpp`)

| 编号 | 测试项 | 前置条件 | 测试步骤 | 预期结果 |
|------|--------|----------|----------|----------|
| UT-S-001 | 对齐 buffer 写入读取 | 创建 O_DIRECT 文件 | pwrite 对齐数据后 pread | 读出数据与写入一致 |
| UT-S-002 | 非对齐 buffer 写入读取 | buffer 地址未对齐 | Write + Read | 自动对齐后写入，读出一致 |
| UT-S-003 | 多 offset 读写 | - | 多个不同 offset 写入后读取 | 所有数据正确 |
| UT-S-004 | 高 offset 写入 | offset 较大 | Write + Read | 数据正确 |
| UT-S-005 | 无效路径初始化失败 | 路径不存在且不可创建 | InitDataFile | 返回 IOError |

#### 4.1.2 AlignedAllocator (`tests/unit/store/test_aligned_allocator.cpp`)

| 编号 | 测试项 | 前置条件 | 测试步骤 | 预期结果 |
|------|--------|----------|----------|----------|
| UT-S-006 | 512 字节对齐 | - | `Allocate(512, 4096)` | 返回地址按 512 对齐 |
| UT-S-007 | 4096 字节对齐 | - | `Allocate(4096, 4096)` | 返回地址按 4096 对齐 |
| UT-S-008 | 多次分配均对齐 | - | 分配 10 次 | 所有地址对齐 |
| UT-S-009 | Free nullptr 无异常 | - | `Free(nullptr)` | 无 crash |
| UT-S-010 | 分配释放重复 | - | 循环 Allocate+Free | 无泄漏 |
| UT-S-011 | IsAligned true | 传入对齐地址 | 检查 | 返回 true |
| UT-S-012 | IsAligned false | 传入非对齐地址 | 检查 | 返回 false |
| UT-S-013 | IsAligned 2 的幂 | - | 检查各种 2 的幂 | 返回 true |
| UT-S-014 | AlignUp 已对齐 | - | AlignUp | 值不变 |
| UT-S-015 | AlignUp 需对齐 | - | AlignUp | 向上对齐 |
| UT-S-016 | AlignUp 多种对齐值 | - | 测试 512, 4096 等 | 结果正确 |
| UT-S-017 | AlignUp 保持值 | 已对齐的值 | AlignUp | 值不变 |

#### 4.1.3 AlignedBufferPool (`tests/unit/store/test_aligned_buffer_pool.cpp`)

| 编号 | 测试项 | 前置条件 | 测试步骤 | 预期结果 |
|------|--------|----------|----------|----------|
| UT-S-018 | 从池中获取 | 池中有空闲 slot | `Get` | 返回对齐 buffer |
| UT-S-019 | 池耗尽动态分配 | 池为空 | `Get` | 动态分配新 buffer |
| UT-S-020 | 归还后复用 | Get → Put → Get | 再次 Get | 复用已归还的 buffer |
| UT-S-021 | 对齐验证 | - | Get 返回的 buffer | 地址按配置对齐 |
| UT-S-022 | Put nullptr 无异常 | - | `Put(nullptr)` | 无 crash |

#### 4.1.4 文件命名 (`tests/unit/store/test_file_naming.cpp`)

| 编号 | 测试项 | 前置条件 | 测试步骤 | 预期结果 |
|------|--------|----------|----------|----------|
| UT-S-023 | store_id 拼接文件名 | ssd_path, store_id=3 | InitDataFile | 文件名为 `ssd_path/kv_data_3` |
| UT-S-024 | store_id=0 | store_id=0 | InitDataFile | 文件名为 `ssd_path/kv_data_0` |
| UT-S-025 | 不同 store_id 不同文件 | store_id 0 和 7 | InitDataFile | 分别创建不同文件 |
| UT-S-026 | store_id 访问器 | - | `store_id()` | 返回配置的 store_id |
| UT-S-027 | 路径格式 | - | 检查 data_file_ | 包含 ssd_path 和 store_id |
| UT-S-028 | fallocate 预分配 | capacity=1GB | InitDataFile | 文件大小为 1GB |

#### 4.1.5 StoreMetaIndex (`tests/unit/store/test_store_meta_index.cpp`)

| 编号 | 测试项 | 前置条件 | 测试步骤 | 预期结果 |
|------|--------|----------|----------|----------|
| UT-S-029 | Put + Get | 插入 key | Get | 返回正确记录 |
| UT-S-030 | Get 缺失 key | 未插入 | Get | 返回 nullopt |
| UT-S-031 | Get 未提交 key | stat=0 | Get | 返回 nullopt |
| UT-S-032 | BatchContains 混合 | 部分存在 | BatchContains | hit/miss 正确 |
| UT-S-033 | Commit 状态转换 | stat=0 | Commit → Get | stat 变为 1 |
| UT-S-034 | Remove 已有 key | key 存在 | Remove | 返回被删除的记录 |
| UT-S-035 | Remove 不存在 key | key 不存在 | Remove | 返回 nullopt |
| UT-S-036 | Touch 更新访问时间 | key 存在 | Touch | access_time 更新 |
| UT-S-037 | CommittedCount | 插入多个 key，部分 committed | CommittedCount | 仅统计 stat=1 的 |
| UT-S-038 | Put 覆盖 | 已有 key | Put 同 key | 记录被覆盖 |
| UT-S-039 | GetColdEntries 基本 | 有冷数据 | GetColdEntries | 返回冷数据列表 |
| UT-S-040 | GetColdEntries 数量限制 | - | GetColdEntries(max_count) | 不超过 max_count |
| UT-S-041 | GetColdEntries 空索引 | 无数据 | GetColdEntries | 返回空列表 |

#### 4.1.6 NodeLocalAccessor (`tests/unit/store/test_node_local_accessor.cpp`)

| 编号 | 测试项 | 前置条件 | 测试步骤 | 预期结果 |
|------|--------|----------|----------|----------|
| UT-S-042 | 对齐写入读取 | 注册 store 文件 | Write + Read | 数据一致 |
| UT-S-043 | 非对齐写入读取 | 非对齐 buffer | Write + Read | 自动对齐，数据一致 |
| UT-S-044 | 多 offset 读写 | - | 多个 offset | 数据正确 |
| UT-S-045 | 未知 store_id 读取 | 未注册 store_id | Read | 返回 IOError |

#### 4.1.7 PendingEvictQueue (`tests/unit/store/test_pending_evict_queue.cpp`)

| 编号 | 测试项 | 前置条件 | 测试步骤 | 预期结果 |
|------|--------|----------|----------|----------|
| UT-S-046 | 入队与宽限期 | 入队后立即检查 | 未超宽限期 | 空间未释放 |
| UT-S-047 | Stop 立即刷新 | 队列有待回收项 | Stop | 立即释放空间 |
| UT-S-048 | 宽限期内不释放 | 入队 | 未超宽限期检查 | 空间未释放 |
| UT-S-049 | 空队列 Stop | 队列为空 | Stop | 无异常 |

#### 4.1.8 EvictManager (`tests/unit/store/test_evict_manager.cpp`)

| 编号 | 测试项 | 前置条件 | 测试步骤 | 预期结果 |
|------|--------|----------|----------|----------|
| UT-S-050 | 冷数据选择与驱逐 | 使用率 > 高水位 | DoEvict | 驱逐最冷数据 |
| UT-S-051 | SyncRemove 失败阻止驱逐 | Mock SyncRemove 失败 | DoEvict | 不驱逐该 key |
| UT-S-052 | 批量大小限制 | - | DoEvict | 单次驱逐数量有限制 |
| UT-S-053 | 超过高水位线触发 | 使用率 90% | DoEvict | 执行驱逐 |
| UT-S-054 | 低于高水位线不驱逐 | 使用率 80% | DoEvict | 不执行驱逐 |

#### 4.1.9 StoreServiceImpl (`tests/unit/store/test_store_service_impl.cpp`)

| 编号 | 测试项 | 前置条件 | 测试步骤 | 预期结果 |
|------|--------|----------|----------|----------|
| UT-S-055 | Ping | Store RPC Server 运行 | Ping RPC | 返回成功 |
| UT-S-056 | Put 后按 key 读取 | - | Put → Read(key) | 数据一致 |
| UT-S-057 | 按 offset 读取 | 已写入数据 | Read(offset) | 数据正确 |
| UT-S-058 | BatchGet 按 key | 已写入多条数据 | BatchGet | 全部数据正确 |

#### 4.1.10 StoreRpcClient (`tests/unit/store/test_store_rpc_client.cpp`)

| 编号 | 测试项 | 前置条件 | 测试步骤 | 预期结果 |
|------|--------|----------|----------|----------|
| UT-S-059 | Connect + Ping | Store RPC Server 运行 | Connect → Ping | 连接成功 |
| UT-S-060 | 按 offset 读取 | 已有数据 | Read(offset) | 数据正确 |
| UT-S-061 | 无效地址连接 | 地址不存在 | Connect | 不 crash |

### 4.2 模块测试 (`tests/module/test_store_engine.cpp`)

| 编号 | 测试项 | 前置条件 | 测试步骤 | 预期结果 |
|------|--------|----------|----------|----------|
| MT-S-001 | BatchWrite 顺序优化 | 多个 WriteItem 的 offset 乱序 | BatchWrite | 实际写入按 offset 升序执行 |
| MT-S-002 | Key-aware Put + Get | - | Put → Get | 数据一致 |
| MT-S-003 | BatchPut + BatchGet | 多个 key | BatchPut → BatchGet | 全部数据正确 |
| MT-S-004 | 写入后立即读取一致性 | 写入数据到 offset | 读取同一 offset | 数据一致 |
| MT-S-005 | 并发写入 | io_threads=4，多个并发任务 | 并发 Write | 全部成功，无数据覆盖 |

---

## 5. Transfer 模块测试

### 5.1 单元测试

#### 5.1.1 TransferChannel 抽象接口 (`tests/unit/transfer/test_transfer_channel.cpp`)

| 编号 | 测试项 | 前置条件 | 测试步骤 | 预期结果 |
|------|--------|----------|----------|----------|
| UT-T-001 | 抽象类不可实例化 | - | 尝试实例化 | 编译错误或运行时正确行为 |
| UT-T-002 | IoSegment 字段 | - | 构造 IoSegment | 字段可正确赋值 |
| UT-T-003 | IoSegment 默认值 | - | 默认构造 | 各字段为零值 |
| UT-T-004 | RpcCallback 抽象 | - | 检查 | 接口正确 |

#### 5.1.2 TransferManager (`tests/unit/transfer/test_transfer_manager.cpp`)

| 编号 | 测试项 | 前置条件 | 测试步骤 | 预期结果 |
|------|--------|----------|----------|----------|
| UT-T-005 | 带配置构造 | - | 构造 TransferManager | 无异常 |
| UT-T-006 | 注册 Store 地址 | - | RegisterStoreAddr | 无异常 |
| UT-T-007 | 未注册地址返回 null | - | GetStoreChannel | 返回 nullptr |
| UT-T-008 | 注册但未连接返回 null | 仅注册地址 | GetStoreChannel | 返回 nullptr |
| UT-T-009 | GetStoreChannels 空输入 | - | GetStoreChannels([]) | 返回空列表 |
| UT-T-010 | GetStoreChannels 多 ID | 已注册 | GetStoreChannels | 返回对应通道 |
| UT-T-011 | GetMetaChannel 无地址 | - | GetMetaChannel | 返回 nullptr |
| UT-T-012 | GetMetaChannel 有地址无连接 | - | GetMetaChannel | 返回 nullptr |
| UT-T-013 | CloseAll 空状态 | - | CloseAll | 无异常 |
| UT-T-014 | CloseAll 注册后 | 已注册 | CloseAll | 通道关闭 |

### 5.2 模块测试 (`tests/module/test_transfer_brpc.cpp`)

| 编号 | 测试项 | 前置条件 | 测试步骤 | 预期结果 |
|------|--------|----------|----------|----------|
| MT-T-001 | 重试策略失败 | 启动 brpc Server | 连接不存在的地址 | 重试后返回错误 |
| MT-T-002 | 重试策略首次成功 | Server 正常 | Connect | 首次即成功 |
| MT-T-003 | TransferManager 未知 Store | - | GetStoreChannel | 返回 null |
| MT-T-004 | TransferManager 注册并连接 | Server 运行 | Register + Connect | 连接成功 |
| MT-T-005 | TransferManager CloseAll | 已连接 | CloseAll | 连接关闭 |

---

## 6. IO Scheduler 模块测试

### 6.1 单元测试

#### 6.1.1 PassthroughPolicy (`tests/unit/scheduler/test_passthrough_policy.cpp`)

| 编号 | 测试项 | 前置条件 | 测试步骤 | 预期结果 |
|------|--------|----------|----------|----------|
| UT-SC-001 | 放通决策 | - | `Decide(IORequest)` | status=0, permitted_ts=request_ts, ticket 非零 |
| UT-SC-002 | ticket 单调递增 | - | 连续调用 Decide 10 次 | ticket 严格递增 |
| UT-SC-003 | OnIOComplete 是空操作 | - | OnIOComplete | 无异常 |
| UT-SC-004 | Name 返回 passthrough | - | Name() | 返回 "passthrough" |
| UT-SC-005 | 多态访问 | 基类指针 | Decide | 正确调用派生类实现 |
| UT-SC-006 | Decide 返回非零 permitted_ts | - | Decide | permitted_ts > 0 |

#### 6.1.2 NodeStats (`tests/unit/scheduler/test_node_stats.cpp`)

| 编号 | 测试项 | 前置条件 | 测试步骤 | 预期结果 |
|------|--------|----------|----------|----------|
| UT-SC-007 | 单次 IO 记录 | - | `RecordIO` | io_count == 1 |
| UT-SC-008 | 同通道多次 IO | - | RecordIO 多次 | io_count 正确 |
| UT-SC-009 | 带宽计算 | 1 秒内记录 100MB IO | BandwidthMBps | 约 100 MB/s |
| UT-SC-010 | 零窗口带宽 | - | window_ns=0 | 返回 0 |
| UT-SC-011 | 大传输带宽 | - | BandwidthMBps | 正确计算 |
| UT-SC-012 | 延迟百分位 | 插入 100 个样本到 channel 0 | local_ssd_read.p50/p99 | 近似正确 |
| UT-SC-013 | 按节点地址统计 | 不同 node_addr | RecordIO | 分别统计 |
| UT-SC-014 | SSD 带宽利用率 | - | 检查 | 计算 correct |
| UT-SC-015 | 按通道延迟隔离 | 4 个通道分别注入不同延迟 | ChannelStats | 各通道延迟独立正确 |

### 6.2 模块测试 (`tests/module/test_scheduler_service.cpp`)

| 编号 | 测试项 | 前置条件 | 测试步骤 | 预期结果 |
|------|--------|----------|----------|----------|
| MT-SC-001 | 启停 | 配置 UDS 路径 | Start → Stop | .sock 文件创建后清理 |
| MT-SC-002 | RequestIO + ReportIOCompletion | Scheduler 运行 | 发送 IORequest → IOResponse → IOCompletion | stats 记录完整 IO 信息 |
| MT-SC-003 | StoreReportIO | Scheduler 运行 | Store 发送 StoreIOReport | NET_RX 通道统计正确 |
| MT-SC-004 | 统计报告输出 | 完成 IO | PrintReport | 输出包含带宽、延迟、负载信息 |

---

## 7. Bypass 机制专项测试

### 7.1 单元测试 (`tests/unit/scheduler/test_bypass.cpp`)

| 编号 | 测试项 | 前置条件 | 测试步骤 | 预期结果 |
|------|--------|----------|----------|----------|
| UT-BP-001 | 初始状态 Disconnected | - | 检查状态 | state != CONNECTED |
| UT-BP-002 | 初始不处于 Connected | - | IsConnected | false |
| UT-BP-003 | RequestIO 返回 bypass | - | RequestIO | 返回 bypass response |
| UT-BP-004 | 多次 bypass 请求 | - | 连续 RequestIO | 每次返回 bypass |
| UT-BP-005 | 连续失败触发 bypass | Mock RPC 连续失败 | RequestIO ×3 | state=BYPASS |
| UT-BP-006 | 重连探测不 crash | - | 触发 reconnect | 无异常 |
| UT-BP-007 | bypass 响应延迟 | - | 测量 | < 100us |
| UT-BP-008 | IsBypassMode | bypass 状态 | 检查 | true |
| UT-BP-009 | ReportIOCompletion bypass 跳过 | bypass 状态 | ReportIOCompletion | 不发送 RPC |
| UT-BP-010 | StoreReportIO bypass 跳过 | bypass 状态 | StoreReportIOAsync | 无异常 |

### 7.2 模块测试 (`tests/module/test_bypass_e2e.cpp`)

| 编号 | 测试项 | 前置条件 | 测试步骤 | 预期结果 |
|------|--------|----------|----------|----------|
| MT-BP-001 | Scheduler stop 后 Client bypass | Scheduler 运行中 | stop Scheduler | Client bypass |
| MT-BP-002 | Scheduler 不可用 bypass | Scheduler 未启动 | Client RequestIO | bypass |
| MT-BP-003 | Scheduler 恢复后重连 | bypass 状态 | 重启 Scheduler | Client 恢复正常 |

---

## 8. 集成测试

集成测试通过 pytest 运行，启动真实的服务进程（Meta、Scheduler），使用 Python Client 或 LMCache RemoteBackend 端到端验证。

### 8.1 服务启动测试 (`tests/integration/test_e2e_batch_ops.py`)

**TestE2EServiceStartup** — 验证各服务进程可正常启停。

| 编号 | 测试项 | 测试步骤 | 预期结果 |
|------|--------|----------|----------|
| IT-SVC-001 | Meta 启动并接受连接 | 启动 falconkv_master，连接 TCP 端口 | 连接成功 |
| IT-SVC-002 | Meta 干净退出 | SIGTERM → wait | 退出码 0 |
| IT-SVC-003 | Scheduler 启动创建 UDS | 启动 falconkv_sched | .sock 文件存在 |
| IT-SVC-004 | Scheduler 干净退出 | SIGTERM → wait | .sock 文件清理 |
| IT-SVC-005 | Meta + Scheduler 同时运行 | 同时启动 | 两者均正常 |
| IT-SVC-006 | Meta 重启 | 停止后重新启动 | 新进程绑定同一端口 |

### 8.2 LMCache Connector 端到端测试 (`tests/integration/test_falconkv_connector_e2e.py`)

**前置条件**：启动 Meta Server（falconkv_master），安装 LMCache + pyfalconkv。

#### TestLocalReadWrite — ACCESS_LOCAL_DIRECT（单 Connector 本地读写）

| 编号 | 测试项 | 测试步骤 | 预期结果 |
|------|--------|----------|----------|
| IT-LC-001 | Put + Get 往返 | Writer put 1 个 key → get | 数据一致 |
| IT-LC-002 | 批量 Put + Get | Writer batch put 3 个 key → batched get | 全部数据一致 |
| IT-LC-003 | Contains 存在和不存在 | put 后 contains 已有 key + 未 put key | 已有=true，未 put=false |
| IT-LC-004 | 连续 Contains | put 2 key → contains 2 key → put 1 key → contains 3 key | 命中数递增 |
| IT-LC-005 | batched_get_non_blocking 部分命中 | put 前 2 个 key → batched_get_non_blocking 3 个 key | 仅返回前 2 个，第 3 个 None |
| IT-LC-006 | Fire-and-Forget 数据完整性 | fire_and_forget put 3 key → 等待 → get | 全部数据一致 |

#### TestSameNodeRead — ACCESS_NODE_DIRECT（同节点跨 Store 直读）

| 编号 | 测试项 | 测试步骤 | 预期结果 |
|------|--------|----------|----------|
| IT-LC-007 | 跨 Store 单 key 直读 | Writer(store1,node1) put → Reader(store2,node1) get | 通过 NodeLocalAccessor DirectIO 成功读取，数据一致 |
| IT-LC-008 | 跨 Store 批量直读 | Writer batch put 5 key → Reader batched get | 全部 5 个 key 数据一致 |

#### TestCrossNodeRead — ACCESS_REMOTE_RPC（跨节点 RPC 读取）

| 编号 | 测试项 | 测试步骤 | 预期结果 |
|------|--------|----------|----------|
| IT-LC-009 | 跨节点 RPC 单 key 读取 | Writer(node1) put → Reader(node2) get | 通过 StoreRpcClient RPC 成功读取，数据一致 |
| IT-LC-010 | 跨节点 RPC 批量读取 | Writer batch put 3 key → Reader batched get | 全部 3 个 key 数据一致 |

### 8.3 功能端到端测试 (`tests/integration/test_functional_e2e.py`)

**TestBatchPutGet** — 基本读写正确性。

| 编号 | 测试项 | 测试步骤 | 预期结果 |
|------|--------|----------|----------|
| IT-FN-001 | 单 key 往返 | put → get | 数据一致 |
| IT-FN-002 | 多 key 往返 | put 10 key → get | 全部一致 |
| IT-FN-003 | 重复 put 跳过 | put key → put 同 key 新值 → get | 第二次 put 被跳过，get 返回首次写入的值 |
| IT-FN-004 | 获取不存在 key | get 未 put key | 返回空 |
| IT-FN-005 | 大值 | put 256KB 数据 → get | 数据一致 |

**TestBatchExist** — 存在性检查。

| 编号 | 测试项 | 测试步骤 | 预期结果 |
|------|--------|----------|----------|
| IT-FN-006 | put 后 exist | put → exist | true |
| IT-FN-007 | exist 未 put | exist 未写入 key | false |
| IT-FN-008 | exist 混合 | put 部分 key → exist 全部 | 部分命中 |

**TestSequentialWrites** — 顺序写入。

| 编号 | 测试项 | 测试步骤 | 预期结果 |
|------|--------|----------|----------|
| IT-FN-009 | 连续写入 100 key | put 100 key → get 100 key | 全部一致 |
| IT-FN-010 | 重复 put 跳过 | put key → put 同 key 新值 ×4 → get | 后续 put 均被跳过，get 始终返回首次写入的值 |

**TestClientLifecycle** — 客户端生命周期。

| 编号 | 测试项 | 测试步骤 | 预期结果 |
|------|--------|----------|----------|
| IT-FN-011 | 创建/关闭/重建 | close → 重新创建 Client | 新实例正常工作 |
| IT-FN-012 | 空 keys batch get | batch_get([]) | 返回空列表 |

**TestDifferentValueSizes** — 不同值大小。

| 编号 | 测试项 | 测试步骤 | 预期结果 |
|------|--------|----------|----------|
| IT-FN-013 | 各种大小数据 | 参数化测试多种 size | 全部读写一致 |

### 8.4 LMCache Adapter 测试 (`tests/integration/test_lmcache_adapter.py`)

**TestLMCacheAdapter** — 验证 FalconKV Connector 自动发现和接口适配（无需服务进程）。

| 编号 | 测试项 | 测试步骤 | 预期结果 |
|------|--------|----------|----------|
| IT-AD-001 | Adapter 自动发现 | ConnectorManager 扫描 | 发现 FalconKVConnectorAdapter |
| IT-AD-002 | schema 属性 | 检查 adapter.schema | "falconkv" |
| IT-AD-003 | batched 接口支持 | 检查 support_batched_* | 全部返回 True |
| IT-AD-004 | async 方法存在 | 检查方法 | exists/batched_get 等均为 async |

### 8.5 Scheduler 集成测试 (`tests/integration/test_scheduler_integration.py`)

| 编号 | 测试项 | 测试步骤 | 预期结果 |
|------|--------|----------|----------|
| IT-SCH-001 | UDS 文件类型 | 启动 Scheduler → 检查 .sock | 文件存在且为 socket |
| IT-SCH-002 | Meta 多连接 | 多个 Client 连接 Meta | 全部成功 |
| IT-SCH-003 | Scheduler 停止后 bypass | 停止 Scheduler → Client IO | 自动 bypass |
| IT-SCH-004 | Meta 端口释放 | 停止 Meta → 重启 | 新进程绑定同一端口 |

### 8.6 Store 驱逐 + Scheduler 统计集成测试 (`tests/integration/test_evict_and_scheduler_e2e.py`)

**前置条件**：启动 Meta Server（falconkv_master）和 Scheduler Server（falconkv_sched），安装 pyfalconkv。

#### TestStoreEviction — Store 容量耗尽后驱逐回收

| 编号 | 测试项 | 测试步骤 | 预期结果 |
|------|--------|----------|----------|
| IT-EV-001 | 驱逐释放空间 | 写入 ~830 个 1MB key 超过 80% 高水位 → 等待驱逐 → 写入新 key | 新 key 写入成功，部分旧 key 被驱逐 |

#### TestSchedulerStats — Scheduler 统计收集（通过 Client API 触发）

Client 配置 `scheduler_enabled=true`，Client 内部的 SchedulerProxy 通过 brpc UDS 自动向 Scheduler 发送 RequestIO + ReportIOCompletion RPC。

| 编号 | 测试项 | 测试步骤 | 预期结果 |
|------|--------|----------|----------|
| IT-STAT-001 | 本地 SSD 读写统计 | Client batch_put 20 key → batch_get → 检查 Scheduler 日志 | 日志含 `local_ssd_write` 和 `local_ssd_read` 统计，ios > 0 |
| IT-STAT-002 | 网络发送读统计 | 两个 Client（不同 node_id）分别写和读 → 检查 Scheduler 日志 | 日志含 `net_tx_read` 统计，ios > 0 |

---

## 9. 性能基准测试

以下测试需在真实硬件环境（NVMe SSD + 100Gbps 网络）执行。

| 编号 | 测试场景 | 指标 | 目标值 | 测试方法 |
|------|----------|------|--------|----------|
| PERF-001 | BatchExist (100 keys) | 延迟 | < 2ms | 1000 次取 P50/P99 |
| PERF-002 | BatchPut (100 keys, L0) | 延迟 | < 10ms | 1000 次取 P50/P99 |
| PERF-003 | BatchGet (100 keys, L0) | 延迟 | < 5ms | 1000 次取 P50/P99 |
| PERF-004 | 单次 Put (L0 DirectIO) | 延迟 | < 1ms | 10000 次取 P50 |
| PERF-005 | 单次 Get (L0 DirectIO) | 延迟 | < 0.5ms | 10000 次取 P50 |
| PERF-006 | 吞吐量 | keys/s | > 5000 | 8 Client 并发持续写入 |
| PERF-007 | Scheduler RPC 延迟 | 延迟 | < 50us | UDS Ping 测试 |
| PERF-008 | Bypass 感知时间 | 延迟 | < 100us | kill Scheduler 后测量 |

---

## 10. 多节点端到端性能测试

### 10.1 概述

多 Client（同节点 / 跨节点）并发批量读写性能测试。验证 FalconKV 在多 Client 并发场景下的吞吐量和延迟分布，覆盖三种读取路径：

- **ACCESS_LOCAL_DIRECT**：Client 读写自身绑定 Store（本地 DirectIO）
- **ACCESS_NODE_DIRECT**：同节点不同 Store 之间的 DirectIO 直读
- **ACCESS_REMOTE_RPC**：跨节点 Store RPC 远程读取

测试拆分为三个**独立用例**，每个用例验证一种读取路径，可单独运行、互不干扰：

| 用例 | 脚本 | 配置文件 | 参与客户端 | 验证路径 |
|------|------|----------|-----------|----------|
| 本地读写 | `run_perf_local.sh` | `perf_config_local.json` | A (writer) | ACCESS_LOCAL_DIRECT |
| 本节点读 | `run_perf_node_read.sh` | `perf_config_node_read.json` | A (writer) + B (same_node_reader) | ACCESS_NODE_DIRECT |
| 跨节点读 | `run_perf_cross_node.sh` | `perf_config_cross_node.json` | A (writer) + C (cross_node_reader) | ACCESS_REMOTE_RPC |

三个用例共享 `perf_common.sh`（公共函数库）和 `perf_client.py` / `perf_aggregate.py`（工作进程和结果汇总）。

### 10.2 公共配置结构

每个配置文件包含五段：

```json
{
  "test": {
    "duration_sec": 20,
    "batch_size": 64,
    "value_size": 1048576,
    "warmup_sec": 5,
    "meta_listen_port": 18900,
    "scheduler_uds_path": "...",
    "result_dir": "..."
  },
  "meta": { "shard_count": 16 },
  "scheduler": { "schedule_policy": "passthrough", "enabled": true },
  "clients": [ ... ],
  "transfer": { "meta_addr": "127.0.0.1:18900" }
}
```

各段说明：

| 段 | 用途 |
|----|------|
| `test` | 测试参数：持续时间、批量大小、值大小、预热时长 |
| `meta` | Meta 服务配置 |
| `scheduler` | Scheduler 配置 |
| `clients` | 每个 Client 定义：client_id, node_id, store_id, listen_port, ssd_path, capacity_gb 等 |
| `transfer` | 传输层配置 |

### 10.3 测试用例详细设计

#### 10.3.1 用例一：本地读写（PERF-LOCAL）

**目标**：验证 Client A 写入自身 Store 后，通过 `ACCESS_LOCAL_DIRECT` 进程内读取的延迟和吞吐量。

**配置文件**：`perf_config_local.json`

**客户端定义**：

| Client | node_id | store_id | 角色 |
|--------|---------|----------|------|
| A | 1 | 1 | writer（写入 + 本地读回） |

**数据路径**：`Client A → batch_put → Store 1 (SSD DirectIO) → batch_get → 进程内读取`

**测试流程**：

1. **Warmup 阶段**（`warmup_sec` 秒）：Client A 持续写入 `warmup_A_{batch}_{i}` 前缀的 key，预热 Store 和 Meta。
2. **Benchmark 阶段**（`duration_sec` 秒），每轮循环：
   - 生成 `bench_A_{batch}_{i}` 前缀的 write keys
   - `batch_exist` 检查 key 存在性，记录延迟
   - `batch_put` 写入新 key，记录延迟
   - `batch_get` 读取上一轮写入的 key（`bench_A_{batch-1}_{i}`），记录延迟
3. 输出 `result_A.json`

**验证要点**：

- put 延迟反映本地 SSD DirectIO 写入性能
- get 延迟反映进程内直接读取性能（无 RPC、无跨进程通信）
- exist 延迟反映 KeyDescCache + Meta RPC 查询性能

#### 10.3.2 用例二：本节点读（PERF-NODE-READ）

**目标**：验证 Client B 通过 `ACCESS_NODE_DIRECT`（NodeLocalAccessor DirectIO）读取同节点 Client A 写入的数据的延迟和吞吐量。

**配置文件**：`perf_config_node_read.json`

**客户端定义**：

| Client | node_id | store_id | 角色 |
|--------|---------|----------|------|
| A | 1 | 1 | writer（写入数据供 B 读取） |
| B | 1 | 2 | same_node_reader（同节点 DirectIO 直读） |

**数据路径**：
- 写入：`Client A → batch_put → Store 1 (SSD)`
- 读取：`Client B → batch_exist → Meta 查询 → KeyDescriptor(access_type=ACCESS_NODE_DIRECT) → NodeLocalAccessor → DirectIO → Store 1 文件`

**测试流程**：

1. **Warmup 阶段**：Client A 写入 `warmup_A_{batch}_{i}` 数据，Client B 等待 A 完成后通过 marker 文件获取 warmup batch 数量。
2. **等待 Meta 同步**：2 秒等待 Meta 数据传播。
3. **Benchmark 阶段**（`duration_sec` 秒），每轮循环：
   - Client A：同用例一（写入新 key + 读回自身旧 key）
   - Client B：
     - 生成 `warmup_A_{batch % warmup_batch_count}_{i}` 前缀的 read keys（循环读取 A 的 warmup 数据）
     - `batch_exist` 检查 key 存在性
     - `batch_get` 通过 NodeLocalAccessor DirectIO 读取 A 的 Store 文件
4. 输出 `result_A.json`、`result_B.json`

**验证要点**：

- Client B 的 get 延迟反映同节点 DirectIO 直读性能（绕过 RPC）
- 对比用例一的 get 延迟，NodeDirect 应高于 LocalDirect（多了 fd cache lookup 和 DirectIO pread）
- Client A 和 Client B 并行运行，验证并发 IO 场景下性能稳定

#### 10.3.3 用例三：跨节点读（PERF-CROSS-NODE）

**目标**：验证 Client C 通过 `ACCESS_REMOTE_RPC`（StoreRpcClient）读取不同节点 Client A 写入的数据的延迟和吞吐量。

**配置文件**：`perf_config_cross_node.json`

**客户端定义**：

| Client | node_id | store_id | 角色 |
|--------|---------|----------|------|
| A | 1 | 1 | writer（写入数据供 C 读取） |
| C | 2 | 3 | cross_node_reader（跨节点 RPC 读取） |

**数据路径**：
- 写入：`Client A → batch_put → Store 1 (node 1, SSD)`
- 读取：`Client C → batch_exist → Meta 查询 → KeyDescriptor(access_type=ACCESS_REMOTE_RPC) → StoreRpcClient → brpc → Store 1 RPC Service → SSD`

**测试流程**：

1. **Warmup 阶段**：同用例二，Client A 写入数据，Client C 等待。
2. **等待 Meta 同步**：2 秒等待。
3. **Benchmark 阶段**（`duration_sec` 秒），每轮循环：
   - Client A：同用例一
   - Client C：
     - 循环读取 `warmup_A_*` 数据
     - `batch_exist` + `batch_get` 通过 RPC 跨节点读取
4. 输出 `result_A.json`、`result_C.json`

**验证要点**：

- Client C 的 get 延迟反映跨节点 RPC 读取性能（包含 brpc 序列化 + 网络传输 + 远端 Store DirectIO）
- 对比用例一和用例二的 get 延迟，CrossNode 应最高（增加网络往返开销）
- 验证跨节点场景下 Meta 同步和 KeyDescriptor 路由的正确性

### 10.4 客户端角色自动判定

`perf_client.py` 根据配置自动判定每个客户端的角色：

```python
def _get_role(client_id, client_cfg, config):
    if client_id == "A":
        return "writer"
    writer_cfg = _find_client_config(config, "A")
    if client_cfg["node_id"] == writer_cfg["node_id"]:
        return "same_node_reader"
    return "cross_node_reader"
```

- Client A 始终为 writer：warmup 写入 + benchmark 写入新 key + 读回自身旧 key
- 其他 Client 根据与 A 的 `node_id` 关系判定为 `same_node_reader` 或 `cross_node_reader`，循环读取 A 的 warmup 数据

**本地读写用例**（仅 Client A）：`perf_client.py` 以 writer 模式运行，只执行本地 put + get。

**本节点读用例**（Client A + B）：B 的 `node_id` 等于 A 的 `node_id`，自动判定为 `same_node_reader`。

**跨节点读用例**（Client A + C）：C 的 `node_id` 不等于 A 的 `node_id`，自动判定为 `cross_node_reader`。

### 10.5 采集指标

每操作类型（exist / put / get）采集：

| 指标 | 说明 |
|------|------|
| `total_ops` | 总操作次数 |
| `avg_ms` | 平均延迟 |
| `p50_ms` / `p95_ms` / `p99_ms` / `max_ms` / `min_ms` | 百分位延迟 |
| `throughput_ops` | 吞吐量 (ops/s) |
| `throughput_mb` | 吞吐量 (MB/s) |
| `errors` | 错误次数 |

### 10.6 结果汇总

`perf_aggregate.py` 读取各 Client 的 `result_{client_id}.json`，打印汇总表格：

```
=== FalconKV Local Read/Write Test ===
===================================================
  Client | Role               | Op     | Total | Avg(ms) | P50(ms) | P99(ms) | Ops/s   | MB/s
  -------------------------------------------------------------------------------------------------------------------
  A(n1,s1)| writer             | exist  |   500 |   0.35  |   0.30  |   0.85  |   40.0  |  0.0
  A(n1,s1)| writer             | put    |   480 |   1.20  |   1.10  |   3.50  |   40.0  |  0.2
  A(n1,s1)| writer             | get    |   460 |   0.50  |   0.45  |   1.20  |   40.0  |  0.2
```

同时输出 `summary.json` 供程序化分析。

### 10.7 脚本架构

公共函数库 `perf_common.sh` 提供：

| 函数 | 说明 |
|------|------|
| `parse_config` | 解析 JSON 配置为 shell 变量 |
| `wait_for_port` | 轮询等待 TCP 端口就绪 |
| `wait_for_file` | 轮询等待文件出现 |
| `stop_process` | SIGTERM → 超时 SIGKILL 停止进程 |
| `run_perf` | 主入口：启动 Meta/Scheduler → 并行启动 Client → 等待完成 → 停止服务 → 汇总结果 |

三个 wrapper 脚本各只需 source `perf_common.sh` 并调用 `run_perf`：

```bash
# run_perf_local.sh
source perf_common.sh
run_perf "$CONFIG_FILE" "FalconKV Local Read/Write Test"
```

### 10.8 一键启动

```bash
cd tests/perf

# 用例一：本地读写（仅 Client A）
./run_perf_local.sh

# 用例二：本节点读（Client A + B）
./run_perf_node_read.sh

# 用例三：跨节点读（Client A + C）
./run_perf_cross_node.sh

# 仍可使用原综合脚本（Client A + B + C 全部运行）
./run_perf.sh perf_config.json
```

每个脚本自动完成：解析配置 → 创建目录 → 启动 Meta → 启动 Scheduler（可选）→ 并行启动 Client → 等待完成 → 停止服务 → 结果汇总。`trap cleanup EXIT` 确保异常退出时进程被清理。

---

## 11. 异常与边界测试

| 编号 | 测试项 | 测试步骤 | 预期结果 |
|------|--------|----------|----------|
| EDGE-001 | Meta 不可达时 Client 行为 | 停止 Meta 进程 | Client 重试后返回错误，不 crash |
| EDGE-002 | Store 不可达时写入 | 停止目标 Store | 返回错误 |
| EDGE-003 | 空 keys 列表 | BatchExist([]) | 返回 hit_count=0，无 RPC |
| EDGE-004 | 超大 batch（1000 keys） | BatchPut 1000 个 key | 全部成功，无超时 |
| EDGE-005 | 重复 key 写入 | BatchPut 同一个 key 两次 | 第二次被跳过（key 已存在），不分配新空间，打印 INFO 日志 |
| EDGE-006 | SSD 文件不存在时 Store 启动 | 删除数据文件后启动 Store | 自动创建文件并 fallocate |
| EDGE-007 | SSD 空间不足 | 写入超出 capacity | 返回 ENOSPC，已有数据不受影响 |
| EDGE-008 | Meta 重启恢复 | 重启 Meta | 重新连接后恢复正常 |
| EDGE-009 | 同节点 Store 文件互相直读 | Client 读另一 Store 的文件 | 通过 NodeLocalAccessor + DirectIO 成功 |
| EDGE-010 | GIL 释放与 Python 多线程 | 4 线程并发 BatchGet | 无死锁，无 crash |

---

## 12. 测试基础设施

### 12.1 目录结构

```
falconkv/
├── tests/
│   ├── unit/                          # C++ 单元测试（GTest）
│   │   ├── client/
│   │   │   ├── test_key_desc_cache.cpp
│   │   │   ├── test_alloc_policy.cpp
│   │   │   └── test_data_structures.cpp
│   │   ├── meta/
│   │   │   ├── test_slot_allocator.cpp
│   │   │   ├── test_meta_sync_commit.cpp
│   │   │   ├── test_meta_service_impl.cpp
│   │   │   └── test_meta_rpc_client.cpp
│   │   ├── store/
│   │   │   ├── test_direct_io.cpp
│   │   │   ├── test_aligned_allocator.cpp
│   │   │   ├── test_aligned_buffer_pool.cpp
│   │   │   ├── test_file_naming.cpp
│   │   │   ├── test_store_meta_index.cpp
│   │   │   ├── test_node_local_accessor.cpp
│   │   │   ├── test_pending_evict_queue.cpp
│   │   │   ├── test_evict_manager.cpp
│   │   │   ├── test_fd_cache.cpp
│   │   │   ├── test_meta_sync_client.cpp
│   │   │   ├── test_store_service_impl.cpp
│   │   │   └── test_store_rpc_client.cpp
│   │   ├── transfer/
│   │   │   ├── test_transfer_channel.cpp
│   │   │   └── test_transfer_manager.cpp
│   │   └── scheduler/
│   │       ├── test_passthrough_policy.cpp
│   │       ├── test_node_stats.cpp
│   │       └── test_bypass.cpp
│   ├── module/                        # C++ 模块测试（GTest + 真实 brpc Server）
│   │   ├── test_client_batch_ops.cpp
│   │   ├── test_meta_rpc_service.cpp
│   │   ├── test_store_engine.cpp
│   │   ├── test_transfer_brpc.cpp
│   │   ├── test_scheduler_service.cpp
│   │   └── test_bypass_e2e.cpp
│   └── integration/                   # Python 集成测试（pytest）
│       ├── conftest.py                # 共享 fixture（服务启停、客户端创建）
│       ├── test_e2e_batch_ops.py      # 服务启停 + C++ Client 端到端
│       ├── test_falconkv_connector_e2e.py  # LMCache RemoteBackend 三级读取
│       ├── test_functional_e2e.py     # 功能正确性（Python Client）
│       ├── test_lmcache_adapter.py    # LMCache Adapter 自动发现
│       ├── test_scheduler_integration.py  # Scheduler 进程级集成
│       └── test_evict_and_scheduler_e2e.py  # Store 驱逐 + Scheduler 统计集成测试
│   └── perf/                            # 端到端性能测试
│       ├── perf_common.sh               # 公共函数库（parse_config, run_perf 等）
│       ├── perf_client.py               # 单 Client 工作进程
│       ├── perf_aggregate.py            # 结果汇总脚本
│       ├── perf_config.json             # 综合测试配置（A+B+C 全部运行）
│       ├── run_perf.sh                  # 综合测试一键启动脚本
│       ├── perf_config_local.json       # 用例一：本地读写配置（仅 Client A）
│       ├── run_perf_local.sh            # 用例一：本地读写启动脚本
│       ├── perf_config_node_read.json   # 用例二：本节点读配置（Client A + B）
│       ├── run_perf_node_read.sh        # 用例二：本节点读启动脚本
│       ├── perf_config_cross_node.json  # 用例三：跨节点读配置（Client A + C）
│       └── run_perf_cross_node.sh       # 用例三：跨节点读启动脚本
```

### 12.2 测试依赖

| 依赖 | 用途 | 版本建议 |
|------|------|----------|
| Google Test (gtest) | C++ 单元/模块测试框架 | 1.14+ |
| brpc | RPC 框架（模块测试进程内 Server） | 1.6+ |
| jsoncpp | JSON 配置解析（编译必需） | 1.9+ |
| glog | 日志库（可选，有 fallback） | 0.6+ |
| pytest | Python 集成测试框架 | 7.0+ |
| LMCache | FalconKVConnector 适配测试 | v0.3.12 |
| PyTorch | LMCache 测试 tensor 分配 | 2.0+ |

### 12.3 Mock 策略

| 被测模块 | Mock 对象 | Mock 方式 |
|----------|-----------|-----------|
| Client 模块测试 | Meta Server | 真实 brpc Server（进程内），真实 MetaManager |
| Store 模块测试 | Meta Server | 真实 MetaSyncClient + 真实 Meta brpc Server |
| Scheduler 模块测试 | UDS brpc Server | 真实 brpc Server on UDS |
| LMCache Adapter | LMCache RemoteConnector | 真实类层次，无外部进程 |

### 12.4 运行命令

```bash
# C++ 单元 + 模块测试（无外部进程依赖，32 个 test binary）
./build.sh test

# Python 集成测试（自动启停 Meta / Scheduler 进程）
pytest tests/integration/ -v

# LMCache Connector 三级读取测试（需 LMCache + PyTorch）
pytest tests/integration/test_falconkv_connector_e2e.py -v

# 功能端到端测试
pytest tests/integration/test_functional_e2e.py -v

# 服务启动测试
pytest tests/integration/test_e2e_batch_ops.py -v

# 端到端性能测试 — 三个独立用例
chmod +x tests/perf/run_perf_*.sh tests/perf/perf_common.sh

# 用例一：本地读写（ACCESS_LOCAL_DIRECT，仅 Client A）
./tests/perf/run_perf_local.sh

# 用例二：本节点读（ACCESS_NODE_DIRECT，Client A + B）
./tests/perf/run_perf_node_read.sh

# 用例三：跨节点读（ACCESS_REMOTE_RPC，Client A + C）
./tests/perf/run_perf_cross_node.sh

# 综合测试（Client A + B + C 全部运行）
./tests/perf/run_perf.sh tests/perf/perf_config.json
```

### 12.5 测试标记

| 标记 | 说明 | CI 是否运行 |
|------|------|------------|
| `@pytest.mark.integration` | 集成测试（需服务进程） | 仅集成测试 CI |

---

## 13. 附录

### 13.1 测试覆盖率目标

| 模块 | 行覆盖率目标 | 分支覆盖率目标 |
|------|------------|--------------|
| Client (C++ Core) | ≥ 80% | ≥ 70% |
| Meta | ≥ 75% | ≥ 65% |
| Store | ≥ 80% | ≥ 70% |
| Transfer | ≥ 70% | ≥ 60% |
| Scheduler | ≥ 80% | ≥ 70% |
| Python Connector | ≥ 80% | ≥ 70% |

### 13.2 测试与设计文档对应关系

| 设计文档 | 本文档覆盖章节 |
|----------|--------------|
| falconkv_client_design.md | 第 2 节（Client 测试） |
| falconkv_meta_design.md | 第 3 节（Meta 测试） |
| falconkv_store_design.md | 第 4 节（Store 测试） |
| falconkv_transfer_design.md | 第 5 节（Transfer 测试） |
| falconkv_scheduler_design.md | 第 6 节（Scheduler 测试）+ 第 7 节（Bypass 测试） |
| falconkv_design.md（全局） | 第 8 节（集成测试）+ 第 9 节（性能测试）+ 第 10 节（多节点性能测试：本地读写 / 本节点读 / 跨节点读）+ 第 11 节（边界测试） |
