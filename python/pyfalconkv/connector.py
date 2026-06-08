"""
FalconKVConnector - LMCache RemoteConnector implementation using FalconKV.

Uses pyfalconkv.Client (which wraps the C Extension) for all operations.
The Client internally delegates to FalconKVBridge -> FalconKVClientImpl.
"""

import asyncio
import os
from typing import List, Optional
from lmcache.logging import init_logger

try:
    from pyfalconkv.client import Client as FalconKVClient
except ImportError:
    FalconKVClient = None

try:
    from lmcache.v1.storage_backend.connector.base_connector import RemoteConnector
except ImportError:
    RemoteConnector = object

logger = init_logger(__name__)


class FalconKVConnector(RemoteConnector):
    """LMCache RemoteConnector implementation using FalconKV.

    This connector is used by LMCache to store/retrieve KV cache data
    via FalconKV's distributed storage backend.

    Inherits from RemoteConnector to get meta_shapes, meta_dtypes,
    meta_fmt, and reshape_partial_chunk from the base class.
    """

    def __init__(
        self,
        config_file: str,
        cache_capacity: int = 100000,
        async_batch_size: int = 16,
        fire_and_forget: bool = True,
        loop=None,
        local_cpu_backend=None,
    ):
        # 调用 RemoteConnector.__init__ 初始化 meta_shapes,
        # meta_dtypes, meta_fmt 和 reshape_partial_chunk
        if RemoteConnector is not object:
            super().__init__(local_cpu_backend.config, local_cpu_backend.metadata)

        if FalconKVClient is None:
            raise RuntimeError(
                "pyfalconkv module not found. "
                "Build with: ./build.sh build --with-python"
            )

        self.loop = loop
        self.local_cpu_backend = local_cpu_backend
        self._fire_and_forget = fire_and_forget

        # 通过 Client -> FalconKVBridge -> FalconKVClientImpl 初始化
        worker_id = local_cpu_backend.metadata.worker_id
        self._client = FalconKVClient(config_file, cache_capacity,
                                      worker_id=worker_id)
        logger.info(f"RUN_TIME_WORKER_ID value is {worker_id}")

        # 异步并发控制
        self._async_semaphore = asyncio.Semaphore(async_batch_size)

    # ============ Batch operation support ============

    def support_batched_get(self) -> bool:
        return True

    def support_batched_put(self) -> bool:
        return True

    def support_batched_contains(self) -> bool:
        return True

    def support_batched_async_contains(self) -> bool:
        return True

    def support_batched_get_non_blocking(self) -> bool:
        return True

    # ============ Batch operations (core path) ============

    async def batched_get(self, keys) -> List[Optional[object]]:
        """Batch get KV cache data."""
        memory_objs = []
        data_ptrs = []
        sizes = []

        for key in keys:
            obj = self.local_cpu_backend.allocate(
                self.meta_shapes, self.meta_dtypes, self.meta_fmt
            )
            memory_objs.append(obj)
            tensor = obj.tensor
            data_ptrs.append(tensor.data_ptr())
            sizes.append(tensor.numel() * tensor.element_size())

        # 通过 Client -> C Extension -> Bridge -> C++ Core 批量读取
        key_strs = [k.to_string() for k in keys]
        bytes_read_list = await asyncio.to_thread(
            self._client.batch_get_sync, key_strs, data_ptrs, sizes
        )

        results = []
        for i, n_read in enumerate(bytes_read_list):
            if n_read <= 0:
                memory_objs[i].ref_count_down()
                results.append(None)
            elif n_read < sizes[i]:
                results.append(
                    self.reshape_partial_chunk(memory_objs[i], n_read)
                )
            else:
                results.append(memory_objs[i])
        return results

    async def batched_put(self, keys, memory_objs):
        """Batch put KV cache data."""
        key_strs = [k.to_string() for k in keys]
        data_ptrs = []
        sizes = []

        for obj in memory_objs:
            tensor = obj.tensor
            data_ptrs.append(tensor.data_ptr())
            sizes.append(tensor.numel() * tensor.element_size())

        if self._fire_and_forget:
            # Fire-and-Forget: 增加引用计数，C++ 后台线程完成后释放
            for obj in memory_objs:
                obj.ref_count_up()
            self._client.fire_and_forget_put(
                key_strs, data_ptrs, sizes, memory_objs
            )
        else:
            # 同步等待写入完成
            await asyncio.to_thread(
                self._client.batch_put_sync, key_strs, data_ptrs, sizes
            )
            for obj in memory_objs:
                obj.ref_count_down()

    def batched_contains(self, keys) -> int:
        """Batch check key existence, returns hit count."""
        key_strs = [k.to_string() for k in keys]
        return self._client.batch_exist_sync(key_strs)

    async def batched_async_contains(self, lookup_id, keys, pin=False) -> int:
        """Async batch contains check."""
        return self.batched_contains(keys)

    async def batched_get_non_blocking(self, lookup_id, keys):
        """Non-blocking batch get, returns continuous prefix."""
        results = await self.batched_get(keys)
        for i, r in enumerate(results):
            if r is None:
                for j in range(i + 1, len(results)):
                    if results[j] is not None:
                        results[j].ref_count_down()
                return results[:i]
        return results

    # ============ Single key operations (fallback) ============

    async def exists(self, key) -> bool:
        return self._client.batch_exist_sync([key.to_string()]) > 0

    def exists_sync(self, key) -> bool:
        return self._client.batch_exist_sync([key.to_string()]) > 0

    async def get(self, key):
        results = await self.batched_get([key])
        return results[0] if results else None

    async def put(self, key, memory_obj):
        await self.batched_put([key], [memory_obj])

    # ============ Lifecycle ============

    async def close(self):
        if self._client:
            self._client.close()
            self._client = None

    async def list(self) -> List[str]:
        raise NotImplementedError("FalconKV does not support list operation")
