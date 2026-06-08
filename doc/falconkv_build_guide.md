# FalconKV 构建指导说明书

## 1. 概述

### 1.1 系统要求

| 项目 | 最低要求 |
|------|----------|
| 操作系统 | Ubuntu 20.04+ / openEuler 22.03+ |
| 编译器 | GCC 8+（需支持 C++17） |
| CMake | >= 3.16 |
| 构建工具 | Ninja（推荐）或 Make |
| 磁盘 | 构建需约 500MB 可用空间 |

### 1.2 依赖总览

| 依赖 | 版本建议 | 安装方式 | 说明 |
|------|----------|----------|------|
| Protobuf | >= 3.12 | 包管理器 | Protocol Buffers，RPC 通信基础 |
| brpc | >= 1.6 | 源码编译 | Apache BRPC，RPC 框架 |
| glog | >= 0.5 | 包管理器 | Google Logging，日志库 |
| gflags | >= 2.2 | 包管理器 | Google Flags，命令行参数解析 |
| jsoncpp | >= 1.9 | 包管理器 | JSON 解析，配置文件加载 |
| OpenSSL | >= 1.1 | 包管理器 | SSL/TLS，brpc 依赖 |
| LevelDB | >= 1.22 | 包管理器 | KV 存储引擎，brpc 依赖 |
| liburing | >= 2.0 | 包管理器 | 可选，io_uring 异步 IO 引擎（缺失时自动降级为线程池） |
| GTest | >= 1.14 | 自动获取 | Google Test，单元测试（缺失时自动从 GitHub 拉取） |
| Python3 | >= 3.8 | 包管理器 | 可选，仅构建 Python 绑定时需要 |

---

## 2. 第三方依赖安装

### 2.1 Ubuntu（20.04 / 22.04 / 24.04）

#### 2.1.1 安装系统工具链

```bash
sudo apt update
sudo apt install -y build-essential cmake ninja-build git
```

#### 2.1.2 通过包管理器安装依赖

```bash
sudo apt install -y \
    libprotobuf-dev protobuf-compiler libprotoc-dev \
    libgoogle-glog-dev \
    libgflags-dev \
    libjsoncpp-dev \
    libssl-dev \
    libleveldb-dev \
    liburing-dev
```

**Ubuntu 各版本默认包版本参考：**

| 依赖 | Ubuntu 20.04 | Ubuntu 22.04 | Ubuntu 24.04 |
|------|-------------|-------------|-------------|
| Protobuf | 3.6.1 | 3.12.4 | 3.21.12 |
| glog | 0.4.0 | 0.5.0 | 0.6.0 |
| gflags | 2.2.2 | 2.2.2 | 2.2.2 |
| jsoncpp | 1.7.4 | 1.9.5 | 1.9.5 |
| OpenSSL | 1.1.1 | 3.0.2 | 3.0.13 |
| LevelDB | 1.22 | 1.23 | 1.23 |
| liburing | 不可用 | 2.0 | 2.5 |

> **注意：** Ubuntu 20.04 的 Protobuf 版本（3.6.1）偏低，建议手动安装 3.12+ 或升级系统。若遇到编译问题，参见 [2.1.4 常见问题](#214-常见问题ubuntu)。

#### 2.1.3 源码编译安装 brpc

brpc 未包含在 Ubuntu 标准软件源中，需从源码编译。

```bash
# 克隆源码
git clone https://github.com/apache/brpc.git
cd brpc

# 使用 Release 模式构建
mkdir build && cd build
cmake -G Ninja \
    -DCMAKE_BUILD_TYPE=Release \
    -DBUILD_SHARED_LIBS=OFF \
    -DWITH_GLOG=ON \
    ..
ninja -j$(nproc)
sudo ninja install
```

构建完成后：
- 静态库安装至 `/usr/local/lib/libbrpc.a`
- 头文件安装至 `/usr/local/include/brpc/`

更新动态链接库缓存：

```bash
sudo ldconfig
```

#### 2.1.4 常见问题（Ubuntu）

**Q: cmake 找不到 brpc？**

确保 `cmake` 能搜索到 `/usr/local` 路径。如果仍然找不到，可在构建 FalconKV 时手动指定：

```bash
cmake -G Ninja \
    -DCMAKE_PREFIX_PATH="/usr/local" \
    ..
```

**Q: Ubuntu 20.04 的 Protobuf 版本太低？**

从源码安装 Protobuf 3.21：

```bash
git clone https://github.com/protocolbuffers/protobuf.git
cd protobuf && git checkout v3.21.12
git submodule update --init --recursive
mkdir build && cd build
cmake -G Ninja \
    -DCMAKE_BUILD_TYPE=Release \
    -Dprotobuf_BUILD_TESTS=OFF \
    -Dprotobuf_BUILD_SHARED_LIBS=ON \
    ..
ninja -j$(nproc)
sudo ninja install
sudo ldconfig
```

**Q: io_uring 相关测试被跳过？**

io_uring 引擎需要 `liburing-dev`。若未安装，构建仍可成功，但运行时自动降级为线程池模式。安装后重新构建即可：

```bash
sudo apt install -y liburing-dev
# 重新构建
cd build && cmake -G Ninja -DCMAKE_BUILD_TYPE=Release .. && ninja -j$(nproc)
```

---

### 2.2 openEuler（22.03 / 24.03）

#### 2.2.1 安装系统工具链

```bash
sudo dnf install -y gcc-c++ cmake ninja-build git
```

#### 2.2.2 通过包管理器安装依赖

```bash
sudo dnf install -y \
    protobuf-devel protobuf-compiler protobuf-lite-devel \
    glog-devel \
    gflags-devel \
    jsoncpp-devel \
    openssl-devel \
    leveldb-devel \
    liburing-devel
```

**openEuler 各版本包可用性参考：**

| 依赖 | openEuler 22.03 | openEuler 24.03 | 备注说明 |
|------|----------------|----------------|----------|
| Protobuf | protobuf-devel | protobuf-devel | 通常可用 |
| glog | glog-devel | glog-devel | 可能需 EPL 仓库 |
| gflags | gflags-devel | gflags-devel | 通常可用 |
| jsoncpp | jsoncpp-devel | jsoncpp-devel | 通常可用 |
| OpenSSL | openssl-devel | openssl-devel | 系统自带 |
| LevelDB | leveldb-devel | leveldb-devel | 可能需 EPL 仓库 |
| liburing | liburing-devel | liburing-devel | 可能需 EPL 仓库 |

> **注意：** 若部分 `-devel` 包无法直接安装，请先启用 EPL 仓库：
> ```bash
> sudo dnf install -y epel-release   # 如可用
> sudo dnf makecache
> ```
> 若仓库中确实缺失，参见 [2.2.4 源码编译缺失依赖](#224-源码编译缺失依赖openeuler)。

#### 2.2.3 源码编译安装 brpc

brpc 未包含在 openEuler 标准软件源中，需从源码编译。

```bash
# 克隆源码
git clone https://github.com/apache/brpc.git
cd brpc

# 使用 Release 模式构建
mkdir build && cd build
cmake -G Ninja \
    -DCMAKE_BUILD_TYPE=Release \
    -DBUILD_SHARED_LIBS=OFF \
    ..
ninja -j$(nproc)
sudo ninja install
sudo ldconfig
```

#### 2.2.4 源码编译缺失依赖（openEuler）

若通过 `dnf` 无法安装某些依赖，可从源码编译。以下列出常见缺失库的编译步骤。

**glog（若 glog-devel 不可用）：**

```bash
git clone https://github.com/google/glog.git
cd glog && git checkout v0.6.0
mkdir build && cd build
cmake -G Ninja -DCMAKE_BUILD_TYPE=Release -DBUILD_SHARED_LIBS=ON ..
ninja -j$(nproc)
sudo ninja install
sudo ldconfig
```

**gflags（若 gflags-devel 不可用）：**

```bash
git clone https://github.com/gflags/gflags.git
cd gflags && git checkout v2.2.2
mkdir build && cd build
cmake -G Ninja -DCMAKE_BUILD_TYPE=Release -DBUILD_SHARED_LIBS=ON ..
ninja -j$(nproc)
sudo ninja install
sudo ldconfig
```

**LevelDB（若 leveldb-devel 不可用）：**

```bash
git clone https://github.com/google/leveldb.git
cd leveldb && git checkout 1.23
mkdir build && cd build
cmake -G Ninja -DCMAKE_BUILD_TYPE=Release -DBUILD_SHARED_LIBS=ON ..
ninja -j$(nproc)
sudo ninja install
sudo ldconfig
```

**Protobuf（若版本不满足要求）：**

```bash
git clone https://github.com/protocolbuffers/protobuf.git
cd protobuf && git checkout v3.21.12
git submodule update --init --recursive
mkdir build && cd build
cmake -G Ninja \
    -DCMAKE_BUILD_TYPE=Release \
    -Dprotobuf_BUILD_TESTS=OFF \
    -Dprotobuf_BUILD_SHARED_LIBS=ON \
    ..
ninja -j$(nproc)
sudo ninja install
sudo ldconfig
```

---

## 3. 构建与安装

### 3.1 获取源码

```bash
git clone <FalconKV 仓库地址>
cd falconkv
```

### 3.2 构建项目

FalconKV 使用 `build.sh` 脚本封装了完整的构建流程。

**Release 构建（默认）：**

```bash
./build.sh
```

**Debug 构建：**

```bash
./build.sh build falconkv --debug
```

**手动构建（等同于 build.sh 内部逻辑）：**

```bash
mkdir -p build && cd build
cmake -G Ninja -DCMAKE_BUILD_TYPE=Release ..
ninja -j$(nproc)
```

**自定义安装路径：**

```bash
./build.sh --install-dir /opt/falconkv
```

**查看详细编译输出：**

```bash
./build.sh --verbose
```

### 3.3 运行测试

```bash
./build.sh test
```

此命令会运行全部 34 个单元测试。也可手动运行指定测试：

```bash
cd build
ctest --output-on-failure -R test_slot_allocator    # 运行单个测试
ctest --output-on-failure -j$(nproc)                  # 并行运行全部测试
```

### 3.4 安装

默认安装至 `/usr/local/falconkv`：

```bash
./build.sh install
```

指定自定义路径：

```bash
./build.sh install --install-dir /opt/falconkv
```

安装目录结构：

```
/usr/local/falconkv/
├── bin/          # 可执行文件（falconkv_master, falconkv_sched）
├── lib/          # 库文件
└── include/      # 头文件
```

### 3.5 构建 Python 绑定（可选）

若需要 Python 绑定（用于 LMCache 集成），需先安装 Python3 开发头文件：

**Ubuntu：**

```bash
sudo apt install -y python3-dev
```

**openEuler：**

```bash
sudo dnf install -y python3-devel
```

然后使用 `--with-python` 选项构建：

```bash
./build.sh build --with-python
```

构建完成后，C Extension 模块 `_pyfalconkv_internal.cpython-3XX-x86_64-linux-gnu.so` 会自动复制到 `python/pyfalconkv/` 目录下。

### 3.6 安装 Python 包（pyfalconkv）

构建完成后，需将 `pyfalconkv` 包安装到 Python 环境中。项目提供了 `setup.py`，支持以可编辑模式（开发模式）或正式模式安装。

#### 3.6.1 可编辑模式安装（推荐开发使用）

```bash
cd python
pip install -e .
```

此模式下修改 Python 代码后无需重新安装即可生效。但若 C Extension 发生变更，需重新执行 `./build.sh build --with-python`。

#### 3.6.2 正式模式安装

```bash
cd python
pip install .
```

#### 3.6.3 验证安装

安装完成后，可通过以下方式验证：

```bash
# 验证包是否可导入
python3 -c "import pyfalconkv; print(pyfalconkv.__version__)"

# 验证 C Extension 是否加载成功
python3 -c "from pyfalconkv import _pyfalconkv_internal; print('C Extension loaded')"
```

预期输出：

```
0.1.0
C Extension loaded
```

> **注意：** 若提示 `_pyfalconkv_internal C extension not found`，说明未使用 `--with-python` 选项构建，或 `.so` 文件未正确复制到 `python/pyfalconkv/` 目录。请先执行 `./build.sh build --with-python`。

#### 3.6.4 包结构说明

安装后的 `pyfalconkv` 包结构如下：

```
pyfalconkv/
├── __init__.py                          # 包入口，导出 Client 类
├── client.py                            # 高层 Python Client 封装
├── adapter.py                           # LMCache 适配器（falconkv:// URL 自动发现）
├── connector.py                         # LMCache RemoteConnector 实现
└── _pyfalconkv_internal.cpython-*.so    # C Extension 模块（编译产物）
```

#### 3.6.5 基本使用示例

```python
from pyfalconkv import Client

# 创建客户端（需提供 FalconKV 配置文件路径）
client = Client(config_file="/usr/local/falconkv/config/falconkv.json")

# 批量检查 key 是否存在
hit_count = client.batch_exist_sync(["key1", "key2", "key3"])

# 批量写入
client.batch_put_sync(keys, data_ptrs, sizes)

# 批量读取
results = client.batch_get_sync(keys, data_ptrs, sizes)

# 关闭客户端
client.close()
```

---

## 4. 快速验证

### 4.1 验证依赖安装

构建前可先检查关键依赖是否就绪：

```bash
# 检查 protobuf
protoc --version

# 检查 brpc
ls /usr/local/lib/libbrpc.a 2>/dev/null || ls /usr/lib*/libbrpc.a 2>/dev/null

# 检查 glog
pkg-config --modversion libglog 2>/dev/null || dpkg -l libgoogle-glog-dev 2>/dev/null
```

### 4.2 验证构建结果

```bash
# 检查构建产物
ls build/src/meta/falconkv_master       # Meta 服务可执行文件
ls build/src/scheduler/falconkv_sched   # Scheduler 服务可执行文件

# 运行全部测试
./build.sh test
```

预期输出：

```
100% tests passed, 0 tests failed out of 34
```

### 4.3 验证安装

```bash
ls /usr/local/falconkv/bin/falconkv_master
ls /usr/local/falconkv/bin/falconkv_sched
```

---

## 5. 依赖关系图

```
FalconKV
├── falconkv_common  ──→ glog, jsoncpp, pthread
├── falconkv_proto   ──→ protobuf
├── falconkv_meta    ──→ brpc (→ openssl, gflags, glog, leveldb)
├── falconkv_store   ──→ brpc, liburing (可选)
├── falconkv_transfer ──→ brpc
├── falconkv_scheduler ──→ brpc
└── falconkv_client  ──→ 以上所有模块
```

所有模块通过 `falconkv_common` 的 `PUBLIC` 依赖自动获得 glog，无需在各模块 CMakeLists 中重复声明。
