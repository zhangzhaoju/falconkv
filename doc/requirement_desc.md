需求描述:

我需要一款能够对接lmcache/mooncake加速LLM推理的低时延/高吞吐的分布式KV存储系统,  该系统主体代码使用C++17实现,分为如下几个模块:

1. meta 模块用于管理元数据
2. transfer模块用于数据传输/元数据通信
   1. store模块用于本地数据读写后经由transfer模块将数据交给client返回给 lmcache/mooncake

3. client 模块用于对接lmcache/mooncake



client模块功能说明:

client模块接口实现需要适配LMCache的RemoteConnector, 因此需要实现如下主要接口:

BatchGet(keys, vals)/BatchPut(keys, vals)/BatchExist(keys)

所有接口均需提供同步和异步两套实习, 同时实现是需要考虑高性能需要考虑GIL锁/0Copy等因素.


BatchExist(keys)处理逻辑大致为: 

1. 将所有的keys封装成一个大消息发送到meta 模块, meta模块返回命中的key 描述信息(数据存放位置)
2. 接收返回消息, 缓存key信息(因为很快会调用get,避免再次去meta查询建议进行缓存), 返回命中keys数量



BatchPut(keys, vals)处理逻辑大致为:

1. 基于keys信息去meta 模块申请数据存放节点(store节点信息)和vals存放位置信息,  meta模块返回分配结果
2. 接收meta返回信息后, 调用transfer将数据写到对应的 store节点
3. 数据全部落盘后批量更新meta模块的元数据信息,主要是更新stat状态为done
4. 为了提升效率可以进行预分配, 预分配空间在client推出时自动回收(meta进行连接监控)


BatchGet(keys, vals)处理逻辑大致为:

1. 基于从meta获取的key 描述信息, 调用transfer模块接口,将数据内容放到vals中

Transfer模块负责维护链接关系:

1. Tranfer模块需要进行接口抽象, 方便适配多种协议, 当前可以仅使用brpc



meta模块功能说明:

1. meta模块基于pg来存储元数据,

2. pg操作使用lowlevel接口,提升性能

   元数据表数据包含(每个store节点独立建表,基于key值hash拆分成多个元数据表来降低冲突):

   key, offset, size, time, stat(start, done, evict)

   key对应KV key值, offset表示数据在该文件内的位置偏移(需要做页面对齐提升效率), Size表示该数据的长度, time 表示最后一次数据访问时间, stat表示状态.

   在put申请空间时状态为start/写完后状态切换为done/回收后更新为evict

 3. meta模块需要维护Store模块的空间,   KVCache场景数据块大小固定, 因此可以参考linux内核管理内存方案管理每个store上的空间

 4. client在BatchPut中调用alloc申请空间时, 遵循数据亲和+负载均衡原则; client和store在同一节点, 且store空间使用率低于70 则优先分配到 client节点上的store; 否则分配到剩余容量最大的store上;

 5. store空间回收,  定时扫描store对应的表, 将 time 长时间未刷新的数据进行驱逐

    // 由于没有考虑内存命中导致的SSD不访问, time不刷新场景, 这里的驱逐可能存在误判, 需要带后续完善内存方案后解决该问题


store模块功能说明:

​     store和client在同一进程空间,store模块在启动时向meta模块注册自己的NodeID和空间大小到meta, 同时store根据自身SSD空间大小创建大文件用来存放数据.

​     数据读写均操作同一个文件仅是操作位置不同, 因此需要使用directIO, 同时需要去除文件元数据中修改时间的更新来提升性能.

请参考/home/zhangzhaoju/Learn/code目录下的falconfs项目/ LMCache项目 设计一套完善的方案, 并将方案按照全局(falconkv_design.md) 模块(falconkv_client_design.md/falconkv_meta_design.md/falconkv_transfer_design.md/falconkv_store_design.md)的方式完成设计,并将方案设计输出到对应文档.

第二轮补充设计:

当前LMCache/mooncake接入vLLM推理框架时是每个GPU一个推理进程, 对应一个FalconKV的client,   每个节点上一般是8个GPU/NPU(Worker),  实际上每个Node上有多个Client,  多个Store; 

同一节点上的多个Client和Store是会存在IO争抢的,  在同一节点我们需要一个调度模块来进行IO调度, 思路是创建一个独立的IO调度模块;

该模块负责控制本节点内Client和Store的IO调度, 本节点内的Client在从Meta获取Key信息后 发消息给调度模块, 告诉调度模块自己需要IO的信息(StoreID, Size), 由调度模块来返回该Client发起IO的时间点; Store节点接收到其他Client的RPC读写请求时也要上报给调度模块.

同时Client在IO完成后需要向调度模块上报自己的IO启动时间,IO完成时间, IO Size信息,  调度模块需要根据这些信息计算本节点的带宽和时延信息, 并应用于调度.

调度模块需要定期打印吞吐和时延统计信息.

当前对于同一类型的IO调度策略可以先直接使用放通模式, 先完成本节点内IO带宽和时延的汇总统计; 由于LLM推理场景, 多个IO是瞬时并发的, 需要提供机制判断峰值吞吐是否可以打满硬件带宽(即需要对同一时间段内多个IO请求的带宽进行累加)

同时调度模块并非是必须的, 当调度模块异常退出或hang时Client需要能够快速感知, 并bypass调度, 请增加调度模块的设计; 

需要提供调度设计文档, 更新总的设计文档/Client/Store设计

第三轮补充设计:

1. Client和Store共进程启动, 同一物理节点上的Store 是互相可见的可以互相读写文件内容,  在做亲和性选择时同节点文件优先,且可以直接读写, 请基于该信息重新设计Meta的数据分配和Client的读写.

第四轮补充设计:

调度模块的IO申请类型分为 网络IO读写和本节点SSDIO两类(本节点SSD IO分为通过网络后Store接受的请求和 本地Client发起的请求), 网络IO请求需要根据目的地址进行区分,不同的写入方向使用不同的网络通道.  当前调度模块统计只分了READ/WRITE两种类型, 无法体现各个连接通道到的具体负载, 无法支撑进行后续的流控调度, 请优化调度模块/Client/Store的设计.

第五轮补充设计:

Store模块FalconKVStore::InitDataFile中进行初始化文件时,  所有Store的文件名是相同的, 使用不同的文件名更加合理, 文件名建议拼接store_id, 每个Store有唯一的store_id, 启动时从配置文件读取;(真实上线可以是NodeId凭借GPUID/NPUID)

告诉AI按照设计文档进行开发, 先完成了框架搭建, 内部比较多的实现AI都使用了取巧方法:

1. 使用BRPC 进行Transfer 通信直接没有实现, 使用同进程内函数调用替换
2. 元数据存储使用内存且不支持并发的方案, 并没有使用PostGres
3. client侧批量读写应该使用stor进行,  被简化成了cient直接调用接口进行读写

第六轮交互:(在框架搭建完成后, 按模块确认代码和设计一致)

原则:每次只做一件事情, 避免AI多目标变傻

目标1: 当前实现Client和Meta模块运行在同一个进程, 按照doc目录的设计方案Client和Meta是分开部署的, 需要基于RPC通信来进行元数据信息的查询和更新, 请基于设计文档实现Client和Meta的交互. 

目标2: Client不能直接调用操作文件接口, Client中读写本机的文件需要通过Store模块来进行, Client不直接看到文件, 请基于这个原则更新Client和Store的设计文档, 然后基于新的设计更新实现.  对于本节点ACCESS_NODE_DIRECT的文件, 需要在本进程的Store内通过inproc 函数调用直接读写进行加速, 请更新实现方案.


第七轮交互:(做了重新思考,结果如下)
1. 每个Store持有一个文件, 其剩余空间管理由Meta中心模块转移到Store内部自行管理, 由Store自己基于伙伴内存管理算法进行空间分配和回收.
   Store在完成元数据落盘后或回收后, 将变动的元数据信息更新到Meta节点.
2. 每个Client 绑定一个Store, 该Client只能往自己的Store中写数据(inproc 写).
3. Store本地持有自己的一份元数据信息方便本地快速查找, Client在进行Meta数据查询时优先查询本地, 本地匹配不到的剩余Key再去Meta查询
   数据读取根据Meta查询结果进行读取,和当前处理逻辑一致.


第八轮交互:
1. 为了避免Store节点的数据清理期间在其他节点触发读取(清理期间的数据读取行为不可预知), Store节点需要先去Meta节点删除元数据;
2. 同时为了避免元数据删除的同时, 其他节点已经取到该节点元数据并触发读取,需要去除Client的KeyDescCache缓存查询处理
3. Store节点在通知Meta节点删除元数据后, 将本节点的元数据迁移到临时队列, 临时队列内的元数据保留5S后再真正回收到空闲空间, 避免删除和读取的并发引入问题.

第九轮交互:
Meta模块元数据实现内存存储即可, 当前不考虑PostGres方案, 请删除PostGres相关设计描述, 内存实现方案需要考虑各接口的并发高性能处理, 请基于当前代码和设计优化设计.

第十轮交互:
当前Schedule和Client/Store的实现没有打通, 即Client和Store在读写操作时并没有向Scheduler模块上报信息, 请基于falconkv_scheduler_design.md 中的设计完善这部分实现.


第十一轮交互:
falconKV当前并没有实现python到C++接口的调用, 请基于falconkv_client_design.md的
falconfs/python_interface/_pyfalconfs_internal/
/home/zhangzhaoju/Learn/code/LMCache/lmcache/v1/storage_backend/connector/falcon_connector.py
/home/zhangzhaoju/Learn/code/LMCache/lmcache/v1/storage_backend/connector/falcon_adapter.py

第十二轮交互:
请在FalconKVBridge中持有FalconKVStore并完成其初始化, 并建立FalconKVStore和FalconKVClientImpl的关系绑定

第十三轮交互:
当前ACCESS_NODE_DIRECT读模式存在, 但是并没有端到端的打通实现, client在调用BatchExist时返回的所有recorde均默认使用ACCESS_REMOTE_RPC; 请在config中增加node_id参数用于标记store和client的节点id,若record中记录的node_id和 client的node_id一致则需要使用ACCESS_NODE_DIRECT; 


第十四轮交互:
Meta模块需要创建独立进程,进程名叫falconkv_master, falconkv_master独立启动; 
   同时若falconkv_master没有启动各store就无法和meta模块建立连接, 此时client进行元数据查询时跳过远程查询, 仅使用本地cache, 各store定期重连meta, 重连成功则重新注册并推送本地全量Key信息到meta模块; 即在meta模块进程falconkv_master没有启动或异常时所有客户端也可以正常工作, 在falconkv_master正常工作时额外提供从其他Store获取kvcache数据的能力.

第十五轮交互:
scheduler模块也是一个独立启动的进程, 进程名叫falconkv_scheduler, 这个进程需要独立启动监控和调度本节点的数据流, 请补充falconkv_scheduler的启动入口设计和实现.

第十六轮交互:
请使用glog作为falconkv的日志打印三方库

第十七论交互:
当前Store写数据前分配空间固定使用chunk_size, 而不是数据的真实大小, 请按照数据的真实大小进行空间分配; 数据Evict时也需要基于真实Size进行回收
同时请检查代码chunk_size是否真的有用, 如无用请删除chunck_size相关的配置和代码

第十八轮交互:
当前每个进程的日志都被拆分成了三个文件, INFO/WARNING/ERROR 请分析方案实现共用日志文件; 请为每个进程生成一份日志文件,日志中包含INFO/WARNING/ERROR三种日志, 日志文件名需要包含{进程名}_进程ID_时间信息, client时进程名使用falconkv_client来代替.

第十九轮交互:
当前的测试日志都打印到目录/tmp/falconkv_log, 无法找到日志和用例的对应关系.  请分析方案基于用例设计对日志进行分目录存放, 共用进程的用例可以放到同一目录或文件,
不共用进程的用例需要根据用例进行分目录存放.

第二十轮交互:
在同一Store节点, 在对同一Key值重复put时, 会进行空间的重新申请并将新的StoreKeyRecord记录到StoreMetaIndex, 此时会导致旧的StoreKeyRecord失效, 但并没有看到将旧的StoreKeyRecord对应的地址进行回收, 请优化put处理逻辑若StoreMetaIndex中已经有对应记录,则本次操作跳过(打印一条info日志,说明已经被插入过); 避免重复操作或空间泄漏;

在FalconKVStore::Put和FalconKVStore::BatchPuts均没有对access_time_ms赋初始值, 请确认这是否存在问题;

请确认StoreMetaIndex::Commit和StoreMetaIndex::CommittedCount是否真的需要, StoreMetaIndex::GetAllCommittedEntries命名是否合理

第二十一轮交互:
当前的端到端测试用例中没有触发Evict 和 Scheduler统计, 请完善测试设计
1. 请完善端到端用例设计需要包含Store的cap空间写满后触发Evict,旧的数据成功触发清理, 新的数据可以重新写入.
2. 请完善端到端用例设计需要Scheduler够统计到写吞吐和三种读吞吐, 并可以在日志中观察到吞吐信息.

第二十二轮交互:
当前已经有单元测试/模块测试/集成测试, 还缺少端到端的性能测试, 请基于如下要求完成性能测试的设计和开发
1. 性能测试要求启动多个client端(模拟同主机和跨主机)/启动meta端/启动Schedule
2. 需要可以配置各Client的Node_id, Store的容量, 每次读写的Size, 总的测试总时长(单位秒), 可以基于统一的配置文件生成.
3. 在测试总时长内, 各client并发发起批量读写, 存在跨client使用key值一致的情况,即会出现同节点读/跨节点读数据 
4. 需要给出一键启动脚本, 启动多个客户端/meta/schedule;
5. 每次读取前需要先调用Exist判断是否存在

第二十三轮交互:
当前falconkv.json 的common::scheduler_enabled和scheduler::enabled 是否存在语义重复, 请确认;       
若重复请合并配置项到common模块同时修改项目中的设计/实现/测试用例

第二十四轮交互:
当前KeyDescCache的LRU算法很低效,lru_list_是一个简单链表, 链表中存的是key值, 这导致每次查询后将查询到的key移到lru_list尾部时, 需要遍历完整队列在push_back, 这很低效,性能极差; 请设计高性能的LRU淘汰机制支撑高性能.

第二十五轮交互:
当前store的驱逐机制存在如下问题,请设计优化方案 
1. 当前写入过快时, 清理不及时导致分片空间失败, 需要在lease清理的基础上提供一种强制清理机制, 当前定时基于cold_threshold_ms清理可能导致空间无法被清理(例如空间过小,都是热数据)
   请先替换为LRU清理, 避免写失败
2. 当前的清理遍历机制效率过低, 需要修改数据结构为 hash+链表的LRU结构
3. 在store put阶段若alloc空间失败, 需要立即触发清理, 在清理完成后重新申请空间; 这里需要打印日志明确空间耗尽,触发了主动清理

第二十六轮交互:
当前FalconKVStore::BatchPut并没有使用ioring机制也没有使用多线程并发机制提升性能, FalconKVStore::BatchRead直接使用了 std::async并不高效, 请结合ioring和固定线程池提供高性能的并发方案设计

第二十七轮交互:
在falconkv 和lmcache集成时需要从lmcache中获取workid,并传递给falconkv作为store_id使用;lmcache中获取workid的方式为 worker_id = os.getenv("LMCACHE_PLUGIN_WORKER_ID", "0"),请修改falconkv实现该机制, 同时由于vllm推理集群一般会存在多个节点每个节点内的worker_id唯一,节点间不唯一;为了确保store_id全局唯一,     不能直接将worker_id 作为falconkv的store_id, 需要使用falconkv的node_id * 10+work_id作为falconkv的store_id避免全局空间冲突.

第二十八轮交互:
当前FalconKVStore::Write和FalconKVStore::Read在判断未做对齐后会进行对齐处理和内存copy后再次使用原fd写入, 重新对齐后进行的读写位置均发生了变化.
请修改为对文件打开两次, 分别使用O_DIRECT模式和非O_DIRECT模式, 当前配置开启o_direct模式且满足对齐条件时使用 direct 对应的fd进行读写, 否则使用非direct的fd进行读写;
请分析该方案是否可行,若可行请基于当前代码实施修改.

第二十九轮交互:
当前falconkv的客户端在进行ACCESS_LOCAL_DIRECT数据读写时,已经支持了IORING和多线程并发处理, 但是ACCESS_NODE_DIRECT依然采用低效的单线程循环处理, 请参考ACCESS_LOCAL_DIRECT的处理机制为ACCESS_NODE_DIRECT也实现多线程和IO_RING处理

第三十轮交互:
当前store模块已经使用双fd机制来进行文件读写, 对其且支持direct_io就是用direct_io对应的fd, 否则使用buffer_fd; 因此请分析确认是否还有必要保留慢速读写逻辑, 如不需要请给出整改方案.

第三十一轮交互:
当前falconkv的store模块使用SlotAllocator进行文件空间分配, 每次分配的size固定, 但是需要从配置文件中读取每次的分配size; 这需要在部署时识别模型需要存储的size, 这增加了对部署人员的知识要求;
虽然模型部署后每次使用的size都是固定的,但不易在部署时获得具体是多大, falconkv需要根据alloc设置size大小而不是启动时设定, 且若申请多次申请的size大小发生变化需要抛出异常,表示当前不支持多种size;
请设计方案支持这种情况.


第三十二轮交互:
当前在FalconKVBridge::FalconKVBridge中配置类brpc最大的传输size为512M, 由于对BRPC的数据传输做了批处理, 若一个批次的总size操作512M会失败, 因此需要基于设置的brpc最大传输size设置brpc数据传输batch的size. 请基于该情况分析优化方案.

第三十三轮交互:
当前同一节点的VLLM会拉起多个 Worker, 每个worker动态加载FalconKVConnector, 为了简化配置所有FalconKVConnector共用同一个配置falconkv.json, 但是falconkv.json中配置的store连接信息 store_rpc_host和listen_port是固定的, 为了确保每个FalconKVConnector有独立的监听端口, 需要将falconkv.json配置的listen_port 添加一个基于store_id的偏移值, 即实际的监听端口listen_port = 配置的listen_port+ worker_id; worker_id 通过PyWrapper_Init接口传入, 并用于计算store_id; 请给出完整的方案

其他待处理:
1. FireAndForgetPut的逻辑存在错误, 需要将FireAndForgetPut的优先级低于读数据的优先级别, 但是当前并没有这种处理
   可以先对接待后续完善
2. 当前并能不支持从磁盘恢复Meta信息, 后续使用pwritev+preadv 读写meta + data的模式存放数据, 同时通过bitmap来标记有效block位置, 定期持久化bitmap来用于数据恢复.
3. 当前rpc通信存在延时问题,需要将速率统计的的RPC消息修改为提交异步任务上报速率统计
4. 考虑如何支持集成HCCL进行通信
5. FalconKVClientImpl::DoRead 和 local_store_addr_ 貌似不再使用了需要清理
