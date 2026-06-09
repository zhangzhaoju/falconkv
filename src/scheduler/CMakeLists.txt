add_library(falconkv_scheduler STATIC
    passthrough_policy.cpp
    node_stats.cpp
    scheduler_service.cpp
    scheduler_service_impl.cpp
    scheduler_server.cpp
    scheduler_proxy.cpp
)

target_include_directories(falconkv_scheduler PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/..
    ${CMAKE_SOURCE_DIR}
)

target_link_libraries(falconkv_scheduler PUBLIC
    falconkv_common
    falconkv_proto
    falconkv_transfer
    Threads::Threads
)

if(FALCONKV_HAS_BRPC)
    target_compile_definitions(falconkv_scheduler PUBLIC FALCONKV_HAS_BRPC)
    target_link_libraries(falconkv_scheduler PUBLIC falconkv_brpc_deps)
endif()

# Standalone Scheduler daemon process
add_executable(falconkv_sched falconkv_scheduler_main.cpp)
target_link_libraries(falconkv_sched PRIVATE falconkv_scheduler Threads::Threads)
if(FALCONKV_HAS_BRPC)
    target_link_libraries(falconkv_sched PRIVATE falconkv_brpc_deps)
endif()
install(TARGETS falconkv_sched RUNTIME DESTINATION bin)
