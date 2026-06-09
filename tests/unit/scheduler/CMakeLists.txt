set(SCHEDULER_TESTS
    test_passthrough_policy
    test_node_stats
    test_bypass
)

foreach(TEST_NAME ${SCHEDULER_TESTS})
    add_executable(${TEST_NAME} ${TEST_NAME}.cpp)
    target_link_libraries(${TEST_NAME} PRIVATE
        falconkv_scheduler
        falconkv_common
        GTest::gtest_main
    )
    add_test(NAME ${TEST_NAME} COMMAND ${TEST_NAME})
endforeach()
