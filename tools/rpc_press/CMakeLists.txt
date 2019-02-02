file(GLOB SOURCES "${PROJECT_SOURCE_DIR}/tools/rpc_press/*.cpp"
        "${PROJECT_SOURCE_DIR}/../../src/proto/vse.pb.cc"
        "${PROJECT_SOURCE_DIR}/../../src/proto/common.pb.cc"
        "${PROJECT_SOURCE_DIR}/../../src/proto/altair.pb.cc"
        "${PROJECT_SOURCE_DIR}/../../src/proto/task.pb.cc"
        "${PROJECT_SOURCE_DIR}/../../src/proto/config.pb.cc"
        )
message("---------" ${SOURCES})
add_executable(rpc_press ${SOURCES})
target_link_libraries(rpc_press brpc-static ${DYNAMIC_LIB} )
