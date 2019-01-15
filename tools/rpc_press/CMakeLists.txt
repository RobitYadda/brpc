file(GLOB SOURCES "${PROJECT_SOURCE_DIR}/tools/rpc_press/*.cpp" "${PROJECT_SOURCE_DIR}/tools/rpc_press/proto/*.pb.cc")
add_executable(rpc_press ${SOURCES})
target_link_libraries(rpc_press brpc-static ${DYNAMIC_LIB} )
