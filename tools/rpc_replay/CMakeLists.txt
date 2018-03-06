file(GLOB SOURCES "*.cpp")
add_executable(rpc_replay ${SOURCES})
target_link_libraries(rpc_replay brpc-static ${DYNAMIC_LIB})
