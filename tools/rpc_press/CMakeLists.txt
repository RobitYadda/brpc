file(GLOB SOURCES "*.cpp")
add_executable(rpc_press ${SOURCES})
target_link_libraries(rpc_press brpc-static ${DYNAMIC_LIB})
