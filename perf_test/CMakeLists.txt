include_directories(${PROJECT_SOURCE_DIR}/include)
include_directories(${PROJECT_SOURCE_DIR}/test)

# perf test
add_executable(
    rdma_client
    client.cc
)
target_link_libraries(rdma_client polarkv rdmacm ibverbs ibumad pci)


add_executable(
    rdma_server
    server.cc
)
target_link_libraries(rdma_server polarkv rdmacm ibverbs ibumad pci)