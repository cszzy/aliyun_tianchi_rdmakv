include_directories(${PROJECT_SOURCE_DIR}/include)
include_directories(${PROJECT_SOURCE_DIR}/test)

# perf test
add_executable(
    perf_client
    client.cc
)
target_link_libraries(perf_client polarkv rdmacm ibverbs ibumad pci)


add_executable(
    perf_server
    server.cc
)
target_link_libraries(perf_server polarkv rdmacm ibverbs ibumad pci)