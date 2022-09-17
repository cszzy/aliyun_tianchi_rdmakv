include_directories(${PROJECT_SOURCE_DIR}/include)
include_directories(${PROJECT_SOURCE_DIR}/test)

# perf test
add_executable(
    client
    client.cc
)
target_link_libraries(client polarkv rdmacm ibverbs ibumad pci)


add_executable(
    server
    server.cc
)
target_link_libraries(server polarkv rdmacm ibverbs ibumad pci)

add_executable(
    bitmap_test
    bitmap_test.cc
)
target_link_libraries(bitmap_test)