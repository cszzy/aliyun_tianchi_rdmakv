cmake_minimum_required(VERSION 2.8)
project(POLAR_CONTEST)

set(CMAKE_BUILD_TYPE "Release")

set(RDMA_LIB "-lrdmacm -libverbs -libumad -lpci")
set(IPP_LIB "-lippcp")
set(CMAKE_CXX_FLAGS "-std=c++17 ${CMAKE_CXX_FLAGS}")
set(CMAKE_CXX_FLAGS "-pthread -ldl -lrt ${IPP_LIB} ${RDMA_LIB} -${CMAKE_CXX_FLAGS}")
# set(CMAKE_CXX_FLAGS "-pthread -ldl -lrt ${RDMA_LIB} -${CMAKE_CXX_FLAGS}")

if( CMAKE_BUILD_TYPE STREQUAL "Debug" )
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g -Wall -Wno-unused-variable")
else( CMAKE_BUILD_TYPE STREQUAL "Release" )
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O3 -fopenmp")
endif( CMAKE_BUILD_TYPE STREQUAL "Debug" )

message("Build Type: ${CMAKE_BUILD_TYPE} ${CMAKE_CXX_FLAGS}")

set(CMAKE_INSTALL_PREFIX ${CMAKE_BINARY_DIR}/polarkv)
MESSAGE("Install prefix is: ${CMAKE_INSTALL_PREFIX}")

add_subdirectory(include)
add_subdirectory(source)

add_subdirectory(new_test)