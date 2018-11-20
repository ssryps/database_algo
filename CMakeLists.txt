cmake_minimum_required(VERSION 2.8)
project(rdma_measure)

set(CMAKE_CXX_FLAGS  "-w -std=c++11 -D_GNU_SOURCE")
set (CMAKE_WARN_DEPRECATED 0)

set(CMAKE_RUNTIME_OUTPUT_DIRECTORY bin/)

#set(TEST1 rdma_lib_test)

aux_source_directory(src DIR_SRCS)

include_directories(
		src/
		src/test/test_pthread.cpp
		src/Storage/
		src/CC_Algorithm/OCC/
		src/CC_Algorithm/Twopl/
		src/CC_Algorithm/Timestamp/
#		lib/Network/
	)


set( addition_lib
	ibverbs rdmacm memcached pthread )

add_executable(main src/main.cpp src/test/test_utils.hpp src/CC_Algorithm/CCServer.hpp)

TARGET_LINK_LIBRARIES(main ${addition_lib})

#add_executable(${TEST1} test/${TEST1}.cpp)

#TARGET_LINK_LIBRARIES(${TEST1} ${my_lib})


add_compile_definitions(DEBUG)