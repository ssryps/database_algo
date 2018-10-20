cmake_minimum_required(VERSION 2.8)
project(rdma_measure)

set(CMAKE_CXX_FLAGS  "-w -std=c++11 -D_GNU_SOURCE")
set (CMAKE_WARN_DEPRECATED 0)

set(CMAKE_RUNTIME_OUTPUT_DIRECTORY bin/)
set(LIBRARY_OUTPUT_PATH ${CMAKE_SOURCE_DIR}/lib/)

set(TEST1 rdma_lib_test)

aux_source_directory(src DIR_SRCS)

include_directories(
	${HELLO_SOURCE_DIR}/. 
	./rdma_test
	./Twopl
	./OCC
	./Timestamp
	./Storage
	./Network
	./pthread_local_test.cpp
	)

link_directories (
	${HELLO_BINARY_DIR}/.
	${CMAKE_SOURCE_DIR}/lib/
	) 

set(mylib 
	ibverbs 
	rdmacm 
	memcached
	pthread)

add_executable(rdma_measure main.cpp)

TARGET_LINK_LIBRARIES(rdma_measure ${my_lib})

add_executable(${TEST1} ./rdma_test/${TEST1}.cpp)

TARGET_LINK_LIBRARIES(${TEST1} ${my_lib})

