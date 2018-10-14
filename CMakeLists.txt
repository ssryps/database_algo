cmake_minimum_required(VERSION 2.8)
project(rdma_measure)

set(CMAKE_CXX_FLAGS  "-w -std=c++11 -D_GNU_SOURCE")
set (CMAKE_WARN_DEPRECATED 0)

set(CMAKE_RUNTIME_OUTPUT_DIRECTORY bin/)
set(LIBRARY_OUTPUT_PATH ${CMAKE_SOURCE_DIR}/lib/)

include_directories(
	${HELLO_SOURCE_DIR}/. 
	./rdma_test
	./Twopl
	./OCC
	./Timestamp
	./Storage
	./pthread_local_test.cpp
	)

link_directories (
	${HELLO_BINARY_DIR}/.
	${CMAKE_SOURCE_DIR}/lib/
	) 

set(mylib 
	ibverbs 
	rdmacm 
	pthread)

add_executable(rdma_measure main.cpp)

TARGET_LINK_LIBRARIES(rdma_measure ${my_lib})
