cmake_minimum_required(VERSION 2.8)
project(rdma_measure)

set(CMAKE_CXX_FLAGS  "-w -std=c++11 -D_GNU_SOURCE")
set (CMAKE_WARN_DEPRECATED 0)

set(CMAKE_RUNTIME_OUTPUT_DIRECTORY bin/)

set (CMAKE_CXX_STANDARD 11)

enable_testing()
find_package(GTest REQUIRED)

include_directories(
		src/
		src/test/test_framework.cpp
		src/Storage/
		src/CC_Algorithm/Occ/
		src/CC_Algorithm/Twopl/
		src/CC_Algorithm/Timestamp/
#		lib/Network/
	)


set( addition_lib
	ibverbs rdmacm memcached pthread )

add_executable(main
		src/main.cpp
#		src/test/test_utils.hpp
		src/CC_Algorithm/CCServer.hpp
		src/CC_Algorithm/Twopl/Twopl.cpp
		src/CC_Algorithm/Timestamp/Timestamp.cpp
		src/CC_Algorithm/Occ/Occ.cpp
		src/utils.cpp)
target_link_libraries(main ${addition_lib})


add_executable(ALGO_TEST
		src/test/test_main.cpp
		src/test/test_utils.hpp
		src/CC_Algorithm/CCServer.hpp
		src/CC_Algorithm/Twopl/Twopl.cpp
		src/CC_Algorithm/Timestamp/Timestamp.cpp
		src/CC_Algorithm/Occ/Occ.cpp
		src/utils.cpp)
target_link_libraries(ALGO_TEST GTest::GTest GTest::Main)


add_compile_definitions(DEBUG)

add_compile_definitions(TWO_SIDE)
