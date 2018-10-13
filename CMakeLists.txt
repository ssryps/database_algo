cmake_minimum_required(VERSION 2.8)
project(rdma_measure)

set(CMAKE_CXX_FLAGS  "-w -std=c++11 -D_GNU_SOURCE")
set (CMAKE_WARN_DEPRECATED 0)

add_executable(rdma_measure
        OCC/Occ.h OCC/Occ.cpp
        Twopl/Twopl.h Twopl/Twopl.cpp
    #    MVCC/Mvcc.cpp MVCC/Mvcc.h
        Timestamp/Timestamp.cpp Timestamp/Timestamp.h
        pthread_local_test.cpp
        main.cpp
        rdma_test/rdma_twopl.cpp rdma_test/rdma_utils.h)

TARGET_LINK_LIBRARIES(rdma_measure ibverbs rdmacm pthread)
