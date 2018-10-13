cmake_minimum_required(VERSION 3.10)
project(rdma_measure)

set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -pthread")


add_executable(rdma_measure
        OCC/Occ.h OCC/Occ.cpp
        Twopl/Twopl.h Twopl/Twopl.cpp
    #    MVCC/Mvcc.cpp MVCC/Mvcc.h
        Timestamp/Timestamp.cpp Timestamp/Timestamp.h
        pthread_local_test.cpp
        main.cpp
        rdma_test/rdma_twopl.cpp rdma_test/rdma_utils.h)