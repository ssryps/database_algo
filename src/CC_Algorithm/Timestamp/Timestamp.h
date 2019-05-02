//
// Created by mason on 10/12/18.
//

#ifndef RDMA_MEASURE_TIMESTAMP_H
#define RDMA_MEASURE_TIMESTAMP_H


#include <map>
#include <list>
#include <atomic>
#include "utils.h"
#include "../CCServer.hpp"

#define TIMESTAMP_TABLE_NUM  10

#define ATOMIC_OFFSET 0
#define TABLE_OFFSET sizeof(std::atomic<int>)

// operation code
#define TS_FEACH_AND_ADD_TIMESTAMP 0

#define TS_COMPARE_AND_SWAP_VALUE 0
#define TS_COMPARE_AND_SWAP_LAST_READ 1
#define TS_COMPARE_AND_SWAP_LAST_WRITE 2
#define TS_COMPARE_AND_SWAP_LOCK 3


#define TS_READ_ALL_VALUE 0
#define TS_READ_LAST_READ 1
#define TS_READ_LAST_WRITE 2


#define TS_WRITE_ALL_VALUE 0
#define TS_WRITE_LAST_WRITE_AND_VALUE 1


#define TS_RECV_TRANSACTION 0
#define TS_RECV_READ 1
#define TS_RECV_WRITE 2
#define TS_RECV_COMPARE_AND_SWAP 3
#define TS_RECV_FETCH_AND_ADD 4
#define TS_RECV_RESPONSE 5
#define TS_RECV_CLOSE 6

#ifdef TWO_SIDE

#else

#define TS_READ_LOCK 3
#define TS_WRITE_LOCK 3

#endif



struct TimestampEntry{
    idx_value_t value;
    idx_value_t lastRead;
    idx_value_t lastWrite;

#ifdef TWO_SIDE
   // idx_value_t lock;
#else
    idx_value_t lock;
#endif
};

struct TimestampDataBuf{
    TimestampEntry entries[MAX_DATA_PER_MACH];
};



class TimestampServer : public CCServer {

public:
    TimestampServer();
    TransactionResult * handle(Transaction *transaction);
    int run();

#ifdef RDMA

#else
    bool init(int id, char** data_buf, int sz, std::atomic<int>* generator);
#endif

private:


#ifdef RDMA
#else

    std::atomic<int>* timestamp_generator;
    char** global_buf;

//    int listenfd, connfd;
#endif

    bool write             (int mach_id, int type, idx_key_t key, char* value, int sz);
    bool read(int mach_id, int type, idx_key_t key, char *value, int *sz);
    bool send_i(int mach_id, int type, char *buf, int sz, comm_identifer ident);
    bool recv_i(int *mach_id, int *type, char **buf, int *sz, comm_identifer ident);
    bool compare_and_swap  (int mach_id, int type, idx_key_t key, idx_value_t old_value, idx_value_t new_value);
    bool fetch_and_add     (int mach_id, int type, idx_key_t key, idx_value_t* value);


    bool rdma_write             (int mach_id, int type, idx_key_t key, char* value, int sz);
    bool rdma_read              (int mach_id, int type, idx_key_t key, char* value, int* sz);
    bool rdma_send(int mach_id, int type, char *buf, int sz, comm_identifer ident);
    bool rdma_recv(int *mach_id, int *type, char **buf, int *sz, comm_identifer ident);
    bool rdma_compare_and_swap  (int mach_id, int type, idx_key_t key, idx_value_t old_value, idx_value_t new_value);
    bool rdma_fetch_and_add     (int mach_id, int type, idx_key_t key, idx_value_t* value);

    //  bool rdma_fetch_and_add     (int mach_id, int type, idx_key_t key);

    bool pthread_write          (int mach_id, int type, idx_key_t key, char* value, int sz);
    bool pthread_read           (int mach_id, int type, idx_key_t key, char* value, int* sz);
    bool pthread_send(int mach_id, int type, char *buf, int sz, comm_identifer ident);
    bool pthread_recv(int *mach_id, int *type, char **buf, int *sz, comm_identifer ident);
    bool pthread_compare_and_swap(int mach_id, int type, idx_key_t key, idx_value_t old_value, idx_value_t new_value);
    bool pthread_fetch_and_add     (int mach_id, int type, idx_key_t key, idx_value_t* value);


    int listen_socket(comm_identifer *ident);

    comm_identifer start_socket(int mach_id);

    comm_identifer accept_socket(comm_identifer socket, comm_addr* addr, comm_length* length);

    int close_socket(comm_identifer ident);

    int get_transaction_socket(int id);

    int get_operation_socket(int id);


    bool get_timestamp(idx_value_t* value);
    bool get_entry(idx_key_t key, TimestampEntry *value, comm_identifer ident);
    bool write_entry(idx_key_t key, idx_value_t value, idx_value_t lastRead, idx_value_t lastWrite, comm_identifer ident);
    bool close_connection(comm_identifer ident);
    bool rollback(Transaction* transaction, int i, std::vector<idx_value_t> value_list,
            std::vector<idx_value_t> write_time_list);


//    int get_transaction_socket(int id);
//    int get_operation_socket(int id);

};


#endif //RDMA_MEASURE_TIMESTAMP_H
