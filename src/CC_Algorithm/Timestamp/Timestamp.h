//
// Created by mason on 10/12/18.
//

#ifndef RDMA_MEASURE_TIMESTAMP_H
#define RDMA_MEASURE_TIMESTAMP_H


#include <map>
#include <list>
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


#define TS_READ_VALUE 0
#define TS_READ_LAST_READ 1
#define TS_READ_LAST_WRITE 2
#define TS_READ_LOCK 3

#define TS_WRITE_VALUE 0
#define TS_WRITE_LAST_READ 1
#define TS_WRITE_LAST_WRITE 2
#define TS_WRITE_LOCK 3




struct TimestampEntry{
    idx_value_t value;
    idx_value_t lastRead;
    idx_value_t lastWrite;

#ifdef TWO_SIDE

#else
    idx_value_t lock;
#endif
};

class TimestampDatabase{
public:
    TimestampDatabase(char* buf);
    bool get(idx_key_t key, TimestampEntry* entry);
    bool insert(idx_key_t key, TimestampEntry* entry);
private:
    TimestampEntry *table;
};



class TimestampServer : public CCServer {

public:
    TimestampServer();
    TransactionResult handle(Transaction* transaction);
#ifdef RDMA

#else
    bool init(int id, char** data_buf, int sz);
#endif

    int run();
private:
    int server_id;
    int buf_sz;
#ifdef RDMA
#else

    char** global_buf;
#endif

    bool write             (int mach_id, int type, idx_key_t key, idx_value_t value);
    bool read              (int mach_id, int type, idx_key_t key, idx_value_t* value);
    bool send              (int mach_id, int type, char* buf, int sz);
    bool recv              (int* mach_id, int type, char* buf, int* sz);
    bool compare_and_swap  (int mach_id, int type, idx_key_t key, idx_value_t old_value, idx_value_t new_value);
    bool fetch_and_add     (int mach_id, int type, idx_key_t key, idx_value_t* value);


    bool rdma_write             (int mach_id, int type, idx_key_t key, idx_value_t entry);
    bool rdma_read              (int mach_id, int type, idx_key_t key, idx_value_t* entry);
    bool rdma_send              (int mach_id, int type, char* buf, int sz);
    bool rdma_recv              (int* mach_id, int type, char* buf, int* sz);
    bool rdma_compare_and_swap  (int mach_id, int type, idx_key_t key, idx_value_t old_value, idx_value_t new_value);
    bool rdma_fetch_and_add     (int mach_id, int type, idx_key_t key, idx_value_t* value);

    //  bool rdma_fetch_and_add     (int mach_id, int type, idx_key_t key);

    bool pthread_write          (int mach_id, int type, idx_key_t key, idx_value_t entry);
    bool pthread_read           (int mach_id, int type, idx_key_t key, idx_value_t* entry);
    bool pthread_send               (int mach_id, int type, char* buf, int sz);
    bool pthread_recv               (int* mach_id, int type, char* buf, int* sz);
    bool pthread_compare_and_swap(int mach_id, int type, idx_key_t key, idx_value_t old_value, idx_value_t new_value);
    bool pthread_fetch_and_add     (int mach_id, int type, idx_key_t key, idx_value_t* value);

    bool get_timestamp(idx_value_t* value);
    bool get_entry(idx_key_t key, TimestampEntry* value);
    bool change_entry(idx_key_t key, idx_value_t value, idx_value_t lastRead, idx_value_t lastWrite);
    bool rollback(Transaction* transaction, int i, std::vector<idx_value_t> value_list,
            std::vector<idx_value_t> write_time_list);

};


#endif //RDMA_MEASURE_TIMESTAMP_H
