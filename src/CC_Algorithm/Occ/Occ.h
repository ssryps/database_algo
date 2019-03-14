//
// Created by mason on 9/30/18.
//

#ifndef RDMA_MEASURE_OCC_H
#define RDMA_MEASURE_OCC_H



#define OCC_FEACH_AND_ADD_TIMESTAMP 0

#define OCC_READ_SERVER_TXN_NUM 0
#define OCC_READ_SERVER_TXN_BUF 1


#define OCC_READ_PHASE 0
#define OCC_VAL_PHASE 1



#include <map>
#include <mutex>
#include "utils.h"
#include "../CCServer.hpp"
#include <atomic>
#include <set>

#define OCC_SEGMENT_SIZE (this->server_id == 0 ? this->buf_sz / 4 : this->buf_sz / 3)

struct OccDataEntry {
    idx_value_t value;

#ifdef TWO_SIDE
//    idx_value_t lock;
#else
    idx_value_t lock;
#endif
};

#define OCC_DATA_BUF(id, buf_list, sz) ({   \
    char* buf;                     \
    if(id == 0) {                  \
        buf = buf_list[id];        \
    } else {                       \
        buf = buf_list[id];        \
    }                              \
    buf;                           \
})



struct OccTxnEntry {
    int endtime;
    int write_begin_ptr;
    int write_end_ptr;
};

#define OCC_TXN_NUM(id, buf_list, sz) ({   \
    char* buf;                     \
    if(id == 0) {                  \
        buf = buf_list[id] + sz / 4;        \
    } else {                       \
        buf = buf_list[id] + sz / 3;        \
    }                              \
    buf;                           \
})

#define OCC_TXN_BUF(id, buf_list, sz) ({   \
    char* buf;                     \
    if(id == 0) {                  \
        buf = buf_list[id] + sz / 4 + sizeof(idx_value_t);        \
    } else {                       \
        buf = buf_list[id] + sz / 3 + sizeof(idx_value_t);        \
    }                              \
    buf;                           \
})


struct OccKeyEntry {
    idx_key_t key;
};

#define OCC_KEY_BUF(id, buf_list, sz) ({   \
    char* buf;                     \
    if(id == 0) {                  \
        buf = buf_list[id] + sz * 2 / 4 + sizeof(idx_value_t);        \
    } else {                       \
        buf = buf_list[id] + sz * 2 / 3 + sizeof(idx_value_t);        \
    }                              \
    buf;                           \
})


struct OccServerTxnEntry {
    idx_value_t ts;
    int mach_id;

    idx_value_t start_time;
    idx_value_t end_time;

};

#define OCC_SERVER_TXN_NUM(id, buf_list, sz) ({   \
    char* buf;                     \
    if(id == 0) {                  \
        buf = buf_list[id] + sz * 3 / 4;        \
    } else {                       \
        assert(false);             \
    }                              \
    buf;                           \
})



#define OCC_SERVER_TXN_BUF(id, buf_list, sz) ({   \
    char* buf;                     \
    if(id == 0) {                  \
        buf = buf_list[id] + sz * 3 / 4 + sizeof(idx_value_t);        \
    } else {                       \
        assert(false);             \
    }                              \
    buf;                           \
})



class OccServer : public CCServer{
public:
    OccServer();
    TransactionResult handle(Transaction* transaction);
    int run();

#ifdef RDMA

#else
    bool init(int id, char** data_buf, int sz);
#endif

private:


#ifdef RDMA
#else
    char** global_buf;

//    int listenfd, connfd;
#endif

    bool write             (int mach_id, int type, idx_key_t key, idx_value_t value);
    bool read              (int mach_id, int type, idx_key_t key, idx_value_t* value);
    bool send_i(int mach_id, int type, char *buf, int sz, comm_identifer ident);
    bool recv_i(int *mach_id, int *type, char **buf, int *sz, comm_identifer ident);
    bool compare_and_swap  (int mach_id, int type, idx_key_t key, idx_value_t old_value, idx_value_t new_value);
    bool fetch_and_add     (int mach_id, int type, idx_key_t key, idx_value_t* value);


    bool rdma_write             (int mach_id, int type, idx_key_t key, idx_value_t entry);
    bool rdma_read              (int mach_id, int type, idx_key_t key, idx_value_t* entry);
    bool rdma_send(int mach_id, int type, char *buf, int sz, comm_identifer ident);
    bool rdma_recv(int *mach_id, int *type, char **buf, int *sz, comm_identifer ident);
    bool rdma_compare_and_swap  (int mach_id, int type, idx_key_t key, idx_value_t old_value, idx_value_t new_value);
    bool rdma_fetch_and_add     (int mach_id, int type, idx_key_t key, idx_value_t* value);

    //  bool rdma_fetch_and_add     (int mach_id, int type, idx_key_t key);

    bool pthread_write          (int mach_id, int type, idx_key_t key, idx_value_t entry);
    bool pthread_read           (int mach_id, int type, idx_key_t key, idx_value_t* entry);
    bool pthread_send(int mach_id, int type, char *buf, int sz, comm_identifer ident);
    bool pthread_recv(int *mach_id, int *type, char **buf, int *sz, comm_identifer ident);
    bool pthread_compare_and_swap(int mach_id, int type, idx_key_t key, idx_value_t old_value, idx_value_t new_value);
    bool pthread_fetch_and_add     (int mach_id, int type, idx_key_t key, idx_value_t* value);


    bool get_entry(idx_key_t key, OccDataEntry *value, comm_identifer ident);
    bool get_timestamp(idx_value_t *value, int stage);

    bool store_transaction_info(idx_value_t read_time, std::set<idx_key_t>* read_set, std::set<idx_key_t>* write_set);

    bool get_check_trans(idx_value_t read_time, idx_value_t validation_time, std::vector<int>* mach_set_peer,
            std::vector<idx_value_t>* start_time_set_peer);
};

void* timestamp_generator_thread(void* buf);


#endif //RDMA_MEASURE_OCC_H
