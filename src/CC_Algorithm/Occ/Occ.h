//
// Created by mason on 9/30/18.
//

#ifndef RDMA_MEASURE_OCC_H
#define RDMA_MEASURE_OCC_H


#define OCC_COMPARE_AND_SWAP_DATA_LOCK 0
#define OCC_COMPARE_AND_SWAP_SERVER_LOCK 1


#define OCC_FEACH_AND_ADD_TIMESTAMP 0


#define OCC_READ_SERVER_TXN_NUM 0
#define OCC_READ_SERVER_TXN_BUF 1
#define OCC_READ_TXN_BUF 2
#define OCC_READ_DATA_BUF 3


#define OCC_WRITE_DATA_BUF 3

#define OCC_READ_PHASE 0
#define OCC_VAL_PHASE 1



#include <map>
#include <mutex>
#include "utils.h"
#include "../CCServer.hpp"
#include <atomic>
#include <set>
#include <algorithm>
#include <unordered_set>

#define OCC_SEGMENT_SIZE (this->server_id == 0 ? this->buf_sz / 4 : this->buf_sz / 3)

typedef int occ_idx_num_t ;
typedef int occ_time_t;


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
    occ_time_t endtime;
    int write_begin_ptr;
    int write_end_ptr;
};

//#define OCC_TXN_IDX(id, buf_list, sz) ({   \
//    char* buf;                     \
//    if(id == 0) {                  \
//        buf = buf_list[id] + sz / 4;        \
//    } else {                       \
//        buf = buf_list[id] + sz / 3;        \
//    }                              \
//    buf;                           \
//})

#define OCC_TXN_BUF(id, buf_list, sz) ({   \
    char* buf;                     \
    if(id == 0) {                  \
        buf = buf_list[id] + sz / 4;        \
    } else {                       \
        buf = buf_list[id] + sz / 3;        \
    }                              \
    buf;                           \
})


struct OccKeyEntry {
    idx_key_t key;
};

#define OCC_KEY_TOTAL_IDX ((OCC_SEGMENT_SIZE - sizeof(occ_idx_num_t)) / sizeof(OccKeyEntry))

#define OCC_KEY_IDX(id, buf_list, sz) ({   \
    char* buf;                     \
    if(id == 0) {                  \
        buf = buf_list[id] + sz * 2 / 4;        \
    } else {                       \
        buf = buf_list[id] + sz * 2/ 3;        \
    }                              \
    buf;                           \
})


#define OCC_KEY_BUF(id, buf_list, sz) ({   \
    char* buf;                     \
    if(id == 0) {                  \
        buf = buf_list[id] + sz * 2 / 4 + sizeof(occ_idx_num_t);        \
    } else {                       \
        buf = buf_list[id] + sz * 2 / 3 + sizeof(occ_idx_num_t);        \
    }                              \
    buf;                           \
})


struct OccServerTxnEntry {
    idx_value_t ts;
    int mach_id;

    occ_time_t start_time;
    occ_time_t end_time;

};


const int OCC_SERVER_LOCK_OFFSET      = 0;
const int OCC_SERVER_TIMESTAMP_OFFSET = OCC_SERVER_LOCK_OFFSET      + sizeof(int);
const int OCC_SERVER_TXN_IDX_OFFSET   = OCC_SERVER_TIMESTAMP_OFFSET + sizeof(occ_time_t);
const int OCC_SERVER_TXN_BUF_OFFSET   = OCC_SERVER_TXN_IDX_OFFSET   + sizeof(occ_idx_num_t);


#define OCC_LOCK_ON  0
#define OCC_LOCK_OFF 1



#define OCC_SERVER_LOCK(buf_list, sz) ({   \
    char* buf = buf_list[0] + sz * 3 / 4 + OCC_SERVER_LOCK_OFFSET;        \
    buf;                           \
})

#define OCC_SERVER_TIMESTAMP(buf_list, sz) ({   \
    char* buf = buf_list[0] + sz * 3 / 4 + OCC_SERVER_TIMESTAMP_OFFSET ;   \
    buf;                           \
})


#define OCC_SERVER_TXN_IDX(buf_list, sz) ({   \
    char* buf = buf_list[0] + sz * 3 / 4 + OCC_SERVER_TXN_IDX_OFFSET;   \
    buf;                           \
})



#define OCC_SERVER_TXN_BUF(buf_list, sz) ({   \
    char* buf = buf_list[0] + sz * 3 / 4 + OCC_SERVER_TXN_BUF_OFFSET; \
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

    bool write             (int mach_id, int type, idx_key_t key, char* value, int sz);
    bool read              (int mach_id, int type, idx_key_t key, char *value, int *sz);
    bool send_i            (int mach_id, int type, char *buf, int sz, comm_identifer ident);
    bool recv_i            (int *mach_id, int *type, char **buf, int *sz, comm_identifer ident);
    bool compare_and_swap  (int mach_id, int type, idx_key_t key, idx_value_t old_value, idx_value_t new_value);
    bool fetch_and_add     (int mach_id, int type, idx_key_t key, idx_value_t* value);


    bool rdma_write             (int mach_id, int type, idx_key_t key, char* value, int sz);
    bool rdma_read              (int mach_id, int type, idx_key_t key, char* value, int* sz);
    bool rdma_send              (int mach_id, int type, char *buf, int sz, comm_identifer ident);
    bool rdma_recv              (int *mach_id, int *type, char **buf, int *sz, comm_identifer ident);
    bool rdma_compare_and_swap  (int mach_id, int type, idx_key_t key, idx_value_t old_value, idx_value_t new_value);
    bool rdma_fetch_and_add     (int mach_id, int type, idx_key_t key, idx_value_t* value);

    //  bool rdma_fetch_and_add     (int mach_id, int type, idx_key_t key);

    bool pthread_write          (int mach_id, int type, idx_key_t key, char* value, int sz);
    bool pthread_read           (int mach_id, int type, idx_key_t key, char* value, int* sz);
    bool pthread_send           (int mach_id, int type, char *buf, int sz, comm_identifer ident);
    bool pthread_recv           (int *mach_id, int *type, char **buf, int *sz, comm_identifer ident);
    bool pthread_compare_and_swap(int mach_id, int type, idx_key_t key, idx_value_t old_value, idx_value_t new_value);
    bool pthread_fetch_and_add  (int mach_id, int type, idx_key_t key, idx_value_t* value);


    bool get_entry(idx_key_t key, OccDataEntry *value, comm_identifer ident);

    bool write_entry(idx_key_t key, OccDataEntry* value, comm_identifer ident);

    bool get_start_timestamp(occ_time_t *value);

    bool get_validation_timestamp(occ_time_t *value, int m_id, occ_time_t m_start_t, occ_time_t m_vali_t);


    bool store_transaction_info(occ_time_t read_time, std::unordered_set<idx_key_t>* read_set,
            std::unordered_set<idx_key_t>* write_set);

    bool get_check_trans(occ_time_t read_time, occ_time_t validation_time, std::vector<int>* mach_set_peer,
            std::vector<occ_time_t >* start_time_set_peer);

    bool get_trans_info(int peer, occ_time_t start_time_peer, occ_time_t *end_time_peer,
            std::unordered_set<idx_key_t >* write_set_peer);


};


void* timestamp_generator_thread(void* buf);

static inline bool common_elements(std::unordered_set<idx_key_t> set1, std::unordered_set<idx_key_t> set2){
    auto it = std::find_first_of(set1.begin(), set1.end(), set2.begin(), set2.end());

    return it != set1.end();
}



#endif //RDMA_MEASURE_OCC_H
