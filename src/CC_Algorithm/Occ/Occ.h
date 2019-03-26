//
// Created by mason on 9/30/18.
//

#ifndef RDMA_MEASURE_OCC_H
#define RDMA_MEASURE_OCC_H


#define OCC_COMPARE_AND_SWAP_DATA_LOCK 0
#define OCC_COMPARE_AND_SWAP_TXN_LOCK 1
#define OCC_COMPARE_AND_SWAP_TXN_UNLOCK 2

#define OCC_COMPARE_AND_SWAP_SERVER_LOCK 3
#define OCC_COMPARE_AND_SWAP_SERVER_UNLOCK 4


#define OCC_FEACH_AND_ADD_TIMESTAMP 0
#define OCC_FEACH_AND_ADD_SERVER_TXN_IDX 1

#define OCC_READ_SERVER_TXN_IDX 0
#define OCC_READ_SERVER_TXN_BUF 1
#define OCC_READ_DATA_BUF 2
#define OCC_READ_TXN_BUF 3
#define OCC_READ_KEY_IDX 4
#define OCC_READ_KEY_BUF 5


#define OCC_WRITE_DATA_BUF 0
#define OCC_WRITE_TXN_BUF 1
#define OCC_WRITE_TXN_ENDTIME 2

#define OCC_WRITE_KEY_IDX 3
#define OCC_WRITE_KEY_BUF 4
#define OCC_WRITE_SERVER_TXN 5

#define OCC_SEND_TIMESTAMP 0
#define OCC_SEND_VALI_TIMESTAMP 1
#define OCC_SEND_CHECK_TRANS 2


#define OCC_READ_PHASE 0
#define OCC_VAL_PHASE 1

#define OCC_TXN_RUNNING 0
#define OCC_TXN_SUCCESS 1
#define OCC_TXN_ABORT 2

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

#define OCC_DATA_BUF(id, buf, sz) buf



struct OccTxnEntry {
    occ_time_t end_time;
    int write_begin_ptr;
    int write_end_ptr;
    int abort;
};

#define OCC_TXN_LOCK(id, buf, sz) ({   \
    char* __buf;                     \
    if(id == 0) {                  \
        __buf = buf + sz / 4;        \
    } else {                       \
        __buf = buf + sz / 3;        \
    }                              \
    __buf;                           \
})

#define OCC_TXN_BUF(id, buf, sz) ({   \
    char* __buf;                     \
    if(id == 0) {                  \
        __buf = buf + sz / 4 + sizeof(int);        \
    } else {                       \
        __buf = buf + sz / 3 + sizeof(int);        \
    }                              \
    __buf;                           \
})


struct OccKeyEntry {
    idx_key_t key;
};

#define OCC_KEY_TOTAL_IDX ((OCC_SEGMENT_SIZE - sizeof(occ_idx_num_t)) / sizeof(OccKeyEntry))

#define OCC_KEY_IDX(id, buf, sz) ({   \
    char* _buf;                     \
    if(id == 0) {                  \
        _buf = buf + sz * 2 / 4;        \
    } else {                       \
        _buf = buf + sz * 2/ 3;        \
    }                              \
    _buf;                           \
})


#define OCC_KEY_BUF(id, buf, sz) ({   \
    char* _buf;                     \
    if(id == 0) {                  \
        _buf = buf + sz * 2 / 4 + sizeof(occ_idx_num_t);        \
    } else {                       \
        _buf = buf + sz * 2 / 3 + sizeof(occ_idx_num_t);        \
    }                              \
    _buf;                           \
})


struct OccServerTxnEntry {
    idx_value_t ts;
    int mach_id;

    occ_time_t start_time;
    occ_time_t end_time;

    int abort;
};


const int OCC_SERVER_LOCK_OFFSET      = 0;
const int OCC_SERVER_TIMESTAMP_OFFSET = OCC_SERVER_LOCK_OFFSET      + sizeof(int);
const int OCC_SERVER_TXN_IDX_OFFSET   = OCC_SERVER_TIMESTAMP_OFFSET + sizeof(occ_time_t);
const int OCC_SERVER_TXN_BUF_OFFSET   = OCC_SERVER_TXN_IDX_OFFSET   + sizeof(occ_idx_num_t);


#define OCC_LOCK_OFF 0
#define OCC_LOCK_ON  1



#define OCC_SERVER_LOCK(id, buf, sz) ({   \
    assert(id == 0);                 \
    char* __buf = buf + sz * 3 / 4 + OCC_SERVER_LOCK_OFFSET;        \
    __buf;                           \
})

#define OCC_SERVER_TIMESTAMP(id, buf, sz) ({   \
    assert(id == 0);                 \
    char* __buf = buf + sz * 3 / 4 + OCC_SERVER_TIMESTAMP_OFFSET ;   \
    __buf;                           \
})


#define OCC_SERVER_TXN_IDX(id, buf, sz) ({   \
    assert(id == 0);                 \
    char* __buf = buf + sz * 3 / 4 + OCC_SERVER_TXN_IDX_OFFSET;   \
    __buf;                           \
})



#define OCC_SERVER_TXN_BUF(id, buf, sz) ({   \
    assert(id == 0);                 \
    char* __buf = buf + sz * 3 / 4 + OCC_SERVER_TXN_BUF_OFFSET; \
    __buf;                           \
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

public:
    static int listen_socket(int my_id, Socket_Type type, comm_identifer *ident);

    static comm_identifer start_socket(int mach_id, Socket_Type type);

    static comm_identifer accept_socket(comm_identifer socket, comm_addr* addr, comm_length* length);

    static int close_socket(comm_identifer ident);

    static uint16_t get_socket(int id, Socket_Type type );

private:
    bool get_entry(idx_key_t key, OccDataEntry *value, comm_identifer ident);

    bool write_entry(idx_key_t key, idx_value_t value, comm_identifer ident);

    bool get_timestamp(occ_time_t *value);

    bool get_validation_timestamp(occ_time_t *value, int m_id, occ_time_t m_start_t);


    bool store_transaction_info(occ_time_t read_time, std::unordered_set<idx_key_t>* read_set,
            std::unordered_set<idx_key_t>* write_set);

    bool get_check_trans(occ_time_t read_time, occ_time_t validation_time, std::vector<int>* mach_set_peer,
            std::vector<occ_time_t >* start_time_set_peer);

    bool get_trans_info(int peer, occ_time_t start_time_peer, int* abort, occ_time_t *end_time_peer,
            std::unordered_set<idx_key_t >* write_set_peer);

    bool update_transaction_info(occ_time_t read_time, occ_time_t end_time, int abort);

    bool msg_loop();

    bool setup_meta_server();

    static void* server_thread(void* buf);

};

struct ServerInitInfo {
    int server_id;
    char* server_buf;
    int buf_sz;
    OccServer* this_ptr;
};




static inline bool common_elements(std::unordered_set<idx_key_t> set1, std::unordered_set<idx_key_t> set2){
    auto it = std::find_first_of(set1.begin(), set1.end(), set2.begin(), set2.end());
    return it != set1.end();
}



#endif //RDMA_MEASURE_OCC_H
