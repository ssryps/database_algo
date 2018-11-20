//
// Created by mason on 9/30/18.
//

#ifndef INC_2PL_TWOPL_H
#define INC_2PL_TWOPL_H

#include <cstdlib>
#include <map>
#include <mutex>
#include <set>
#include <vector>
#include <Storage/index_table.hpp>
#include <CC_Algorithm/CCServer.hpp>
#include "index_hashtable.hpp"
#include "utils.h"

#define TWOPL_DATA_LOCK 0
#define TWOPL_DATA_VALUE 1

static int TWOPL_TABLE_NUM = 10;
struct TwoplEntry{
    idx_key_t   key;
    idx_value_t value;
};


struct TwoplDataBuf {
    bool lockBuf[MAX_DATA_PER_MACH];
    idx_value_t valueBuf[MAX_DATA_PER_MACH];
};

struct  TwoplMegBuf {
    bool lock;

};

class TwoplServer : public CCServer{

public:
    TwoplServer(){}
    #ifdef RDMA

    #else
        bool init(int id, char** data_buf, int sz);
    #endif

    TransactionResult handle(Transaction* transaction);
    int run();
private:
    // server metadata
    int server_id;
    #ifdef RDMA
    #else
        char** global_buf;
    #endif
    bool put(idx_key_t key, TwoplEntry* value);
    bool get(idx_key_t key, TwoplEntry* entry);
    bool lock(idx_key_t key);
    bool unlock(idx_key_t key);

    bool write             (int mach_id, int type, idx_key_t key, idx_value_t value);
    bool read              (int mach_id, int type, idx_key_t key, idx_value_t* value);
    bool send              (int mach_id, int type, idx_key_t key, idx_value_t value);
    bool recv              (int mach_id, int type, idx_key_t key, idx_value_t* value);
    bool compare_and_swap  (int mach_id, int type, idx_key_t key, idx_value_t old_value, idx_value_t new_value);

    bool rdma_write             (int mach_id, int type, idx_key_t key, idx_value_t entry);
    bool rdma_read              (int mach_id, int type, idx_key_t key, idx_value_t* entry);
    bool rdma_send              (int mach_id, int type, idx_key_t key, idx_value_t value);
    bool rdma_recv              (int mach_id, int type, idx_key_t key, idx_value_t* value);
    bool rdma_compare_and_swap  (int mach_id, int type, idx_key_t key, idx_value_t old_value, idx_value_t new_value);
  //  bool rdma_fetch_and_add     (int mach_id, int type, idx_key_t key);

    bool pthread_write          (int mach_id, int type, idx_key_t key, idx_value_t entry);
    bool pthread_read           (int mach_id, int type, idx_key_t key, idx_value_t* entry);
    bool pthread_send              (int mach_id, int type, idx_key_t key, idx_value_t value);
    bool pthread_recv              (int mach_id, int type, idx_key_t key, idx_value_t* value);
    bool pthread_compare_and_swap(int mach_id, int type, idx_key_t key, idx_value_t old_value, idx_value_t new_value);
  //  bool pthread_fetch_and_add  (int mach_id, int type, idx_key_t key);

#ifdef RDMA_TEST

#else

#endif

};




#endif //INC_2PL_TWOPL_H
