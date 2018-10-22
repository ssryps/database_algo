//
// Created by mason on 9/30/18.
//

#ifndef INC_2PL_TWOPL_H
#define INC_2PL_TWOPL_H


#include <map>
#include <mutex>
#include <set>
#include <vector>
#include "index_hashtable.hpp"
#include "utils.h"


static int TWOPL_TABLE_NUM = 10;
struct TwoplEntry{
    idx_key_t   key;
    idx_value_t value;
};

class TwoplServer : Server{

public:
    TwoplServer(int id):id(id){}
    TransactionResult handle(Transaction transaction);
private:
    int id;

    bool insert(idx_key_t key, TwoplEntry value);
    bool get(idx_key_t key, TwoplEntry* entry);
    bool lock(idx_key_t key);
    bool unlock(idx_key_t key);
    bool rdma_insert(int mach_id, idx_key_t key, TwoplEntry entry);
    bool rdma_get(int mach_id, idx_key_t key, TwoplEntry* entry);
    bool rdma_lock(int mach_id, idx_key_t key);
    bool rdma_unlock(int mach_id, idx_key_t key);

    HashTableIndex<TwoplEntry*> database;
    HashTableIndex<std::mutex*> mu;
    //    TwoplDatabase database;
};




#endif //INC_2PL_TWOPL_H
