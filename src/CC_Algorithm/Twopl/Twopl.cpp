//
// Created by mason on 9/30/18.
//

#include <assert.h>
//#include <test/test_utils.hpp>
#include <thread>
#include "Twopl.h"

extern Transaction* getTransactionFromBuffer(char* buf, size_t length);



bool TwoplServer::write(int mach_id, int type, idx_key_t key, idx_value_t entry){
#ifdef RDMA
    return rdma_write(mach_id, type, key, entry);
#else
    return pthread_write(mach_id, type, key, entry);
#endif
}


bool TwoplServer::read(int mach_id, int type, idx_key_t key, idx_value_t* entry){
#ifdef RDMA

    return rdma_read(mach_id, type, key, entry);
#else
    return pthread_read(mach_id, type, key, entry);
#endif
}


// type 1 for lock, type 2 for unlock, type 3 for put, type 4 for get
bool TwoplServer::send_i(int mach_id, int type, char *buf, int sz, comm_identifer ident) {
#ifdef RDMA
    return rdma_send(mach_id, type, key, value);
#else
    return pthread_send(mach_id, type, buf, sz);
#endif
}


// type 0 for transaction msg, 1 for lock, 2 for unlock, 3 for put, 4 for get
bool TwoplServer::recv(int* mach_id, int type, char* buf, int* sz){
#ifdef RDMA
    return rdma_recv(mach_id, type, key, value);
#else
    return pthread_recv(mach_id, type, buf, sz);
#endif
}


bool TwoplServer::compare_and_swap(int mach_id, int type, idx_key_t key, idx_value_t old_value, idx_value_t new_value){
#ifdef RDMA
    return rdma_compare_and_swap(mach_id, type, key, old_value, new_value);
#else
    return pthread_compare_and_swap(mach_id, type, key, old_value, new_value);
#endif

}



bool TwoplServer::rdma_read(int mach_id, int type, idx_key_t key, idx_value_t* value){

    return true;
}

bool TwoplServer::rdma_write(int mach_id, int type, idx_key_t key, idx_value_t value){
    return true;
}

bool TwoplServer::rdma_send(int mach_id, int type, char* buf, int sz){
    return true;
}

bool TwoplServer::rdma_recv(int* mach_id, int type, char* buf, int* sz) {
    return true;
}

bool TwoplServer::rdma_compare_and_swap(int mach_id, int type, idx_key_t key, idx_value_t old_value, idx_value_t new_value){
    return true;
}

//bool TwoplServer::rdma_fetch_and_add(int mach_id, int type, idx_key_t key) {
//    return true;
//}

bool TwoplServer::pthread_read(int mach_id, int type, idx_key_t key, idx_value_t* value) {
    TwoplDataBuf* des_buf = reinterpret_cast<TwoplDataBuf*>(this->global_buf[mach_id]);
    switch (type) {
        case TWOPL_DATA_LOCK: {
            *value = des_buf->lockBuf[key % MAX_DATA_PER_MACH];
            break;
        }
        case TWOPL_DATA_VALUE: {
            *value = des_buf->valueBuf[key % MAX_DATA_PER_MACH];
            break;
        }
    }
    return true;
}


bool TwoplServer::pthread_write(int mach_id, int type, idx_key_t key, idx_value_t value) {
    TwoplDataBuf* des_buf = reinterpret_cast<TwoplDataBuf*>(this->global_buf[mach_id]);
    switch (type) {
        case TWOPL_DATA_LOCK: {
            des_buf->lockBuf[key % MAX_DATA_PER_MACH] = value;
            break;
        }
        case TWOPL_DATA_VALUE: {
            des_buf->valueBuf[key % MAX_DATA_PER_MACH] = value;
            break;
        }
    }
    return true;
}


bool TwoplServer::pthread_send(int mach_id, int type, char *buf, int sz) {

}


bool TwoplServer::pthread_recv(int *mach_id, int type, char *buf, int *sz) {

}


bool TwoplServer::pthread_compare_and_swap(int mach_id, int type, idx_key_t key, idx_value_t old_value, idx_value_t new_value) {
    TwoplDataBuf* des_buf = reinterpret_cast<TwoplDataBuf*>(this->global_buf[mach_id]);
    switch (type) {
        case TWOPL_DATA_LOCK: {
            return __sync_bool_compare_and_swap(&(des_buf->lockBuf[key % MAX_DATA_PER_MACH]), old_value, new_value);
        }
        case TWOPL_DATA_VALUE: {
            return __sync_bool_compare_and_swap(&(des_buf->valueBuf[key % MAX_DATA_PER_MACH]), old_value, new_value);
        }
    }
    return false;
}

//bool TwoplServer::pthread_fetch_and_add(int mach_id, int type, idx_key_t key) {
//
//}

bool TwoplServer::put(idx_key_t key, TwoplEntry *entry) {
#ifdef TWO_SIDE

#else
    return write(get_machine_index(key), 1, key % MAX_DATA_PER_MACH, entry->value);
#endif
}

bool TwoplServer::get(idx_key_t key, TwoplEntry* entry) {
#ifdef TWO_SIDE

#else
    return read(get_machine_index(key), 1, key % MAX_DATA_PER_MACH, &(entry->value));
#endif
}

bool TwoplServer::lock(idx_key_t key) {
#ifdef TWO_SIDE

#else
    idx_value_t lock;
    while(true) {
        if(!read(get_machine_index(key), TWOPL_DATA_LOCK, key % MAX_DATA_PER_MACH, &lock))return false;
        if(lock == 0){
            if(compare_and_swap(get_machine_index(key), TWOPL_DATA_LOCK, key % MAX_DATA_PER_MACH, 0, 1))break;
        }
    }
    return true;
#endif
}

bool TwoplServer::unlock(idx_key_t key) {
    #ifdef TWO_SIDE

    #else
        return compare_and_swap(get_machine_index(key), TWOPL_DATA_LOCK, key, 1, 0);
    #endif
}


#ifdef RDMA

#else
bool TwoplServer::init(int id, char **buf, int sz) {
    this->server_id = id;
    this->global_buf = buf;
    this->buf_sz = sz;

    #ifdef TWO_SIDE
        assert(sz / 2 > sizeof(TwoplDataBuf));
        return true;
    #else
        assert(sz  > sizeof(TwoplDataBuf));
        return true;
    #endif
}
#endif

TransactionResult TwoplServer::handle(Transaction* transaction) {

    TransactionResult results;
    std::set<idx_key_t > keys;

    if(!checkGrammar(transaction)){
        results.isSuccess = false;
        return results;
    }

    for (Command command : transaction->commands) { keys.insert(command.key); }
    for (auto i = keys.begin(); i != keys.end(); i++){ lock(*i); }

    idx_value_t *temp_result = new idx_value_t[transaction->commands.size()];
    for (int i = 0; i < transaction->commands.size(); i++) {
        Command command = transaction->commands[i];
        TwoplEntry *entry = new TwoplEntry;
        switch (command.operation) {
            case ALGO_WRITE : {
                idx_value_t r = value_from_command(command, temp_result);
                entry->value = r;
                put(command.key, entry);
                results.results.push_back(r);
                break;
            }
            case ALGO_READ: {
                get(command.key, entry);
                temp_result[i] = entry->value;
                results.results.push_back(entry->value);
                break;
            }
            case ALGO_ADD:
            case ALGO_SUB: {
                idx_value_t r = value_from_command(command, temp_result);
                temp_result[i] = r;
                results.results.push_back(r);
                break;
            }
        }
    }

    for(auto i = keys.begin(); i != keys.end(); i++){ unlock(*i); }

    results.isSuccess = true;

    return results;
}

int TwoplServer::run() {
#ifdef TWO_SIDE
//    std::thread primitive_thread(w)
    char request_i[1024 * 8];
    while (true) {
        memset(request_i, 0, sizeof(char) * 1024 * 8);
        int mach_i, msg_sz;
        recv(&mach_i, 0, request_i, &msg_sz);
//        Transaction* transaction = getTransactionFromBuffer(request_i, msg_sz);
//        this->handle(transaction);
    }
#else
    return 0;

#endif
}