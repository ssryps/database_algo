#include "Timestamp.h"//
// Created by mason on 10/12/18.
//

#include <atomic>
#include <iostream>
#include <assert.h>
#include "Timestamp.h"


bool TimestampServer::write(int mach_id, int type, idx_key_t key, idx_value_t entry){
#ifdef RDMA
    return rdma_write(mach_id, type, key, entry);
#else
    return pthread_write(mach_id, type, key, entry);
#endif
}


bool TimestampServer::read(int mach_id, int type, idx_key_t key, idx_value_t* entry){
#ifdef RDMA

    return rdma_read(mach_id, type, key, entry);
#else
    return pthread_read(mach_id, type, key, entry);
#endif
}

// type 1 for lock, type 2 for unlock, type 3 for put, type 4 for get
bool TimestampServer::send(int mach_id, int type, char* buf, int sz){
#ifdef RDMA
    return rdma_send(mach_id, type, key, value);
#else
    return pthread_send(mach_id, type, buf, sz);
#endif
}

// type 0 for transaction msg, 1 for lock, 2 for unlock, 3 for put, 4 for get
bool TimestampServer::recv(int* mach_id, int type, char* buf, int* sz){
#ifdef RDMA
    return rdma_recv(mach_id, type, key, value);
#else
    return pthread_recv(mach_id, type, buf, sz);
#endif
}


bool TimestampServer::compare_and_swap  (int mach_id, int type, idx_key_t key, idx_value_t old_value, idx_value_t new_value){
#ifdef RDMA
    return rdma_compare_and_swap(mach_id, type, key, old_value, new_value);
#else
    return pthread_compare_and_swap(mach_id, type, key, old_value, new_value);
#endif

}

bool TimestampServer::fetch_and_add(int mach_id, int type, idx_key_t key, idx_value_t* value) {
#ifdef RDMA
    return rdma_fetch_and_add(mach_id, type, key, value);
#else
    return pthread_fetch_and_add(mach_id, type, key, value);
#endif

}


bool TimestampServer::rdma_read(int mach_id, int type, idx_key_t key, idx_value_t* value){
    //*value =
    return true;
}

bool TimestampServer::rdma_write(int mach_id, int type, idx_key_t key, idx_value_t value){
    return true;
}

bool TimestampServer::rdma_send(int mach_id, int type, idx_key_t key, idx_value_t value) {
    return true;
}

bool TimestampServer::rdma_recv(int mach_id, int type, idx_key_t key, idx_value_t *value) {
    return true;
}

bool TimestampServer::rdma_compare_and_swap(int mach_id, int type, idx_key_t key, idx_value_t old_value, idx_value_t new_value){
    return true;
}

bool TimestampServer::rdma_fetch_and_add(int mach_id, int type, idx_key_t key, idx_value_t* value) {
    return true;
}

bool TimestampServer::pthread_read(int mach_id, int type, idx_key_t key, idx_value_t* value) {
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


bool TimestampServer::pthread_write(int mach_id, int type, idx_key_t key, idx_value_t value) {
    TwoplDataBuf* des_buf = reinterpret_cast<TwoplDataBuf*>(this->global_buf[mach_id]);
    switch (type) {
//        case TWOPL_DATA_LOCK: {
//            des_buf->lockBuf[key % MAX_DATA_PER_MACH] = value;
//            break;
//        }
//        case TWOPL_DATA_VALUE: {
//            des_buf->valueBuf[key % MAX_DATA_PER_MACH] = value;
//            break;
//        }
    }
    return true;
}


bool TimestampServer::pthread_send(int mach_id, int type, char *buf, int sz) {

}


bool TimestampServer::pthread_recv(int *mach_id, int type, char *buf, int *sz) {

}


bool TimestampServer::pthread_compare_and_swap(int mach_id, int type, idx_key_t key, idx_value_t old_value, idx_value_t new_value) {
    TimestampEntry* des_buf = reinterpret_cast<TimestampEntry*>(global_buf[mach_id] + TABLE_OFFSET);
    switch (type) {
        case TS_COMPARE_AND_SWAP_VALUE: {
            TimestampEntry* pos = des_buf + (key % MAX_DATA_PER_MACH);
            return __sync_bool_compare_and_swap( ((char*)pos), old_value, new_value);
        }
        case TS_COMPARE_AND_SWAP_LAST_READ: {
            TimestampEntry* pos = des_buf + (key % MAX_DATA_PER_MACH);
            return __sync_bool_compare_and_swap(((char*)pos) + sizeof(idx_value_t), old_value, new_value);
        }
        case TS_COMPARE_AND_SWAP_LAST_WRITE: {
            TimestampEntry* pos = des_buf + (key % MAX_DATA_PER_MACH);
            return __sync_bool_compare_and_swap(((char*)pos) + 2 * sizeof(idx_value_t), old_value, new_value);
        }
        case TS_COMPARE_AND_SWAP_LAST_LOCK: {
            TimestampEntry* pos = des_buf + (key % MAX_DATA_PER_MACH);
            return __sync_bool_compare_and_swap(((char*)pos) + 3 * sizeof(idx_value_t), old_value, new_value);
        }
    }
    return false;
}

bool TimestampServer::pthread_fetch_and_add(int mach_id, int type, idx_key_t key, idx_value_t* value) {
    switch (type) {
        case TS_FEACH_AND_ADD_TIMESTAMP: {
            std::atomic<int> *cur = (std::atomic<int> *) (global_buf[mach_id] + ATOMIC_OFFSET);

            int oldValue, newValue;
            do {
                oldValue = cur->load(std::memory_order_relaxed);
                newValue = oldValue + 1;
            } while (!std::atomic_compare_exchange_weak(cur, &oldValue, newValue));
            (*value) = newValue;
            return true;
        }
    }
    return false;
}



#ifdef RDMA

#else
bool TimestampServer::init(int id, char **buf, int sz) {
    this->server_id = id;
    this->global_buf = buf;
    this->buf_sz = sz;
    #ifdef TWO_SIDE
        assert(sz  > sizeof(TimestampEntry) * MAX_DATA_PER_MACH);
        return true;
    #else
        assert(sz  > sizeof(TimestampEntry) * MAX_DATA_PER_MACH);
        return true;
    #endif
}
#endif

int TimestampServer::run() {


}

bool TimestampServer::get_timestamp(idx_value_t* value) {
    return fetch_and_add(0, TS_FEACH_AND_ADD_TIMESTAMP, 0, value);
}

// non-block, if it can't get the lock or read the data, return false. otherwise return true
bool TimestampServer::get_entry(idx_key_t key, TimestampEntry* value) {
#ifdef TWO_SIDE

#else
    bool ok = true;
    ok = read(get_machine_index(key), TS_READ_VALUE, key, &(value->value));
    if(!ok)return false;
    ok = read(get_machine_index(key), TS_READ_LAST_READ, key, &(value->lastRead));
    if(!ok)return false;
    ok = read(get_machine_index(key), TS_READ_LAST_WRITE, key, &(value->lastWrite));
    if(!ok)return false;
#endif
}


bool TimestampServer::change_entry(idx_key_t key, idx_value_t value, idx_value_t lastRead, idx_value_t lastWrite) {
#ifdef TWO_SIDE

#else

#endif
}

bool TimestampServer::rollback(Transaction *transaction, int lastpos, std::vector<idx_value_t> value_list,
                               std::vector<idx_value_t> write_time_list) {
#ifdef TWO_SIDE
    for(int i = 0; i < lastpos; i ++) {
        Command command = transaction->commands[i];
        switch (command.operation) {
            case ALGO_WRITE: {

                break;
            }

            case ALGO_READ: {

                break;
            }

            case ALGO_ADD:
            case ALGO_SUB: {

                break;
            }
        }
    }

#else
    for(int i = 0; i < lastpos; i ++) {
        Command command = transaction->commands[i];
        switch (command.operation) {
            case ALGO_WRITE: {

                break;
            }

            case ALGO_READ: {

                break;
            }

            case ALGO_ADD:
            case ALGO_SUB: {

                break;
            }
        }
    }
#endif
}



TransactionResult TimestampServer::handle(Transaction* transaction) {
    TransactionResult result;

    if(!checkGrammar(transaction)){
        result.isSuccess = false;
        return result;
    }

    idx_value_t cur_timestamp;
    bool ok = get_timestamp(&cur_timestamp);
    if(!ok) {
        perror("can't get timestamp");
        exit(1);
    }

    // backup for abortion
    std::vector<TimestampEntry> pre_entries;

    bool abort = false;
    idx_value_t *temp_result = new idx_value_t[transaction->commands.size()];

    std::vector<idx_value_t> rollback_value_list(transaction->commands.size());
    std::vector<idx_value_t> rollback_write_time_list(transaction->commands.size());

    int i;
    for(i = 0; i < transaction->commands.size(); i ++ ) {
        Command command = transaction->commands[i];
        switch (command.operation) {
            case ALGO_WRITE: {
                TimestampEntry old_entry;
                idx_value_t r = value_from_command(command, temp_result);

                get_entry(command.key, &old_entry);
                if (old_entry.lastWrite > cur_timestamp || old_entry.lastRead > cur_timestamp) {
                    abort = true;
                } else {
                    rollback_value_list[i] = old_entry.value;
                    rollback_write_time_list[i] = old_entry.lastWrite;
                    result.results.push_back(r);
                    change_entry(command.key, r, old_entry.lastRead, cur_timestamp);
                }

                break;
            }

            case ALGO_READ: {
                TimestampEntry old_entry;
                get_entry(command.key, &old_entry);
                if (old_entry.lastWrite > cur_timestamp) {
                    abort = true;
                } else {
                    temp_result[i] = old_entry.value;
                    result.results.push_back(old_entry.value);
                    change_entry(command.key, old_entry.value, cur_timestamp, old_entry.lastWrite);
                }
                break;
            }

            case ALGO_ADD:
            case ALGO_SUB: {
                idx_value_t r = value_from_command(command, temp_result);
                temp_result[i] = r;
                result.results.push_back(r);
                break;
            }
        }
    }

    if(abort){
        rollback(transaction, i, rollback_value_list, rollback_write_time_list);
        result.isSuccess = false;
        return result;
    }

    return result;

}



// TimestampEntryBatch's implementation is as below.
// This is just a naive one using a vector to store all information
// further improvement such as binary tree is highly possible

TimestampDatabase::TimestampDatabase(char* buf){
    this->table = reinterpret_cast<TimestampEntry*>(buf);
}


bool TimestampDatabase::get(idx_key_t key, TimestampEntry* entry){
    memcpy(entry, this->table + key, sizeof(TimestampEntry));
    return true;
}


bool TimestampDatabase::insert(idx_key_t key, TimestampEntry* entry) {
    memcpy(this->table + key, entry, sizeof(TimestampEntry));
    return true;
}


