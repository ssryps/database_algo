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
    return fetch_and_add(0, FEACH_AND_ADD_TIMESTAMP, 0, value);
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

    std::vector<TimestampEntry> tempEntries;

    std::map<std::string, TimestampEntry> tempMap;

    bool abort = false;
    idx_value_t *temp_result = new idx_value_t[transaction->commands.size()];
    for(Command command : transaction->commands) {
        switch (command.operation) {
            case ALGO_WRITE: {
                read(command.key, comman)

                // check if conflict happens
                if (tempMap[command.key].lastWrite > timeStamp) {
                    abort = true;
                    break;
                } else {
                    // update local map, store new entries to a temp host
                    result.results.push_back(tempMap[command.key].value);
                    int readTime = std::max(tempMap[command.key].lastRead, timeStamp),
                            writeTime = tempMap[command.key].lastWrite;
                    TimestampEntry entry1 = TimestampEntry{command.key, tempMap[command.key].value, readTime,
                                                           writeTime};
                    tempMap[command.key] = entry1;
                    tempEntries.push_back(entry1);
                }

                break;
            }

            case ALGO_READ: {
                if (tempMap.count(command.key) == 0) {
                    TimestampEntry *entry = this->database.get(command.key);
                    if (entry != NULL)tempMap[command.key] = *entry;
                    else tempMap[command.key] = TimestampEntry{command.key, "NULL", -1, -1};
                }

                // check if conflict happens
                if (tempMap[command.key].lastRead > timeStamp || tempMap[command.key].lastWrite > timeStamp) {
                    // if so, abort this transaction
                    abort = true;
                    break;
                } else {
                    // update local map, store new entries to a temp host
                    result.results.push_back(command.value);
                    int readTime = tempMap[command.key].lastRead,
                            writeTime = timeStamp;
                    TimestampEntry entry1 = TimestampEntry{command.key, command.value, readTime, writeTime};
                    tempMap[command.key] = entry1;
                    tempEntries.push_back(entry1);
                }

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
    if(abort){ std::cout << "*********************************************\n";
        return handle(transaction);
    }

    for(TimestampEntry entry: tempEntries) {
        this->database.set(entry.key, entry);
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


