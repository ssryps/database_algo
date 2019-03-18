//
// Created by mason on 9/30/18.
//
#include <chrono>
#include "Occ.h"
#include <sys/time.h>
#include <hash_map>
#include <assert.h>
#include <set>


OccServer::OccServer() {}


bool OccServer::write(int mach_id, int type, idx_key_t key, char* value, int sz){
#ifdef RDMA
    return rdma_write(mach_id, type, key, entry);
#else
    return pthread_write(mach_id, type, key, value, sz);
#endif
}


bool OccServer::read(int mach_id, int type, idx_key_t key, char *value, int *sz){
#ifdef RDMA

    return rdma_read(mach_id, type, key, entry);
#else
    return pthread_read(mach_id, type, key, value, sz);
#endif
}

// type 1 for lock, type 2 for unlock, type 3 for put, type 4 for get
bool OccServer::send_i(int mach_id, int type, char *buf, int sz, comm_identifer ident) {
#ifdef RDMA
    return rdma_send(mach_id, type, key, value);
#else
    return pthread_send(mach_id, type, buf, sz, ident);
#endif
}

// type 0 for transaction msg, 1 for lock, 2 for unlock, 3 for put, 4 for get
bool OccServer::recv_i(int *mach_id, int *type, char **buf, int *sz, comm_identifer ident) {
#ifdef RDMA
    return rdma_recv(mach_id, type, key, value);
#else
    return pthread_recv(mach_id, type, buf, sz, ident);
#endif
}


bool OccServer::compare_and_swap  (int mach_id, int type, idx_key_t key, idx_value_t old_value, idx_value_t new_value){
#ifdef RDMA
    return rdma_compare_and_swap(mach_id, type, key, old_value, new_value);
#else
    return pthread_compare_and_swap(mach_id, type, key, old_value, new_value);
#endif

}

bool OccServer::fetch_and_add(int mach_id, int type, idx_key_t key, idx_value_t* value) {
#ifdef RDMA
    return rdma_fetch_and_add(mach_id, type, key, value);
#else
    return pthread_fetch_and_add(mach_id, type, key, value);
#endif

}


bool OccServer::rdma_read(int mach_id, int type, idx_key_t key, char* value, int sz){
    //*value =
    return true;
}

bool OccServer::rdma_write(int mach_id, int type, idx_key_t key, char* value, int sz){
    return true;
}

bool OccServer::rdma_send(int mach_id, int type, char *buf, int sz, comm_identifer ident) {

    return true;
}

bool OccServer::rdma_recv(int *mach_id, int *type, char **buf, int *sz, comm_identifer ident) {

    return true;
}

bool OccServer::rdma_compare_and_swap(int mach_id, int type, idx_key_t key, idx_value_t old_value, idx_value_t new_value){
    return true;
}

bool OccServer::rdma_fetch_and_add(int mach_id, int type, idx_key_t key, idx_value_t* value) {
    return true;
}

bool OccServer::pthread_read(int mach_id, int type, idx_key_t key, char* value, int* sz) {
    switch (type) {
        case OCC_READ_SERVER_TXN_NUM: {
            occ_idx_num_t *num = (occ_idx_num_t *)OCC_SERVER_TXN_NUM(mach_id, this->global_buf, this->buf_sz);

            std::memcpy(value, num, sizeof(occ_idx_num_t));
            (*sz) = sizeof(occ_idx_num_t);
            break;
        }
        case OCC_READ_SERVER_TXN_BUF: {
            OccServerTxnEntry* server_txn_buf = (OccServerTxnEntry *)OCC_SERVER_TXN_BUF(mach_id, this->global_buf, this->buf_sz);
            std::memcpy(value, &(server_txn_buf[key]), sizeof(OccServerTxnEntry));

            (*sz) = sizeof(OccServerTxnEntry);
            break;
        }
        case OCC_READ_TXN_NUM: {
            occ_idx_num_t *num = (occ_idx_num_t *)OCC_TXN_NUM(mach_id, this->global_buf, this->buf_sz);

            std::memcpy(value, num, sizeof(occ_idx_num_t));
            (*sz) = sizeof(occ_idx_num_t);
            break;
        }
        case OCC_READ_TXN_BUF: {
            OccTxnEntry* server_txn_buf = (OccTxnEntry *)OCC_TXN_BUF(mach_id, this->global_buf, this->buf_sz);
            std::memcpy(value, &(server_txn_buf[key]), sizeof(OccTxnEntry));

            (*sz) = sizeof(OccTxnEntry);
            break;
        }

#ifdef TWO_SIDE

#else
//        case TS_READ_LOCK: {
//            *value = des_buf->entries[key % MAX_DATA_PER_MACH].lock;
//            break;
//        }

#endif
        default:{
            break;
        }
    }
    return true;
}


bool OccServer::pthread_write(int mach_id, int type, idx_key_t key, char* value, int sz) {
//    TimestampDataBuf* des_buf = reinterpret_cast<TimestampDataBuf*>(this->global_buf[mach_id]);
//    switch (type) {
//        case TS_WRITE_VALUE: {
//            des_buf->entries[key % MAX_DATA_PER_MACH].value = value;
//            break;
//        }
//        case TS_WRITE_LAST_READ: {
//            des_buf->entries[key % MAX_DATA_PER_MACH].lastRead = value;
//            break;
//        }
//        case TS_WRITE_LAST_WRITE: {
//            des_buf->entries[key % MAX_DATA_PER_MACH].lastWrite = value;
//            break;
//        }
//#ifdef TWO_SIDE
//
//#else
//        case TS_WRITE_LOCK: {
//            des_buf->entries[key % MAX_DATA_PER_MACH].lock = value;
//            break;
//        }
//#endif
//        default:{
//            break;
//        }
//
//    }
    return true;
}


bool OccServer::pthread_send(int mach_id, int type, char *buf, int sz, comm_identifer ident) {

    // put content into buffer
    char sendline[256];
    memset(sendline, 0, 256);

    sendline[0] = this->server_id;
    sendline[1] = type;
    int prefix_len = 2;
    memcpy(sendline + prefix_len, buf, sz);

    if(send(ident, sendline, sz + prefix_len, 0) < 0){
        printf("send_i msg error: %s(errno: %d)\n", strerror(errno), errno);
        return false;
    }

    return true;
}


bool OccServer::pthread_recv(int *mach_id, int *type, char **buf, int *sz, comm_identifer ident) {

    char recvline[MAXLINE];
    int n = recv(ident, recvline, MAXLINE, 0);

    if(n <= 0){
        return false;
    } else {
        int prefix_len = 2;

        (*mach_id) = recvline[0];
        (*type) = recvline[1];

        (*buf) = new char[n - prefix_len + 1];
        memcpy(*buf, recvline + 2, n - prefix_len);
        (*buf)[n - prefix_len] = 0;
        (*sz) = n - prefix_len;
        return true;
    }
}


bool OccServer::pthread_compare_and_swap(int mach_id, int type, idx_key_t key, idx_value_t old_value, idx_value_t new_value) {
//    TimestampDataBuf* des_buf = reinterpret_cast<TimestampDataBuf*>(this->global_buf[mach_id]);
//    switch (type) {
//        case TS_COMPARE_AND_SWAP_VALUE:  {
//            idx_value_t *value_pos = &(des_buf->entries[key % MAX_DATA_PER_MACH].value) ;
//            return __sync_bool_compare_and_swap( value_pos, old_value, new_value) ;
//        }
//        case TS_COMPARE_AND_SWAP_LAST_READ:  {
//            idx_value_t *last_read_pos = &(des_buf->entries[key % MAX_DATA_PER_MACH].lastRead);
//            return __sync_bool_compare_and_swap(last_read_pos, old_value, new_value);
//        }
//        case TS_COMPARE_AND_SWAP_LAST_WRITE:  {
//            idx_value_t *last_write_pos = &(des_buf->entries[key % MAX_DATA_PER_MACH].lastWrite);
//            return __sync_bool_compare_and_swap(last_write_pos, old_value, new_value);
//        }
//
//#ifdef TWO_SIDE
//
//#else
//
//        case TS_COMPARE_AND_SWAP_LOCK: {
//            idx_value_t *lock_pos = &(des_buf->entries[key % MAX_DATA_PER_MACH].lock);
//            return __sync_bool_compare_and_swap(lock_pos, old_value, new_value);
//        }
//
//#endif
//
//    }
    return false;
}

bool OccServer::pthread_fetch_and_add(int mach_id, int type, idx_key_t key, idx_value_t* value) {
//    switch (type) {
//        case TS_FEACH_AND_ADD_TIMESTAMP: {
//            std::atomic<int> *cur = this->timestamp_generator;
//
//            int oldValue, newValue;
//            do {
//                oldValue = cur->load(std::memory_order_relaxed);
//                newValue = oldValue + 1;
//            } while (!std::atomic_compare_exchange_weak(cur, &oldValue, newValue));
//            (*value) = newValue;
//            return true;
//        }
//        default:{
//
//            break;
//        }
//    }
//    return false;
}


#ifdef RDMA

#else

bool OccServer::init(int id, char **data_buf, int sz,) {

    this->server_id = id;
    this->global_buf = data_buf;
    this->buf_sz = sz;
#ifdef TWO_SIDE
    assert(sz / 4 > sizeof(OccDataEntry) * MAX_DATA_PER_MACH);
    return true;

#else
    assert(sz / 3 > sizeof(OccDataEntry) * MAX_DATA_PER_MACH);


    return true;
#endif
}
#endif


bool OccServer::get_entry(idx_key_t key, OccDataEntry *value, comm_identifer ident) {

}

bool OccServer::get_timestamp(idx_value_t *value, int stage) {
    return fetch_and_add(0, OCC_FEACH_AND_ADD_TIMESTAMP, 0, value);
}

bool OccServer::store_transaction_info(occ_time_t read_time, std::set<idx_key_t> *read_set,
                                       std::set<idx_key_t> *write_set) {
    occ_idx_num_t *txn_idx = (occ_idx_num_t *)OCC_TXN_NUM(this->server_id,
            this->global_buf, this->buf_sz);
    OccTxnEntry *txn_list = (OccTxnEntry*)OCC_TXN_BUF(this->server_id,
            this->global_buf, this->buf_sz);

    (txn_list[read_time]).write_begin_ptr = (*txn_idx);

    for(auto ele: *write_set) {
        idx_key_t *pos = (idx_key_t *)OCC_KEY_BUF(this->server_id, this->global_buf, this->buf_sz);
        pos[*txn_idx] = ele;
        (*txn_idx) ++;
        (*txn_idx) %= OCC_SEGMENT_SIZE / sizeof(OccKeyEntry);
    }

    (txn_list[read_time]).write_end_ptr = (*txn_idx);

}

bool OccServer::get_check_trans(idx_value_t read_time, idx_value_t validation_time, std::vector<int>* mach_set_peer,
        std::vector<idx_value_t>* start_time_set_peer){
#ifdef TWO_SIDE

#else
//    int*  txn_idx = (int*)OCC_SERVER_TXN_NUM(0, this->global_buf, this->buf_sz);
//    OccServerTxnEntry* txn_buf = (OccServerTxnEntry*)OCC_SERVER_TXN_BUF(0, this->global_buf, this->buf_sz);
    occ_idx_num_t txn_idx;
    int idx_sz;
    read(0, OCC_READ_SERVER_TXN_NUM, 0, (char*)&txn_idx, &idx_sz);
    assert(idx_sz == sizeof(occ_idx_num_t));


    for(int i = txn_idx - 1; i >= 0; i--) {
        OccServerTxnEntry txn;
        int txn_sz;
        read(0, OCC_READ_SERVER_TXN_BUF, i, (char*)&txn, &txn_sz);
        assert(txn_sz == sizeof(OccServerTxnEntry));
        if(txn.ts > validation_time) continue;
        if(txn.end_time == -1 || txn.end_time > read_time) {
            mach_set_peer->push_back(txn.mach_id);
            start_time_set_peer->push_back(txn.start_time);
        }
    }
#endif

}

bool OccServer::get_trans_info(int peer, int start_time_peer, int *end_time_peer, std::set<int> *write_set_peer) {
#ifdef TWO_SIDE

#else
    occ_idx_num_t txn_idx;
    int sz;
    read(peer, OCC_READ_TXN_NUM, 0, (char*)&txn_idx, &sz);
    assert(sz == sizeof(occ_idx_num_t));

    for(int i = txn_idx - 1; i >= 0; i--) {
        OccTxnEntry txn_entry;
        int sz;
        read(peer, OCC_READ_TXN_BUF, start_time_peer, (char *)&txn_entry, &sz);
        assert(sz == sizeof(OccTxnEntry));

        tx

        int beg = txn_entry.write_begin_ptr, ed = txn_entry.write_end_ptr;
        for(int j = beg; j != ed; j ++, j %= (OCC_SEGMENT_SIZE / sizeof(OccTxnEntry))){

        }
    }

#endif
}

TransactionResult OccServer::handle(Transaction* transaction){
    TransactionResult result;
    if(!checkGrammar(transaction)){
        result.isSuccess = false;
        return result;
    }
    __gnu_cxx::hash_map<idx_key_t, idx_value_t> local_db;
    std::set<idx_key_t> read_set, write_set;

    idx_value_t *temp_result = new idx_value_t[transaction->commands.size()];

    // read and compute intermediate result
    idx_value_t read_time;
    bool ok = get_timestamp(&read_time, OCC_READ_PHASE);
    if(!ok) {
        printf("can't get timestamp");
        exit(1);
    } else {
        printf("transaction %i get validation %i\n", this->server_id, read_time);
    }

    for(int i = 0; i < transaction->commands.size(); i ++ ) {
        Command command = transaction->commands[i];
        switch (command.operation) {
            case ALGO_WRITE: {
                idx_value_t value = value_from_command(command, temp_result);
                local_db[command.key] = value;

                result.results.push_back(value);
                write_set.insert(command.key);

                break;
            }

            case ALGO_READ: {
                idx_value_t value;
                if (local_db.find(command.key) != local_db.end()) {
                    value = local_db[command.key];
                } else {
                    OccDataEntry tmp_entry;

                    comm_identifer ident = start_socket(get_machine_index(command.key));
                    get_entry(command.key, &tmp_entry, ident);
                    value = tmp_entry.value;

                    local_db[command.key] = value;

                    close_socket(ident);
                }

                read_set.insert(command.key);
                temp_result[i] = value;
                result.results.push_back(value);

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

    // validation phase
    store_transaction_info(read_time, &read_set, &write_set);

    idx_value_t validation_time;
    ok = get_timestamp(&validation_time, OCC_VAL_PHASE);
    if(!ok) {
        printf("can't get timestamp");
        exit(1);
    } else {
        printf("transaction %i get validation %i\n", this->server_id, validation_time);
    }


    // get transactions which satisfy :
    //      1. end_time > this->read_time
    //      2. ts < this.ts
    // return
    //      1. mach_set_peer : the machine containing current transaction
    //      2. start_time_set_peer : start time

    std::vector<int> mach_set_peer;
    std::vector<occ_time_t> start_time_set_peer;

    ok = get_check_trans(read_time, validation_time, &mach_set_peer, &start_time_set_peer);


    bool abort = false;
    for(int i = 0; i < mach_set_peer.size(); i++){
        int mach_peer = mach_set_peer[i];
        occ_time_t start_time_peer = start_time_set_peer[i], end_time_peer;
        std::set<idx_key_t> write_set_peer;

        get_trans_info(mach_peer, start_time_peer, &end_time_peer, &write_set_peer);

        if(end_time_peer < validation_time){
            if(common_elements(write_set, read_set)){
                abort = true;
            }
        } else {
            if(common_elements(write_set, read_set) || common_elements(write_set, write_set)){
                abort = true;
            }
        }
    }

    // write phase
    result.isSuccess = true;
    return result;
}


int OccServer::run() {
#ifdef TWO_SIDE

    if(this->server_id == 0) {
        pthread_t *s_threads = new pthread_t;

        char* m_buf = this->global_buf[this->server_id];
        pthread_create(&s_threads[i], NULL, timestamp_generator_thread, (void *) ());
    }
#else
    return 0;

#endif

}

void* timestamp_generator_thread(void* buf) {

//    std::atomic<int>*
}
