//
// Created by mason on 9/30/18.
//
#include <chrono>
#include "Occ.h"
#include <sys/time.h>
#include <hash_map>
#include <assert.h>
#include <set>
#include <unordered_map>


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


bool OccServer::rdma_read(int mach_id, int type, idx_key_t key, char* value, int* sz){
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
        case OCC_READ_SERVER_TXN_IDX: {
            occ_idx_num_t *num = (occ_idx_num_t *)OCC_SERVER_TXN_IDX(this->global_buf, this->buf_sz);

            std::memcpy(value, num, sizeof(occ_idx_num_t));
            (*sz) = sizeof(occ_idx_num_t);
            break;
        }
        case OCC_READ_SERVER_TXN_BUF: {
            OccServerTxnEntry* server_txn_buf = (OccServerTxnEntry *)OCC_SERVER_TXN_BUF(this->global_buf, this->buf_sz);
            std::memcpy(value, &(server_txn_buf[key]), sizeof(OccServerTxnEntry));

            (*sz) = sizeof(OccServerTxnEntry);
            break;
        }
        case OCC_READ_DATA_BUF: {
            OccDataEntry* data_buf = (OccDataEntry*)OCC_DATA_BUF(mach_id, this->global_buf, this->buf_sz);
            std::memcpy(value, &(data_buf[key % MAX_DATA_PER_MACH]), sizeof(OccDataEntry));

            (*sz) = sizeof(OccDataEntry);
            break;
        }

        case OCC_READ_TXN_BUF: {
            OccTxnEntry* server_txn_buf = (OccTxnEntry *)OCC_TXN_BUF(mach_id, this->global_buf, this->buf_sz);
            std::memcpy(value, &(server_txn_buf[key]), sizeof(OccTxnEntry));

            (*sz) = sizeof(OccTxnEntry);
            break;
        }

        case OCC_READ_KEY_IDX: {
            occ_idx_num_t *num = (occ_idx_num_t *)OCC_KEY_IDX(mach_id, this->global_buf, this->buf_sz);

            std::memcpy(value, num, sizeof(occ_idx_num_t));
            (*sz) = sizeof(occ_idx_num_t);
            break;
        }

        case OCC_READ_KEY_BUF:{
            auto key_buf = (OccKeyEntry*)OCC_KEY_BUF(mach_id, this->global_buf, this->buf_sz);
            std::memcpy(&key_buf[key], value, sizeof(OccKeyEntry));
            (*sz) = sizeof(OccKeyEntry);
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
            assert(false);
            break;
        }
    }
    return true;
}


bool OccServer::pthread_write(int mach_id, int type, idx_key_t key, char* value, int sz) {
    switch (type) {
        case OCC_WRITE_DATA_BUF: {
            idx_value_t *v = (idx_value_t *) value;
            OccDataEntry* data_buf = (OccDataEntry*)OCC_DATA_BUF(mach_id, this->global_buf, this->buf_sz);
            data_buf[key % MAX_DATA_PER_MACH].value = *v;
            break;
        }

        case OCC_WRITE_TXN_BUF: {
            auto txn_buf = (OccTxnEntry*)OCC_TXN_BUF(mach_id, this->global_buf, this->buf_sz);
            memcpy(&txn_buf[key], value, sz);
            break;
        }

        case OCC_WRITE_TXN_ENDTIME: {
            occ_time_t *end_time = (occ_time_t*) value;
            int *abort = (int *) (value + sizeof(occ_time_t));
            auto txn_buf = (OccTxnEntry*)OCC_TXN_BUF(mach_id, this->global_buf, this->buf_sz);
            txn_buf[key].end_time = *end_time;
            txn_buf[key].abort = *abort;
            break;
        }

        case OCC_WRITE_KEY_IDX: {
            auto key_idx = (int*)OCC_KEY_IDX(mach_id, this->global_buf, this->buf_sz);
            memcpy(key_idx, value, sz);
            break;
        }

        case OCC_WRITE_KEY_BUF: {
            auto key_buf = (OccKeyEntry*)OCC_KEY_BUF(mach_id, this->global_buf, this->buf_sz);
            memcpy(&key_buf[key], value, sz);
            break;
        }

        case OCC_WRITE_SERVER_TXN: {
            auto txn_buf = (OccServerTxnEntry*)OCC_SERVER_TXN_BUF(this->global_buf, this->buf_sz);
            memcpy(&txn_buf[key], value, sz);

            break;
        }
#ifdef TWO_SIDE

#else
//        case TS_WRITE_LOCK: {
//            des_buf->entries[key % MAX_DATA_PER_MACH].lock = value;
//            break;
//        }
#endif
        default:{
            assert(false);
            break;
        }

    }
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


bool OccServer::pthread_compare_and_swap(int mach_id, int type, idx_key_t key, idx_value_t old_value,
        idx_value_t new_value) {
    switch (type) {

#ifdef TWO_SIDE

#else


        case OCC_COMPARE_AND_SWAP_DATA_LOCK: {
             auto data_buf = (OccDataEntry*)OCC_DATA_BUF(mach_id, this->global_buf, this->buf_sz);
             idx_value_t *lock_pos = &(data_buf[key % MAX_DATA_PER_MACH].lock);
             return __sync_bool_compare_and_swap(lock_pos, old_value, new_value);
        }

        case OCC_COMPARE_AND_SWAP_SERVER_LOCK: {
            auto lock = (int*)OCC_SERVER_LOCK(this->global_buf, this->buf_sz);
            return __sync_bool_compare_and_swap(lock, OCC_LOCK_OFF, OCC_LOCK_ON);
        }

        case OCC_COMPARE_AND_SWAP_SERVER_UNLOCK: {
            auto lock = (int*)OCC_SERVER_LOCK(this->global_buf, this->buf_sz);
            return __sync_bool_compare_and_swap(lock, OCC_LOCK_ON, OCC_LOCK_OFF);
        }

        case OCC_COMPARE_AND_SWAP_TXN_LOCK:{
            auto lock = (int*)OCC_TXN_LOCK(mach_id, this->global_buf, this->buf_sz);
            return __sync_bool_compare_and_swap(lock, OCC_LOCK_OFF, OCC_LOCK_ON);
        }

        case OCC_COMPARE_AND_SWAP_TXN_UNLOCK:{
            auto lock = (int*)OCC_TXN_LOCK(mach_id, this->global_buf, this->buf_sz);
            return __sync_bool_compare_and_swap(lock, OCC_LOCK_ON, OCC_LOCK_OFF);
        }

#endif
        default:{

            assert(false);
            break;
        }
    }
    return false;
}

bool OccServer::pthread_fetch_and_add(int mach_id, int type, idx_key_t key, idx_value_t* value) {
#ifdef TWO_SIDE

#else
    switch (type) {
        case OCC_FEACH_AND_ADD_TIMESTAMP: {
            auto timestamp = (occ_time_t *) OCC_SERVER_TIMESTAMP(this->global_buf, this->buf_sz);
            (*timestamp) ++;
            *value = *timestamp;
            break;
        }

        case OCC_FEACH_AND_ADD_SERVER_TXN_IDX: {
            auto server_txn_idx = (occ_idx_num_t *) OCC_SERVER_TXN_IDX(this->global_buf, this->buf_sz);
            (*server_txn_idx)++;
            (*value) = *server_txn_idx;
            break;
        }

        default:{
            assert(false);
            break;
        }
    }
    return true;
#endif
}


#ifdef RDMA

#else

bool OccServer::init(int id, char **data_buf, int sz) {

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
#ifdef TWO_SIDE

#else

    int sz;
    bool ok = read(get_machine_index(key), OCC_READ_DATA_BUF, key, (char*)value, &sz);
    assert(ok);
    return true;
#endif

}

bool OccServer::write_entry(idx_key_t key, idx_value_t value, comm_identifer ident) {
#ifdef TWO_SIDE

#else
    bool ok;
    ok = compare_and_swap(get_machine_index(key), OCC_COMPARE_AND_SWAP_DATA_LOCK, key, 0, 1);
    assert(ok);
    ok = write(get_machine_index(key), OCC_WRITE_DATA_BUF, key, (char*)&value, sizeof(OccDataEntry));
    assert(ok);

    ok = compare_and_swap(get_machine_index(key), OCC_COMPARE_AND_SWAP_DATA_LOCK, key, 1, 0);
    assert(ok);
    return true;
#endif
}


bool OccServer::get_timestamp(occ_time_t *value) {

#ifdef TWO_SIDE

#else

    bool ok;

    while(compare_and_swap(0, OCC_COMPARE_AND_SWAP_SERVER_LOCK, 0, 0, 1))
        ;

    idx_value_t v;
    ok = fetch_and_add(0, OCC_FEACH_AND_ADD_TIMESTAMP, 0, &v);
    assert(ok);

    (*value) = v;

    assert(compare_and_swap(0, OCC_COMPARE_AND_SWAP_SERVER_UNLOCK, 0, 1, 0));

    return true;

#endif
}

bool OccServer::get_validation_timestamp(occ_time_t *value, int m_id, occ_time_t m_start_t, occ_time_t m_vali_t) {
#ifdef TWO_SIDE

#else


//    auto lock = (int*)OCC_SERVER_LOCK(this->global_buf, this->buf_sz);
//    while(!__sync_bool_compare_and_swap(lock, OCC_LOCK_OFF, OCC_LOCK_ON))
//        ;
//
//    auto timestamp = (occ_time_t*)OCC_SERVER_TIMESTAMP(this->global_buf, this->buf_sz);
//    *value = *timestamp;
//
//    auto txn_idx = (occ_idx_num_t*)OCC_SERVER_TXN_IDX(this->global_buf, this->buf_sz);
//    auto txn_buf = (OccServerTxnEntry*)OCC_SERVER_TXN_BUF(this->global_buf, this->buf_sz);
//
//    (*txn_idx)++;
//    txn_buf[(*txn_idx)].ts = m_vali_t;
//    txn_buf[(*txn_idx)].mach_id = m_id;
//    txn_buf[(*txn_idx)].start_time = m_start_t;
//    txn_buf[(*txn_idx)].end_time = -1;
//
//
//    assert(__sync_bool_compare_and_swap(lock, OCC_LOCK_ON, OCC_LOCK_OFF));
    bool ok;

    while(compare_and_swap(0, OCC_COMPARE_AND_SWAP_SERVER_LOCK, 0, 0, 1))
        ;

    // get timestamp
    ok = fetch_and_add(0, OCC_FEACH_AND_ADD_TIMESTAMP, 0, (idx_value_t *)value);
    assert(ok);

    // increase server_txn_idx
    occ_idx_num_t server_txn_idx;
    ok = fetch_and_add(0, OCC_FEACH_AND_ADD_SERVER_TXN_IDX, 0, (idx_value_t *)&server_txn_idx);
    assert(ok);

    OccServerTxnEntry server_txn_entry;
    server_txn_entry.ts = m_vali_t;
    server_txn_entry.mach_id = m_id;
    server_txn_entry.start_time = m_start_t;
    server_txn_entry.end_time = -1;
    ok = write(0, OCC_WRITE_SERVER_TXN, server_txn_idx, (char*)&server_txn_entry, sizeof(OccServerTxnEntry));
    assert(ok);


    assert(compare_and_swap(0, OCC_COMPARE_AND_SWAP_SERVER_UNLOCK, 0, 1, 0));

    return true;


#endif
}



bool OccServer::store_transaction_info(occ_time_t read_time, std::unordered_set<idx_key_t> *read_set,
                                       std::unordered_set<idx_key_t> *write_set) {
#ifdef TWO_SIDE

#else
    while(compare_and_swap(this->server_id, OCC_COMPARE_AND_SWAP_TXN_LOCK , 0, 0, 1))
        ;

    OccTxnEntry n_txn_entry;
    n_txn_entry.end_time = -1;
    n_txn_entry.abort = OCC_TXN_RUNNING;

    int key_pos, sz;
    bool ok = read(this->server_id, OCC_READ_KEY_IDX, 0, (char*)&key_pos, &sz);
    assert(ok);
    n_txn_entry.write_begin_ptr = key_pos;

    for(auto it = write_set->begin(); it != write_set->end(); it++) {
        idx_key_t v = (*it);
        write(this->server_id, OCC_WRITE_KEY_BUF, key_pos, (char*)&v, sizeof(idx_key_t));
        key_pos ++;
        key_pos %= OCC_KEY_TOTAL_IDX;
    }

    ok = write(this->server_id, OCC_WRITE_KEY_IDX, 0, (char*)&key_pos, sizeof(int));
    assert(ok);
    n_txn_entry.write_end_ptr = key_pos;

    ok = write(this->server_id, OCC_WRITE_TXN_BUF, read_time, (char*)&n_txn_entry, sizeof(OccTxnEntry));
    assert(ok);

    assert(compare_and_swap(this->server_id, OCC_COMPARE_AND_SWAP_TXN_UNLOCK, 0, 1, 0));

#endif
}

bool OccServer::get_check_trans(occ_time_t read_time, occ_time_t validation_time,
        std::vector<int>* mach_set_peer, std::vector<occ_time_t >* start_time_set_peer){
#ifdef TWO_SIDE

#else
    while(compare_and_swap(0, OCC_COMPARE_AND_SWAP_SERVER_LOCK, 0, 0, 1))
        ;

    occ_idx_num_t txn_idx;
    int idx_sz;
    bool ok = read(0, OCC_READ_SERVER_TXN_IDX, 0, (char*)&txn_idx, &idx_sz);
    assert(ok);

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

    assert(compare_and_swap(0, OCC_COMPARE_AND_SWAP_SERVER_UNLOCK, 0, 1, 0));

    return true;
#endif

}

bool OccServer::get_trans_info(int peer, occ_time_t start_time_peer, int *abort, occ_time_t *end_time_peer,
        std::unordered_set<idx_key_t> *write_set_peer) {
#ifdef TWO_SIDE

#else
    while(compare_and_swap(peer, OCC_COMPARE_AND_SWAP_TXN_LOCK , 0, 0, 1))
        ;

    OccTxnEntry txn_entry;
    int sz;
    read(peer, OCC_READ_TXN_BUF, start_time_peer, (char *)&txn_entry, &sz);
    assert(sz == sizeof(OccTxnEntry));

    (*end_time_peer) = txn_entry.end_time;
    (*abort) = txn_entry.abort;

    int beg = txn_entry.write_begin_ptr, ed = txn_entry.write_end_ptr;
    for(int i = beg; i != ed; i ++, i %= OCC_KEY_TOTAL_IDX){
        OccKeyEntry key_entry;
        int sz;
        read(peer, OCC_READ_KEY_BUF, i, (char*)&key_entry, &sz);
        assert(sz == sizeof(OccKeyEntry));

        write_set_peer->insert(key_entry.key);
    }

    assert(compare_and_swap(peer, OCC_COMPARE_AND_SWAP_TXN_UNLOCK, 0, 1, 0));

    return true;
#endif
}


bool OccServer::update_transaction_info(occ_time_t read_time, occ_time_t end_time, int abort) {

#ifdef TWO_SIDE

#else

    int msg_sz = sizeof(occ_time_t) + sizeof(int);
    char* msg = new char[msg_sz];
    memcpy(msg, &end_time, sizeof(occ_time_t));
    memcpy(msg + sizeof(occ_time_t), &abort, sizeof(int));

    // update local info
    while(compare_and_swap(this->server_id, OCC_COMPARE_AND_SWAP_TXN_LOCK , 0, 0, 1))
        ;

    write(this->server_id, OCC_WRITE_TXN_ENDTIME, read_time, msg, msg_sz);

    assert(compare_and_swap(this->server_id, OCC_COMPARE_AND_SWAP_TXN_UNLOCK, 0, 1, 0));


    // update info on server
    while(compare_and_swap(0, OCC_COMPARE_AND_SWAP_SERVER_LOCK , 0, 0, 1))
        ;

    occ_idx_num_t txn_idx;
    int idx_sz;
    bool ok = read(0, OCC_READ_SERVER_TXN_IDX, 0, (char*)&txn_idx, &idx_sz);
    assert(ok);

    OccServerTxnEntry server_txn_entry;
    int i;
    for(i = txn_idx - 1; i >= 0; i--) {
        int txn_sz;
        read(0, OCC_READ_SERVER_TXN_BUF, i, (char*)&server_txn_entry, &txn_sz);
        assert(txn_sz == sizeof(OccServerTxnEntry));
        if(server_txn_entry.start_time == read_time) {
            break;
        }
    }
    assert(i >= 0);

    server_txn_entry.end_time = end_time;
    server_txn_entry.abort = abort;
    ok = write(0, OCC_WRITE_SERVER_TXN, i, (char*)&server_txn_entry, sizeof(OccServerTxnEntry));
    assert(ok);

    assert(compare_and_swap(0, OCC_COMPARE_AND_SWAP_SERVER_UNLOCK, 0, 1, 0));

    return true;

#endif
}


TransactionResult OccServer::handle(Transaction* transaction){
    TransactionResult result;
    if(!checkGrammar(transaction)){
        result.is_success = false;
        return result;
    }
    std::unordered_map<idx_key_t, idx_value_t> local_db;
    std::unordered_set<idx_key_t> read_set, write_set;

    idx_value_t *temp_result = new idx_value_t[transaction->commands.size()];

    // read and compute intermediate result
    occ_time_t read_time;
    bool ok = get_timestamp(&read_time);
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

    occ_time_t validation_time;
    ok = get_validation_timestamp(&validation_time, this->server_id, read_time, validation_time);
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
    if(!ok) {
        printf("can't get check transactions");
        exit(1);
    }

    bool abort = false;
    for(int i = 0; i < mach_set_peer.size(); i++){
        int mach_peer = mach_set_peer[i];

        int abort_peer;
        occ_time_t start_time_peer = start_time_set_peer[i], end_time_peer;
        std::unordered_set<idx_key_t> write_set_peer;

        get_trans_info(mach_peer, start_time_peer, &abort_peer, &end_time_peer, &write_set_peer);

        if(abort_peer == 1)
            continue;

        if(end_time_peer < validation_time){
            if(common_elements(write_set_peer, read_set)){
                abort = true;
                break;
            }
        } else {
            if(common_elements(write_set_peer, read_set) || common_elements(write_set_peer, write_set)){
                abort = true;
                break;
            }
        }
    }

    // write phase
    if(abort) {
        result.is_success = false;
        update_transaction_info(read_time, -1, OCC_TXN_ABORT);
        return result;
    }


    for(auto it = local_db.begin(); it != local_db.end(); it++){
        idx_key_t key = (*it).first;
        idx_value_t value = (*it).second;
        write_entry(key, value, 0);
    }

    occ_time_t end_time;
    get_timestamp(&end_time);
    update_transaction_info(read_time, end_time, OCC_TXN_SUCCESS);

    result.is_success = true;
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

