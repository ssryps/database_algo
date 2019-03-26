#include "Timestamp.h"//
// Created by mason on 10/12/18.
//

#include <atomic>
#include <iostream>
#include <assert.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <zconf.h>
#include "Timestamp.h"

TimestampServer::TimestampServer() {}


bool TimestampServer::write(int mach_id, int type, idx_key_t key, char* value, int sz){
#ifdef RDMA
    return rdma_write(mach_id, type, key, entry);
#else
    return pthread_write(mach_id, type, key, value, sz);
#endif
}


bool TimestampServer::read(int mach_id, int type, idx_key_t key, char *value, int *sz){
#ifdef RDMA

    return rdma_read(mach_id, type, key, entry);
#else
    return pthread_read(mach_id, type, key, value, sz);
#endif
}

// type 1 for lock, type 2 for unlock, type 3 for put, type 4 for get
bool TimestampServer::send_i(int mach_id, int type, char *buf, int sz, comm_identifer ident) {
#ifdef RDMA
    return rdma_send(mach_id, type, key, value);
#else
    return pthread_send(mach_id, type, buf, sz, ident);
#endif
}

// type 0 for transaction msg, 1 for lock, 2 for unlock, 3 for put, 4 for get
bool TimestampServer::recv_i(int *mach_id, int *type, char **buf, int *sz, comm_identifer ident) {
#ifdef RDMA
    return rdma_recv(mach_id, type, key, value);
#else
    return pthread_recv(mach_id, type, buf, sz, ident);
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


bool TimestampServer::rdma_read(int mach_id, int type, idx_key_t key, char* value, int* sz){
    //*value =
    return true;
}

bool TimestampServer::rdma_write(int mach_id, int type, idx_key_t key, char* value, int sz){
    return true;
}

bool TimestampServer::rdma_send(int mach_id, int type, char *buf, int sz, comm_identifer ident) {

    return true;
}

bool TimestampServer::rdma_recv(int *mach_id, int *type, char **buf, int *sz, comm_identifer ident) {

    return true;
}

bool TimestampServer::rdma_compare_and_swap(int mach_id, int type, idx_key_t key, idx_value_t old_value, idx_value_t new_value){
    return true;
}

bool TimestampServer::rdma_fetch_and_add(int mach_id, int type, idx_key_t key, idx_value_t* value) {
    return true;
}

bool TimestampServer::pthread_read(int mach_id, int type, idx_key_t key, char* value, int* sz) {
    TimestampDataBuf* des_buf = reinterpret_cast<TimestampDataBuf*>(this->global_buf[mach_id]);
    switch (type) {
        case TS_READ_ALL_VALUE: {
            TimestampEntry* entry = (TimestampEntry*) value;
            entry->value = des_buf->entries[key % MAX_DATA_PER_MACH].value;
            entry->lastRead = des_buf->entries[key % MAX_DATA_PER_MACH].lastRead;
            entry->lastWrite = des_buf->entries[key % MAX_DATA_PER_MACH].lastWrite;
            (*sz) = sizeof(TimestampEntry);
            break;
        }

        case TS_READ_LAST_WRITE: {
            idx_value_t *v = (idx_value_t *)value;
            (*v) = des_buf->entries[key % MAX_DATA_PER_MACH].lastWrite;
            (*sz) = sizeof(idx_value_t);
            break;
        }

#ifdef TWO_SIDE

#else
        case TS_READ_LOCK: {
            idx_value_t *v = (idx_value_t*) value;
            *v = des_buf->entries[key % MAX_DATA_PER_MACH].lock;
            (*sz) = sizeof(idx_value_t);
            break;
        }

#endif
        default:{
            break;
        }
    }
    return true;
}


bool TimestampServer::pthread_write(int mach_id, int type, idx_key_t key, char* value, int sz) {
    TimestampDataBuf* des_buf = reinterpret_cast<TimestampDataBuf*>(this->global_buf[mach_id]);
    switch (type) {
        case TS_WRITE_ALL_VALUE: {
            TimestampEntry *entry = (TimestampEntry *)value;
            des_buf->entries[key % MAX_DATA_PER_MACH].value = entry->value;
            des_buf->entries[key % MAX_DATA_PER_MACH].lastRead = entry->lastRead;
            des_buf->entries[key % MAX_DATA_PER_MACH].lastWrite = entry->lastWrite;
            break;
        }


        case TS_WRITE_LAST_WRITE_AND_VALUE: {
            TimestampEntry *entry = (TimestampEntry *)value;
            des_buf->entries[key % MAX_DATA_PER_MACH].value = entry->value;
            des_buf->entries[key % MAX_DATA_PER_MACH].lastWrite = entry->lastWrite;
            break;
        }

#ifdef TWO_SIDE

#else
        case TS_WRITE_LOCK: {
            idx_value_t *v = (idx_value_t*) value;
            des_buf->entries[key % MAX_DATA_PER_MACH].lock = (*v);
            break;
        }
#endif
        default:{
            break;
        }

    }
    return true;
}


bool TimestampServer::pthread_send(int mach_id, int type, char *buf, int sz, comm_identifer ident) {

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


bool TimestampServer::pthread_recv(int *mach_id, int *type, char **buf, int *sz, comm_identifer ident) {

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


bool TimestampServer::pthread_compare_and_swap(int mach_id, int type, idx_key_t key, idx_value_t old_value, idx_value_t new_value) {
    TimestampDataBuf* des_buf = reinterpret_cast<TimestampDataBuf*>(this->global_buf[mach_id]);
    switch (type) {
        case TS_COMPARE_AND_SWAP_VALUE:  {
            idx_value_t *value_pos = &(des_buf->entries[key % MAX_DATA_PER_MACH].value) ;
            return __sync_bool_compare_and_swap( value_pos, old_value, new_value) ;
        }
        case TS_COMPARE_AND_SWAP_LAST_READ:  {
            idx_value_t *last_read_pos = &(des_buf->entries[key % MAX_DATA_PER_MACH].lastRead);
            return __sync_bool_compare_and_swap(last_read_pos, old_value, new_value);
        }
        case TS_COMPARE_AND_SWAP_LAST_WRITE:  {
            idx_value_t *last_write_pos = &(des_buf->entries[key % MAX_DATA_PER_MACH].lastWrite);
            return __sync_bool_compare_and_swap(last_write_pos, old_value, new_value);
        }

        #ifdef TWO_SIDE

        #else

        case TS_COMPARE_AND_SWAP_LOCK: {
            idx_value_t *lock_pos = &(des_buf->entries[key % MAX_DATA_PER_MACH].lock);
            return __sync_bool_compare_and_swap(lock_pos, old_value, new_value);
        }

        #endif

    }
    return false;
}

bool TimestampServer::pthread_fetch_and_add(int mach_id, int type, idx_key_t key, idx_value_t* value) {
    switch (type) {
        case TS_FEACH_AND_ADD_TIMESTAMP: {
            std::atomic<int> *cur = this->timestamp_generator;

            int oldValue, newValue;
            do {
                oldValue = cur->load(std::memory_order_relaxed);
                newValue = oldValue + 1;
            } while (!std::atomic_compare_exchange_weak(cur, &oldValue, newValue));
            (*value) = newValue;
            return true;
        }
        default:{

            return false;
        }
    }
}


int TimestampServer::listen_socket(comm_identifer *ident){
#ifdef TWO_SIDE
#ifdef RDMA
    return 0;

#else
    struct sockaddr_in servaddr;

    int listenfd;
    if((listenfd = socket(AF_INET, SOCK_STREAM, 0)) == -1){
        printf("creat socket error: %s(errno: %d)\n",strerror(errno),errno);
        return 0;
    }

    memset(&servaddr, 0, sizeof(servaddr));
    servaddr.sin_family = AF_INET;
    servaddr.sin_addr.s_addr = htonl(INADDR_ANY);
    servaddr.sin_port = htons(get_operation_socket(this->server_id));
    if(bind(listenfd, (struct sockaddr*)&servaddr, sizeof(servaddr)) == -1){
        printf("bind socket error: %s(errno: %d)\n",strerror(errno),errno);
        return 0;
    }

    if(listen(listenfd, 1000) == -1){
        printf("listen socket error: %s(errno: %d)\n",strerror(errno),errno);
        return 0;
    }
    *ident = listenfd;
    return 0;
#endif
#endif
}

comm_identifer TimestampServer::start_socket(int mach_id){
#ifdef TWO_SIDE
#ifdef RDMA

#else
    int sockfd;
    struct sockaddr_in servaddr;

    if((sockfd = socket(AF_INET, SOCK_STREAM, 0)) < 0){
        printf("creat socket error: %s(errno: %d)\n", strerror(errno),errno);
        return 0;
    }

    memset(&servaddr, 0, sizeof(servaddr));
    servaddr.sin_family = AF_INET;
    servaddr.sin_port = htons(get_operation_socket(mach_id));
    if(inet_pton(AF_INET, "127.0.0.1", &servaddr.sin_addr) <= 0){
        printf("inet_pton error for %s\n", "127.0.0.1");
        return false;
    }

    if(connect(sockfd, (struct sockaddr*)&servaddr, sizeof(servaddr)) < 0){
        printf("connect error: %s(errno: %d)\n",strerror(errno), errno);
        return false;
    }
    return sockfd;
#endif
#else
    return 0;
#endif

}


comm_identifer TimestampServer::accept_socket(comm_identifer socket, comm_addr* addr, comm_length* length){
#ifdef TWO_SIDE
    #ifdef RDMA


    #else
        return accept(socket, (struct sockaddr*)addr, length);
    #endif
#endif

}


int TimestampServer::close_socket(comm_identifer ident){
#ifdef TWO_SIDE
#ifdef RDMA

#else
    return close(ident);
#endif
#else
    return 0;
#endif
}

int TimestampServer::get_transaction_socket(int id) {
    return TXN_SCK_NUM + id * 10;
}

int TimestampServer::get_operation_socket(int id) {
    return MSG_SCK_NUM + id * 10;
}


#ifdef RDMA

#else

bool TimestampServer::init(int id, char **data_buf, int sz, std::atomic<int> *generator) {

    this->server_id = id;
    this->global_buf = data_buf;
    this->buf_sz = sz;
    this->timestamp_generator = generator;
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
#ifdef TWO_SIDE
    // ident for listening socket
    comm_identifer listening_socket;
    listen_socket(&listening_socket);

    while (true) {
        comm_addr client_addr;
        comm_length length = sizeof(client_addr);
        comm_identifer conn_socket = accept_socket(listening_socket, &client_addr, &length);
        if(conn_socket < 0) {
            std::cout << "Timestampserver::run() error: can't accept socket" << std::endl;
            exit(1);
        }

        int mach_i, type_i, msg_sz;
        char* request_i;


        while(true) {
            bool end_loop = false;

            bool recv_msg = recv_i(&mach_i, &type_i, &request_i, &msg_sz, conn_socket);
            if (!recv_msg) {
                break;
            }


//        std::cout << "listen socket recv msg :" << mach_i << " " << type_i << " " << msg_sz
//        << " " << *(reinterpret_cast<idx_key_t *>(request_i))<< std::endl;


            TimestampDataBuf *des_buf = reinterpret_cast<TimestampDataBuf *>(this->global_buf[this->server_id]);
            switch (type_i) {
                case TS_RECV_READ: {

                    idx_key_t *key = reinterpret_cast<idx_key_t *>(request_i);

                    idx_value_t result[3];
                    result[0] = des_buf->entries[(*key) % MAX_DATA_PER_MACH].value;
                    result[1] = des_buf->entries[(*key) % MAX_DATA_PER_MACH].lastRead;
                    result[2] = des_buf->entries[(*key) % MAX_DATA_PER_MACH].lastWrite;

                    if (send_i(this->server_id, TS_RECV_RESPONSE, (char *) result, sizeof(idx_value_t) * 3,
                               conn_socket) < 0) {
                        printf("Timestamp run() reply error : %s\n", strerror(errno));
                        return false;
                    }
                    break;
                }

                case TS_RECV_WRITE: {
                    idx_key_t *key = reinterpret_cast<idx_key_t *>(request_i);

                    idx_value_t result[3];
                    memcpy(result, request_i + sizeof(idx_key_t), sizeof(idx_value_t) * 3);
                    des_buf->entries[(*key) % MAX_DATA_PER_MACH].value = result[0];
                    des_buf->entries[(*key) % MAX_DATA_PER_MACH].lastRead = result[1];
                    des_buf->entries[(*key) % MAX_DATA_PER_MACH].lastWrite = result[2];
//                std::cout << "write " << (*key) << " "  << result[0] << " " << result[1] << " " << result[2] << std::endl;
                    break;
                }

                case TS_RECV_COMPARE_AND_SWAP: {

                    break;
                }

                case TS_RECV_FETCH_AND_ADD: {

                    break;
                }

                case TS_RECV_CLOSE: {
                    end_loop = true;
                    break;
                }

                default: {

                    break;
                }

            }
            if(end_loop)break;
        }
//        Transaction* transaction = getTransactionFromBuffer(request_i, msg_sz);
//        this->handle(transaction);
    }


#else
    return 0;

#endif

}

bool TimestampServer::get_timestamp(idx_value_t* value) {
    return fetch_and_add(0, TS_FEACH_AND_ADD_TIMESTAMP, 0, value);
}

// non-block, if it can't get the lock or read the data, return false. otherwise return true
bool TimestampServer::get_entry(idx_key_t key, TimestampEntry *value, comm_identifer ident) {
#ifdef TWO_SIDE
    char* t = new char[sizeof(idx_key_t)];
//    memset(t, 0, sizeof(char) * sizeof(idx_key_t));
    memcpy(t, &key, sizeof(idx_key_t));

//    comm_identifer ident = start_socket(get_machine_index(key));
    send_i(get_machine_index(key), TS_RECV_READ, t, sizeof(idx_key_t), ident);

    int mach_id, type_id, sz;
    char* buf;
    recv_i(&mach_id, &type_id, &buf, &sz, ident);

    idx_value_t result[3];
    memcpy(result, buf, sz);

    value->value = result[0];
    value->lastRead = result[1];
    value->lastWrite = result[2];
//    std::cout << "entry_value " << result[0] << " " << result[1] << " " << result[2] << std::endl;

#else
    bool ok;

    int sz1;

    idx_value_t lock;
    while(true) {
        if(!read(get_machine_index(key), TS_READ_LOCK, key % MAX_DATA_PER_MACH, (char*)&lock, &sz1))return false;
        if(lock == 0){
            if(compare_and_swap(get_machine_index(key), TS_COMPARE_AND_SWAP_LOCK, key % MAX_DATA_PER_MACH, 0, 1))break;
        }
    }


    int sz2;
    ok = read(get_machine_index(key), TS_READ_ALL_VALUE, key, (char*)value, &sz2);
    if(!ok){
        compare_and_swap(get_machine_index(key), TS_COMPARE_AND_SWAP_LOCK, key, 1, 0);
        return false;
    }

    compare_and_swap(get_machine_index(key), TS_COMPARE_AND_SWAP_LOCK, key, 1, 0);
    return true;


#endif
}


bool TimestampServer::write_entry(idx_key_t key, idx_value_t value, idx_value_t lastRead, idx_value_t lastWrite, comm_identifer ident) {
#ifdef TWO_SIDE
    int total_sz = sizeof(idx_key_t) + sizeof(idx_value_t) * 3;
    char* t = new char[total_sz];
    memcpy(t, &key, sizeof(idx_key_t));
    idx_value_t *value_ptr = (idx_value_t*)(t + sizeof(idx_key_t));
    value_ptr[0] = value;
    value_ptr[1] = lastRead;
    value_ptr[2] = lastWrite;

//    comm_identifer ident = start_socket(get_machine_index(key));
    send_i(get_machine_index(key), TS_RECV_WRITE, t, total_sz, ident);

//    int mach_id, type_id, sz;
//
//    idx_value_t result[3];
//    char* buf;
//    recv_i(&mach_id, &type_id, &buf, &sz, ident);
//    memcpy(result, buf, sz);

//    std::cout << "entry_value " << result[0] << " " << result[1] << " " << result[2] << std::endl;


#else
    bool ok;

    idx_value_t lock;
    while(true) {
        int sz;
        if(!read(get_machine_index(key), TS_READ_LOCK, key % MAX_DATA_PER_MACH, (char*)&lock, &sz))return false;
        if(lock == 0){
            if(compare_and_swap(get_machine_index(key), TS_COMPARE_AND_SWAP_LOCK, key % MAX_DATA_PER_MACH, 0, 1))break;
        }
    }


    TimestampEntry entry = {value, lastRead, lastWrite};

    ok = write(get_machine_index(key), TS_WRITE_ALL_VALUE, key, (char*)&entry, sizeof(idx_value_t) * 3);
    if(!ok){
        compare_and_swap(get_machine_index(key), TS_COMPARE_AND_SWAP_LOCK, key, 1, 0);
        return false;
    }


    compare_and_swap(get_machine_index(key), TS_COMPARE_AND_SWAP_LOCK, key, 1, 0);
    return true;
#endif
}

bool TimestampServer::close_connection(comm_identifer ident) {

#ifdef TWO_SIDE
    char i;
    send_i(this->server_id, TS_RECV_CLOSE, &i, sizeof(char), ident);
    close_socket(ident);
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
                idx_value_t lock;
                idx_key_t key = command.key;
                while(true) {
                    int sz;
                    if(!read(get_machine_index(key), TS_READ_LOCK, key % MAX_DATA_PER_MACH, (char*)&lock, &sz))return false;
                    if(lock == 0){
                        if(compare_and_swap(get_machine_index(key), TS_COMPARE_AND_SWAP_LOCK, key % MAX_DATA_PER_MACH, 0, 1))break;
                    }
                }
                idx_value_t  old_value;
                int sz;
                read(get_machine_index(key), TS_READ_LAST_WRITE, key % MAX_DATA_PER_MACH, (char*)&old_value, &sz);
                if(value_list[i] == old_value){
                    TimestampEntry entry;
                    entry.value = value_list[i];
                    entry.lastWrite = write_time_list[i];

                    write(get_machine_index(key), TS_WRITE_LAST_WRITE_AND_VALUE, key % MAX_DATA_PER_MACH, (char*)&entry,
                          sizeof(idx_value_t) * 2);
                }

                compare_and_swap(get_machine_index(key), TS_COMPARE_AND_SWAP_LOCK, key, 1, 0);
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
        result.is_success = false;
        return result;
    }

    idx_value_t cur_timestamp;
    bool ok = get_timestamp(&cur_timestamp);
    if(!ok) {
        printf("can't get timestamp");
        exit(1);
    } else {
        printf("transaction %i get timestamp %i\n", this->server_id, cur_timestamp);
    }

    // backup for abortion
    std::vector<TimestampEntry> pre_entries;

    bool abort = false;
    idx_value_t *temp_result = new idx_value_t[transaction->commands.size()];

    std::vector<idx_value_t> rollback_value_list(transaction->commands.size());
    std::vector<idx_value_t> rollback_wtime_list(transaction->commands.size());

    int i;
    for(i = 0; i < transaction->commands.size(); i ++ ) {
        Command command = transaction->commands[i];
        switch (command.operation) {
            case ALGO_WRITE: {
                TimestampEntry old_entry;
                idx_value_t r = value_from_command(command, temp_result);

                comm_identifer ident = start_socket(get_machine_index(command.key));
                get_entry(command.key, &old_entry, ident);

                if (old_entry.lastWrite > cur_timestamp || old_entry.lastRead > cur_timestamp) {
                    abort = true;
                } else {
                    rollback_value_list[i] = old_entry.value;
                    rollback_wtime_list[i] = old_entry.lastWrite;
                    result.results.push_back(r);
                    write_entry(command.key, r, old_entry.lastRead, cur_timestamp, ident);
                }

                close_connection(ident);

                break;
            }

            case ALGO_READ: {
                TimestampEntry old_entry;

                comm_identifer ident = start_socket(get_machine_index(command.key));
                get_entry(command.key, &old_entry, ident);

                if (old_entry.lastWrite > cur_timestamp) {
                    abort = true;
                } else {
                    temp_result[i] = old_entry.value;
                    result.results.push_back(old_entry.value);
                    write_entry(command.key, old_entry.value, cur_timestamp, old_entry.lastWrite, ident);
                }

                close_connection(ident);
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
        rollback(transaction, i, rollback_value_list, rollback_wtime_list);
        result.is_success = false;
        return result;
    }

    result.is_success = true;
    return result;

}


