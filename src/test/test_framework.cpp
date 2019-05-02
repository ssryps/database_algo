#include <iostream>
#include "test_utils.hpp"
#include <cstdlib>
#include <CC_Algorithm/Twopl/Twopl.h>
#include <CC_Algorithm/Occ/Occ.h>
#include <CC_Algorithm/Timestamp/Timestamp.h>
#include <thread>
#include <atomic>


struct ServerThreadInfo{
    int server_type; // 0 - 3 for twopl, occ, mvcc, timestamp
    int id;
    char** thread_buf;
    std::atomic<int>* timestamp_generator;
};

struct ClientThreadInfo {
    int server_type;
    int id;
    char** server_buf;
    std::atomic<int>* timestamp_generator;
};

void* cc_server(void *args){
    ServerThreadInfo* info = (ServerThreadInfo*)args;
    switch (info->server_type) {
        case ALGO_TWOPL: {
            TwoplServer *server = new TwoplServer;
            server->init(info->id, info->thread_buf, SERVER_DATA_BUF_SIZE);
            server->run();
            break;
        }

        case ALGO_TIMESTAMP: {
            TimestampServer *server = new TimestampServer;
            server->init(info->id, info->thread_buf, SERVER_DATA_BUF_SIZE, info->timestamp_generator);
            server->run();
            break;
        }

        case ALGO_OCC: {
            OccServer* server = new OccServer;
            server->init(info->id, info->thread_buf, SERVER_DATA_BUF_SIZE);
            server->run();
            break;
        }
        case ALGO_MVCC: {

        }

        default:  {

            assert(false);
            break;
        }
    }

}

void* cc_client(void *arg){
    ClientThreadInfo* info = (ClientThreadInfo*)arg;
    TransactionResult *result;

    switch (info->server_type) {
        case ALGO_TWOPL: {
            #ifdef TWO_SIDE

            #else
                TwoplServer *server = new TwoplServer;
                server->init(info->id, info->server_buf, SERVER_DATA_BUF_SIZE);
                Transaction *transaction = benchmark_txns(info->id);
                result = server->handle(transaction);
            #endif
            break;
        }

        case ALGO_TIMESTAMP: {
            #ifdef TWO_SIDE
                TimestampServer* server = new TimestampServer;
                server->init(info->id, info->server_buf, SERVER_DATA_BUF_SIZE,
                             info->timestamp_generator);
                Transaction *txn = benchmark_txns(info->id);
                result = server->handle(txn);

            #else
                TimestampServer* server = new TimestampServer;
                server->init(info->id, info->server_buf, SERVER_DATA_BUF_SIZE,
                        info->timestamp_generator);
                Transaction *transaction = benchmark_txns(info->id);

                while (true) {
                    result = server->handle(transaction);
                    if(result.is_success)
                        break;
                }
            #endif
            break;
        }

        case ALGO_OCC: {
        #ifdef TWO_SIDE
            OccServer* server = new OccServer;
            server->init(info->id, info->server_buf, SERVER_DATA_BUF_SIZE);
            Transaction *txn = benchmark_txns(info->id);
            while(true) {
                result = server->send_transaction_to_server(txn);
                if(result->is_success){
                    break;
                }
            }

        #else
            OccServer* server = new OccServer;
            server->init(info->id, info->server_buf, SERVER_DATA_BUF_SIZE);
            Transaction *transaction = benchmark_txns(info->id);

            while (true) {
                result = server->handle(transaction);
                if(result.is_success)
                    break;
            }

        #endif
            break;
        }

        default:{
            assert(false);
        }
    }

    return result;
}


void* validdate_db_state(void* arg){
    ClientThreadInfo* info = (ClientThreadInfo*)arg;
    TransactionResult *result;

    switch (info->server_type) {
        case ALGO_TWOPL: {
#ifdef TWO_SIDE

#else
            TwoplServer *server = new TwoplServer;
            server->init(info->id, info->server_buf, SERVER_DATA_BUF_SIZE);
            Transaction *transaction = benchmark_txns(info->id);
            TransactionResult result = server->handle(transaction);
#endif

            break;
        }

        case ALGO_TIMESTAMP: {
#ifdef TWO_SIDE
            TimestampServer* server = new TimestampServer;
            server->init(info->id, info->server_buf, SERVER_DATA_BUF_SIZE,
                         info->timestamp_generator);
            Transaction *transaction = benchmark_validation_txn();
            result = server->handle(transaction);
#else
            TimestampServer* server = new TimestampServer;
                server->init(info->id, info->server_buf, SERVER_DATA_BUF_SIZE,
                        info->timestamp_generator);
                Transaction *transaction = benchmark_txns(info->id);

                TransactionResult result;
                while (true) {
                    result = server->handle(transaction);
                    if(result.is_success)
                        break;
                }
#endif
            break;
        }


        case ALGO_OCC: {
#ifdef TWO_SIDE
            OccServer* server = new OccServer;
            server->init(info->id, info->server_buf, SERVER_DATA_BUF_SIZE);
            Transaction *txn = benchmark_validation_txn();

            while(true) {
                result = server->send_transaction_to_server(txn);
                if(result->is_success){
                    break;
                }
            }
#else
            OccServer* server = new OccServer;
            server->init(info->id, info->server_buf, SERVER_DATA_BUF_SIZE);
            Transaction *transaction = benchmark_txns(info->id);

            TransactionResult result;
            while (true) {
                result = server->handle(transaction);
                if(result.is_success)
                    break;
            }
#endif
            break;
        }
        default:{
            assert(false);
        }
    }
    return (void*)result;

}


void PthreadTest(CC_ALGO algo_name, int benchmark = SELF_MADE_BENCHMARK){

    ServerThreadInfo *server_info = new ServerThreadInfo[server_thread_num];
    ClientThreadInfo *client_info = new ClientThreadInfo[client_thread_num];

    // each server's data buffer
    char** global_buf = new char*[server_thread_num];
    for(int i = 0; i < server_thread_num; i++){
        global_buf[i] = new char[SERVER_DATA_BUF_SIZE];
        memset(global_buf[i], 0, sizeof(char) * SERVER_DATA_BUF_SIZE);
    }

    std::atomic<int>* timestamp_generator = new std::atomic<int>(0);

    assert(
            (algo_name == ALGO_TWOPL) || (algo_name == ALGO_OCC)||
            (algo_name == ALGO_TIMESTAMP) || (algo_name == ALGO_MVCC)
    );

    benchmark_init(benchmark);

    for(int i = 0; i < server_thread_num; i++){
        server_info[i].server_type = algo_name;
        server_info[i].id = i;
        server_info[i].thread_buf = global_buf;

        server_info[i].timestamp_generator = timestamp_generator;
    }

    for(int i = 0; i < client_thread_num; i++){
        client_info[i].server_type = algo_name;
        client_info[i].id = ~i;
        client_info[i].server_buf = global_buf;

        client_info[i].timestamp_generator = timestamp_generator;
    }

    pthread_t* s_threads = new pthread_t[server_thread_num];
    for(int i = 0; i < server_thread_num; i++){
        pthread_create(&s_threads[i], NULL, cc_server, (void *) (&server_info[i]));
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(1000));

    pthread_t* c_threads = new pthread_t[client_thread_num];
    for(int i = 0; i < client_thread_num; i++){
        pthread_create(&c_threads[i], NULL, cc_client, (void *) (&client_info[i]));
    }

    for(int i = 0; i < client_thread_num; i++){
        pthread_join(c_threads[i], NULL);
    }

    pthread_t *vali_thread = new pthread_t;
    pthread_create(vali_thread, NULL, validdate_db_state, (void *) (&client_info[0]));

    TransactionResult* txn_rst;
    pthread_join(*vali_thread, (void**)&txn_rst);

#ifdef DEBUG
    txn_rst_print(txn_rst);
#endif
    if(! benchmark_varify_rst(txn_rst)){
        printf("Benchmark result is incorrect\n");
    } else {
        printf("Benchmark result is correct\n");
    }

}
