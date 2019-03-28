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

void* PthreadServer(void* args){
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

void* PthreadClient(void* args){
    ClientThreadInfo* info = (ClientThreadInfo*)args;
    switch (info->server_type) {
        case ALGO_TWOPL: {
            #ifdef TWO_SIDE

            #else
                // now this server act as a client
                TwoplServer *server = new TwoplServer;
                server->init(info->id, info->server_buf, SERVER_DATA_BUF_SIZE);
                Transaction *transaction = generataTransaction(info->id);
                TransactionResult result = server->handle(transaction);
                int offset = 0;
                char* output_buf = new char[COMMMAND_PER_TRANSACTION * 100];
                offset += sprintf(output_buf + offset, "thread %d: result: %s\n",info->id, result.is_success? "success": "abort");
                for (auto i : result.results)
                    offset += sprintf(output_buf + offset, "%i\n", i);
                printf(output_buf);
            #endif

            break;
        }

        case ALGO_TIMESTAMP: {
            #ifdef TWO_SIDE
                TimestampServer* server = new TimestampServer;
                server->init(info->id, info->server_buf, SERVER_DATA_BUF_SIZE,
                             info->timestamp_generator);
                Transaction *transaction = generataTransaction(info->id);
                TransactionResult result = server->handle(transaction);
                int offset = 0;
                char* output_buf = new char[COMMMAND_PER_TRANSACTION * 100];
                offset += sprintf(output_buf + offset, "thread %d's result: %s\n",info->id, result.is_success? "success": "abort");
                for (auto i : result.results)
                    offset += sprintf(output_buf + offset, "%i\n", i);
                printf(output_buf);

            #else
                TimestampServer* server = new TimestampServer;
                server->init(info->id, info->server_buf, SERVER_DATA_BUF_SIZE,
                        info->timestamp_generator);
                Transaction *transaction = generataTransaction(info->id);

                TransactionResult result;
                while (true) {
                    result = server->handle(transaction);
                    if(result.is_success)
                        break;
                }

                int offset = 0;
                char* output_buf = new char[COMMMAND_PER_TRANSACTION * 100];
                offset += sprintf(output_buf + offset, "thread %d: result: %s\n",info->id, result.is_success? "success": "abort");
                for (auto i : result.results)
                    offset += sprintf(output_buf + offset, "%i\n", i);
                printf(output_buf);

            #endif
            break;
        }


        case ALGO_OCC: {
            #ifdef TWO_SIDE
                OccServer* server = new OccServer;
                server->init(info->id, info->server_buf, SERVER_DATA_BUF_SIZE);
                Transaction *transaction = generataTransaction(info->id);

                TransactionResult result;
                while(true) {
                    result = server->send_transaction_to_server(transaction);
                    if(result.is_success){
                        break;
                    }
                }

                int offset = 0;
                char* output_buf = new char[COMMMAND_PER_TRANSACTION * 100];
                offset += sprintf(output_buf + offset, "thread %d's result: %s\n",info->id, result.is_success? "success": "abort");
                for (auto i : result.results)
                    offset += sprintf(output_buf + offset, "%i\n", i);
                printf(output_buf);

            #else
                OccServer* server = new OccServer;
                server->init(info->id, info->server_buf, SERVER_DATA_BUF_SIZE);
                Transaction *transaction = generataTransaction(info->id);

                TransactionResult result;
                while (true) {
                    result = server->handle(transaction);
                    if(result.is_success)
                        break;
                }

                int offset = 0;
                char* output_buf = new char[COMMMAND_PER_TRANSACTION * 100];
                offset += sprintf(output_buf + offset, "thread %d: result: %s\n",info->id, result.is_success? "success": "abort");
                for (auto i : result.results)
                    offset += sprintf(output_buf + offset, "%i\n", i);
                printf(output_buf);

            #endif
            break;
        }


        default:{
            assert(false);
        }
    }
}

void PthreadTest(int argv, char* args[], CC_ALGO algo_name){
    // server name: twopl, mvcc, occ, timestamp
    srand(time(NULL));

    ServerThreadInfo *server_info = new ServerThreadInfo[SERVER_THREAD_NUM];
    ClientThreadInfo *client_info = new ClientThreadInfo[CLIENT_THREAD_NUM];
    // global buffer to simulate network traffic

//    // transaction message tunnel
//    char* trans_flag = new char[SERVER_THREAD_NUM];
//    char** trans_msg = new char*[SERVER_THREAD_NUM];

    // each server's data buffer
    char** global_buf = new char*[SERVER_THREAD_NUM];
    for(int i = 0; i < SERVER_THREAD_NUM; i++){
        global_buf[i] = new char[SERVER_DATA_BUF_SIZE];
        memset(global_buf[i], 0, sizeof(char) * SERVER_DATA_BUF_SIZE);
//        trans_msg[i] = new char[MEG_BUF_SIZE];
    }

    std::atomic<int>* timestamp_generator = new std::atomic<int>(0);

    if(algo_name == ALGO_TWOPL) {
        for(int i = 0; i < SERVER_THREAD_NUM; i++){
            server_info[i].server_type = ALGO_TWOPL;
            server_info[i].id = i;
            server_info[i].thread_buf = global_buf;
        }
        for(int i = 0; i < CLIENT_THREAD_NUM; i++){
            client_info[i].server_type = ALGO_TWOPL;
            client_info[i].id = ~i;
            client_info[i].server_buf = global_buf;
        }
    } else if(algo_name == ALGO_TIMESTAMP) {
        for(int i = 0; i < SERVER_THREAD_NUM; i++) {
            server_info[i].server_type = ALGO_TIMESTAMP;
            server_info[i].id = i;
            server_info[i].thread_buf = global_buf;
            server_info[i].timestamp_generator = timestamp_generator;
        }
        for(int i = 0; i < CLIENT_THREAD_NUM; i++){
            client_info[i].server_type = ALGO_TIMESTAMP;
            client_info[i].id = ~i;
            client_info[i].server_buf = global_buf;
            client_info[i].timestamp_generator = timestamp_generator;
        }

    } else if(algo_name == ALGO_OCC) {
        for(int i = 0; i < SERVER_THREAD_NUM; i++){
            server_info[i].server_type = ALGO_OCC;
            server_info[i].id = i;
            server_info[i].thread_buf = global_buf;
        }
        for(int i = 0; i < CLIENT_THREAD_NUM; i++){
            client_info[i].server_type = ALGO_OCC;
            client_info[i].id = ~i;
            client_info[i].server_buf = global_buf;
        }
    } else if(algo_name == ALGO_MVCC) {

    } else {
        std::cout << "wrong server name";
        exit(0);
    }

    pthread_t* s_threads = new pthread_t[SERVER_THREAD_NUM];
    for(int i = 0; i < SERVER_THREAD_NUM; i++){
        pthread_create(&s_threads[i], NULL, PthreadServer, (void*)(&server_info[i]));
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(1000));

    pthread_t* c_threads = new pthread_t[CLIENT_THREAD_NUM];
    for(int i = 0; i < CLIENT_THREAD_NUM; i++){
        pthread_create(&c_threads[i], NULL, PthreadClient, (void*)(&client_info[i]));
    }

    for(int i = 0; i < CLIENT_THREAD_NUM; i++){
        pthread_join(c_threads[i], NULL);
    }


}
