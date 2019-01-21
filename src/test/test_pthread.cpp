#include <iostream>
#include "test_utils.hpp"
#include <cstdlib>
#include <CC_Algorithm/Twopl/Twopl.h>
#include <CC_Algorithm/OCC/Occ.h>
#include <CC_Algorithm/Timestamp/Timestamp.h>
#include <thread>



struct ServerThreadInfo{
    int server_type; // 0 - 3 for twopl, occ, mvcc, timestamp
    int id;
    char** thread_buf;
};

struct ClientThreadInfo {
    int server_type;
    int id;
    char** server_buf;
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
            server->init(info->id, info->thread_buf, SERVER_DATA_BUF_SIZE);
            server->run();
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
                offset += sprintf(output_buf + offset, "thread %d: result: %d\n",info->id, result.isSuccess);
                for (auto i : result.results)
                    offset += sprintf(output_buf + offset, "%i\n", i);
                printf(output_buf);
            #endif

            break;
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
//        trans_msg[i] = new char[MEG_BUF_SIZE];
    }

    if(algo_name == ALGO_TWOPL) {
        for(int i = 0; i < SERVER_THREAD_NUM; i++){
            server_info[i].server_type = ALGO_TWOPL;
            server_info[i].id = i;
            server_info[i].thread_buf = global_buf;
        }
        for(int i = 0; i < CLIENT_THREAD_NUM; i++){
            client_info[i].server_type = ALGO_TWOPL;
            client_info[i].id = -i;
            client_info[i].server_buf = global_buf;
        }
    } else if(algo_name == ALGO_TIMESTAMP) {
        for(int i = 0; i < SERVER_THREAD_NUM; i++) {
            server_info[i].server_type = ALGO_TIMESTAMP;
            server_info[i].id = i;
            server_info[i].thread_buf = global_buf;
        }
        for(int i = 0; i < CLIENT_THREAD_NUM; i++){
            client_info[i].server_type = ALGO_TIMESTAMP;
            client_info[i].id = -i;
            client_info[i].server_buf = global_buf;
        }

    } else if(algo_name == ALGO_MVCC) {
        //      server = new MvccServer;
    } else if(algo_name == ALGO_OCC) {
        for(int i = 0; i < SERVER_THREAD_NUM; i++){
            server_info[i].server_type = ALGO_TWOPL;
            server_info[i].id = i;
            server_info[i].thread_buf = global_buf;
        }
        for(int i = 0; i < CLIENT_THREAD_NUM; i++){
            client_info[i].server_type = ALGO_TWOPL;
            client_info[i].id = -i;
            client_info[i].server_buf = global_buf;
        }
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
