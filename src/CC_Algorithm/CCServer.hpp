//
// Created by mason on 11/20/18.
//

#ifndef RDMA_MEASURE_CCSERVER_HPP
#define RDMA_MEASURE_CCSERVER_HPP

#include <utils.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <zconf.h>
#include <iostream>

class CCServer {
public:
//    CCServer(){}
    virtual TransactionResult handle(Transaction* transaction) = 0;
    virtual int run() = 0;


protected:
    // primitive

    int server_id;
    int buf_sz;



    virtual bool write  (int mach_id, int type, idx_key_t key, char* value, int sz){}
    virtual bool read(int mach_id, int type, idx_key_t key, char *value, int *sz){}
    virtual bool send_i(int mach_id, int type, char *buf, int sz, comm_identifer ident) {}
    virtual bool recv_i(int *mach_id, int *type, char **buf, int *sz, comm_identifer ident) {}
    virtual bool compare_and_swap(int mach_id, int type, idx_key_t key, idx_value_t old_value, idx_value_t new_value){}

    int listen_socket(comm_identifer *ident){
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

    comm_identifer start_socket(int mach_id){
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


    comm_identifer accept_socket(comm_identifer socket, comm_addr* addr, comm_length* length){
#ifdef TWO_SIDE
    #ifdef RDMA


    #else
        return accept(socket, (struct sockaddr*)addr, length);
    #endif
#endif

    }


    int close_socket(comm_identifer ident){
#ifdef TWO_SIDE
    #ifdef RDMA

    #else
        return close(ident);
    #endif
#else
        return 0;
#endif
    }

    int get_transaction_socket(int id) {
        return SOCKET_TRANCTION + id * 10;
    }

    int get_operation_socket(int id) {
        return SOCKET_OPERATION + id * 10;
    }



    bool checkGrammar(Transaction* transaction){
        int *vali = new int[transaction->commands.size()];
        memset(vali, 0, transaction->commands.size() * sizeof(int));

        for(int i = 0; i < transaction->commands.size(); i++){
            Command command = transaction->commands[i];
            switch(command.operation){
                case ALGO_READ: {
                    vali[i] = 1;
                    break;
                }
                case ALGO_WRITE: {
                    bool r_1 = (command.read_result_index_1 < 0 || (command.read_result_index_1 < i && vali[command.read_result_index_1] == 1));
                    if(!r_1)return false;
                    break;
                }
                case ALGO_SUB:
                case ALGO_ADD: {
                    bool r_1 = (command.read_result_index_1 < 0 || (command.read_result_index_1 < i && vali[command.read_result_index_1] == 1)),
                         r_2 = (command.read_result_index_2 < 0 || (command.read_result_index_2 < i && vali[command.read_result_index_2] == 1));
                    if(!r_1 || !r_2)return false;
                    vali[i] = 1;
                    break;
                }
            }
        }
        return true;
    }




};

#endif //RDMA_MEASURE_CCSERVER_HPP
