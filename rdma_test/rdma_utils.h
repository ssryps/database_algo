
//
// Created by mason on 10/09/18.
//

#ifndef RDMA_MEASURE_RDMA_UTILS_H
#define RDMA_MEASURE_RDMA_UTILS_H


#include <assert.h>
#include <stdio.h>
#define _XOPEN_SOURCE 600	/* for posix_memalign */
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <netdb.h>
#include <infiniband/verbs.h>
#include <arpa/inet.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <thread>
#include <iostream>
#include "../utils.h"

struct exchange_params {
    int lid;
    int qpn;
    int psn;
};

#define DEPTH 1024
#define NUM_RTTS 1000000
#define TICKS_PER_USEC 2400

const int COMMMAND_PER_TRANSACTION = 4;
const int KEY_RANGE = 5;
const int VALUE_RANGE = 5;

static uint64_t rtt_times[NUM_RTTS];

static uint64_t
rdtsc()
{
    uint32_t lo, hi;
    __asm__ __volatile__ ("rdtsc" : "=a" (lo), "=d" (hi));
    return (((uint64_t)hi << 32) | lo);
}

static int
ibGetLID(struct ibv_context *ctxt, int port)
{
    struct ibv_port_attr ipa;
    if (ibv_query_port(ctxt, port, &ipa)) {
        fprintf(stderr, "ibv_query_port failed\n");
        exit(1);
    }
    return ipa.lid;
}

static struct ibv_device *
ibFindDevice(const char *name)
{
    struct ibv_device **devices;

    devices = ibv_get_device_list(NULL);
    if (devices == NULL)
        return NULL;

    if (name == NULL)
        return devices[0];

    for (int i = 0; devices[i] != NULL; i++) {
        if (strcmp(devices[i]->name, name) == 0)
            return devices[i];
    }

    return NULL;
}

static void
ibPostReceive(struct ibv_qp *qp, struct ibv_mr *mr, char *rxbuf, size_t rxbufsize)
{
    struct ibv_sge isge = { (uint64_t)rxbuf, rxbufsize, mr->lkey };
    struct ibv_recv_wr irwr;

    memset(&irwr, 0, sizeof(irwr));
    irwr.wr_id = 1;
    irwr.next = NULL;
    irwr.sg_list = &isge;
    irwr.num_sge = 1;

    struct ibv_recv_wr *bad_irwr;
    if (ibv_post_recv(qp, &irwr, &bad_irwr)) {
        fprintf(stderr, "failed to ibv_post_recv\n");
        exit(1);
    }
}

static void
ibPostSend(struct ibv_qp *qp, struct ibv_mr *mr, char *txbuf, size_t txbufsize)
{
    struct ibv_sge isge = { (uint64_t)txbuf, txbufsize, mr->lkey };
    struct ibv_send_wr iswr;

    memset(&iswr, 0, sizeof(iswr));
    iswr.wr_id = 2;
    iswr.next = NULL;
    iswr.sg_list = &isge;
    iswr.num_sge = 1;
    iswr.opcode = IBV_WR_SEND;
    iswr.send_flags = IBV_SEND_SIGNALED;

    struct ibv_send_wr *bad_iswr;
    if (ibv_post_send(qp, &iswr, &bad_iswr)) {
        fprintf(stderr, "ibv_post_send failed!\n");
        exit(1);
    }
}

static void
ibPostSendAndWait(struct ibv_qp *qp, struct ibv_mr *mr, char *txbuf, size_t txbufsize, struct ibv_cq *cq)
{
    ibPostSend(qp, mr, txbuf, txbufsize);

    struct ibv_wc iwc;
    while (ibv_poll_cq(cq, 1, &iwc) < 1)
        ;
    if (iwc.status != IBV_WC_SUCCESS) {
        fprintf(stderr, "ibv_poll_cq returned failure\n");
        exit(1);
    }
}




static struct exchange_params
client_exchange(const char *server, uint16_t port, struct exchange_params *params)
{
    int s = socket(PF_INET, SOCK_STREAM, IPPROTO_TCP);
    if (s == -1) {
        perror("socket");
        exit(1);
    }

    struct hostent *hent = gethostbyname(server);
    if (hent == NULL) {
        perror("gethostbyname");
        exit(1);
    }

    struct sockaddr_in sin;
    memset(&sin, 0, sizeof(sin));
    sin.sin_family = PF_INET;
    sin.sin_port = htons(port);
    sin.sin_addr = *((struct in_addr *)hent->h_addr);

    if (connect(s, (struct sockaddr *)&sin, sizeof(sin)) == -1) {
        perror("connect");
        exit(1);
    }

    write(s, params, sizeof(*params));
    read(s, params, sizeof(*params));

    close(s);

    return *params;
}

static struct exchange_params
server_exchange(uint16_t port, struct exchange_params *params)
{
    int s = socket(PF_INET, SOCK_STREAM, IPPROTO_TCP);
    if (s == -1) {
        perror("socket");
        exit(1);
    }

    int on = 1;
    if (setsockopt(s, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on)) == -1) {
        perror("setsockopt");
        exit(1);
    }

    struct sockaddr_in sin;
    memset(&sin, 0, sizeof(sin));
    sin.sin_family = PF_INET;
    sin.sin_port = htons(port);
    sin.sin_addr.s_addr = htonl(INADDR_ANY);

    if (bind(s, (struct sockaddr *)&sin, sizeof(sin)) == -1) {
        perror("bind");
        exit(1);
    }

    if (listen(s, 1) == -1) {
        perror("listen");
        exit(1);
    }

    struct sockaddr_in csin;
    socklen_t csinsize = sizeof(csin);
    int c = accept(s, (struct sockaddr *)&csin, &csinsize);
    if (c == -1) {
        perror("accept");
        exit(1);
    }

    write(c, params, sizeof(*params));
    read(c, params, sizeof(*params));

    close(c);
    close(s);

    return *params;
}


Command generateCommand(){
    Command command;
    Operation operation = (rand() % 2 == 0? WRITE: READ);
    std::string key = std::to_string(rand() % KEY_RANGE), value = std::to_string(rand() % VALUE_RANGE);
    command.operation = operation;
    command.key = key;
    command.value = value;
    return command;
}

size_t putTransactionToBuffer(Transaction* transaction, char* buf){
    char* start = buf;
    for(auto command: transaction->commands) {
        (*start) = (command.operation == READ ? '0' : '1');
        start++;
        (*start) = '\t';
        start++;
        for (int i = 0; i < command.key.size(); i++) {
            (*start) = command.key[i];
            start++;
        }
        (*start) = '\t';
        start++;
        for (int i = 0; i < command.value.size(); i++) {
            (*start) = command.value[i];
            start++;
        }
        (*start) = '\n';
        start++;
    }
    return start - buf;
}

Transaction getTransactionFromBuffer(char* buf, size_t length){
    Transaction *transaction = new Transaction;
    char* startPos = buf;
    Command *tempCommand = new Command;
    int type = 0, size = 0;
    for(int i = 0; i < length; i++){
        if(buf[i] == '\n' || buf[i] == '\t'){
            std::string temp = std::string(startPos, size);
            switch (type){
                case 0: {
                    tempCommand->operation = (temp == "0"? READ: WRITE);
                    break;
                }
                case 1: {
                    tempCommand->key = temp;
                    break;
                }
                default: {
                    tempCommand->value = temp;
                    break;
                }
            }
            if(buf[i] == '\n') {
                transaction->commands.push_back(*tempCommand);
                tempCommand = new Command;
            }
            type = (type + 1) % 3;
            startPos = buf + i + 1;
            size = 0;
        } else {
            size ++ ;
        }
    }
    return *transaction;
}

size_t putResultToBuffer(TransactionResult result, char* start){
    char *init = start;
    for(auto command: result.results){
        for(int i = 0; i < command.size(); i++){
            *start = command[i];
            start ++;
        }
        *start = '\n';
        start ++ ;
    }
    return start - init;
}

TransactionResult getResultFromBuffer(char* buf, size_t length){
    TransactionResult *result = new TransactionResult;
    int len = 0;
    char *startpos = buf;
    for(size_t i = 0; i < length; i++){
        if(*(buf + i) == '\n'){
            std::string *temp = new std::string(startpos, len);
            result->results.push_back(*temp);
            startpos = buf + i + 1;
            len = 0;
        } else {
            len ++;
        }
    }
    return *result;

}


#endif //RDMA_MEASURE_RDMA_UTILS_H
