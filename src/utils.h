//
// Created by mason on 9/30/18.
//

#ifndef RDMA_MEASURE_UTILS_H
#define RDMA_MEASURE_UTILS_H

#include <string>
#include <vector>
#include <cstring>
#include "defines.h"


#define MAXLINE 4096


enum RC {OK, ABORT, COMMIT, WAIT, ERROR};

enum CC_ALGO {ALGO_TWOPL, ALGO_OCC, ALGO_MVCC, ALGO_TIMESTAMP};

enum Operation {ALGO_READ, ALGO_WRITE, ALGO_ADD, ALGO_SUB};

enum Socket_Type {MSG_SCK, META_SERVER_SCK, TXN_SCK };

#define TXN_SCK_NUM 9000
#define MSG_SCK_NUM 9001
#define META_SERVER_SCK_NUM 9002


struct Command{
    Operation operation;
    idx_key_t key;
    idx_value_t imme_1, imme_2;
    // if you need to use the immediate numbers, keep corresponding read_result_index less than 0
    // if reading results are used as one or two algo operation input, set read_result_index to:
    //     index of read command in transaction
    int read_result_index_1, read_result_index_2;
};



idx_value_t value_from_command(Command command, idx_value_t* temp_result);

struct Transaction{
    std::vector<Command> commands;
};

class TransactionResult{
public:
    std::vector<idx_value_t> results;
    bool is_success;
};



int get_machine_index(idx_key_t key);

const int MAX_DATA_PER_MACH = 1000000;

const int SERVER_THREAD_NUM = 4;
const int CLIENT_THREAD_NUM = 2;
const int SERVER_DATA_BUF_SIZE = 1024 * 1024 * 100;
const int MEG_BUF_SIZE = 1024 * 10;


#endif //RDMA_MEASURE_UTILS_H
