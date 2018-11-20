//
// Created by mason on 9/30/18.
//

#ifndef RDMA_MEASURE_UTILS_H
#define RDMA_MEASURE_UTILS_H

#include <string>
#include <vector>
#include <cstring>
#include "defines.h"


#define COMMAND_LEN sizeof(Command)
const int MACHINE_NUM = 8;
const int MAX_DATA_PER_MACH = 100000;

enum RC {OK, ABORT, COMMIT, WAIT, ERROR};

enum CC_ALGO{ALGO_TWOPL, ALGO_OCC, ALGO_MVCC, ALGO_TIMESTAMP};

enum Operation {READ, WRITE, ALGO_ADD, ALGO_SUB};



struct Command{
    Operation operation;
    idx_key_t key;
    idx_value_t imme_1, imme_2;
    // if you need to use the immediate numbers, keep corresponding read_result_index less than 0
    // if reading results are used as one or two algo operation input, set read_result_index to:
    //     index of read command in transaction
    int read_result_index_1, read_result_index_2;
};



idx_value_t value_from_command(Command command, idx_value_t* temp_result){
    idx_value_t result = 0;

    if(command.operation == WRITE) {
        if (command.read_result_index_1 < 0)result += command.imme_1;
        else result += temp_result[command.read_result_index_1];
    }
    if(command.operation == ALGO_ADD){
        if (command.read_result_index_1 < 0)result += command.imme_1;
        else result += temp_result[command.read_result_index_1];

        if (command.read_result_index_2 < 0)result += command.imme_2;
        else result += temp_result[command.read_result_index_2];
    }

    if(command.operation == ALGO_SUB){
        if (command.read_result_index_1 < 0)result += command.imme_1;
        else result += temp_result[command.read_result_index_1];

        if (command.read_result_index_2 < 0)result -= command.imme_2;
        else result -= temp_result[command.read_result_index_2];
    }
    return result;
}


class Transaction{
public:
    std::vector<Command> commands;
};

class TransactionResult{
public:
    std::vector<idx_value_t> results;
    bool isSuccess;
};



int get_machine_index(idx_key_t key){
    return (key % (MAX_DATA_PER_MACH * MACHINE_NUM)) / MAX_DATA_PER_MACH;
}




#endif //RDMA_MEASURE_UTILS_H
