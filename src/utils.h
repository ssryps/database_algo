//
// Created by mason on 9/30/18.
//

#ifndef RDMA_MEASURE_UTILS_H
#define RDMA_MEASURE_UTILS_H

#include <string>
#include <vector>
#include "defines.h"

const int MACHINE_NUM = 8;
const int MACHINE_MAX_HASH = 1000;

enum RC {OK, ABORT, COMMIT, WAIT, ERROR};

enum Operation {READ, WRITE};

struct Command{
    Operation operation;
    idx_key_t key;
    idx_value_t value;
};

class Transaction{
public:
    std::vector<Command> commands;
};

class TransactionResult{
public:
    std::vector<std::string> results;
    bool isSuccess;
};

class Server {
public:
    TransactionResult handle(Transaction transaction);
    void show();
};

int get_machine_index(idx_key_t key){
    return (key % (MACHINE_MAX_HASH * MACHINE_NUM)) / MACHINE_MAX_HASH;
}


#endif //RDMA_MEASURE_UTILS_H
