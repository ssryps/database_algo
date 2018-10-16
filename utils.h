//
// Created by mason on 9/30/18.
//

#ifndef RDMA_MEASURE_UTILS_H
#define RDMA_MEASURE_UTILS_H

#include <string>
#include <vector>

enum RC {OK, ABORT, COMMIT, WAIT, ERROR};

enum Operation {READ, WRITE};

struct Command{
    Operation operation;
    std::string key, value;
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
#endif //RDMA_MEASURE_UTILS_H
