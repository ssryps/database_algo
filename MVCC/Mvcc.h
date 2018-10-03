//
// Created by mason on 10/1/18.
//

#ifndef RDMA_MEASURE_MVCC_H
#define RDMA_MEASURE_MVCC_H


#include <map>
#include "../utils.h"

struct TableEntry {
    long id;
    long start, endl;
    long pointer;
    bool isValid;
};


class Mvcc : public Server{
public:
    Mvcc(){}
    TransactionResult handle(Transaction transaction);
    void show();
private:
    std::atomic<long> curTimeStamp;
    std::map<std::string, std::vector<TableEntry>> database;
};


#endif //RDMA_MEASURE_MVCC_H
