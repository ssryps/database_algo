//
// Created by mason on 9/30/18.
//

#ifndef RDMA_MEASURE_OCC_H
#define RDMA_MEASURE_OCC_H


#include <map>
#include <mutex>
#include "../utils.h"

class OccServer : Server{
public:
    OccServer(){}
    TransactionResult handle(Transaction transaction);
    void show();
private:
    std::mutex mu;
    std::map<std::string, std::string> database;
    std::vector<long> endtime;
};


#endif //RDMA_MEASURE_OCC_H
