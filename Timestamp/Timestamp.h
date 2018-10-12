//
// Created by mason on 10/12/18.
//

#ifndef RDMA_MEASURE_TIMESTAMP_H
#define RDMA_MEASURE_TIMESTAMP_H


#include <map>
#include <list>
#include "../utils.h"

class TimestampDatabase {
    TimestampDatabase(){};
};

class Timestamp : public Server {

public:
    Timestamp(){};
    TransactionResult handle(Transaction transaction);
    void show();
private:
    TimestampDatabase();
};


#endif //RDMA_MEASURE_TIMESTAMP_H
