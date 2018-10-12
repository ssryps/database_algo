//
// Created by mason on 10/12/18.
//

#ifndef RDMA_MEASURE_TIMESTAMP_H
#define RDMA_MEASURE_TIMESTAMP_H


#include <map>
#include <list>
#include "../utils.h"

const int TABLE_NUM = 2;

struct TimestampEntry{
    std::string key, value;
    int lastRead, lastWrite;
};


class TimestampDatabase {
public:
    static std::hash<std::string> chash;

    TimestampDatabase();
    TimestampEntry* get(std::string key);
    void set(std::string key, TimestampEntry entry);
    void show();
private:
    std::vector<std::vector<TimestampEntry>> tables;
};

int getCurrentTimeStamp();

class TimestampServer : public Server {

public:
    TimestampServer(){};
    TransactionResult handle(Transaction transaction);
    void show();
private:
    TimestampDatabase database;
};


#endif //RDMA_MEASURE_TIMESTAMP_H
