//
// Created by mason on 10/12/18.
//

#ifndef RDMA_MEASURE_TIMESTAMP_H
#define RDMA_MEASURE_TIMESTAMP_H


#include <map>
#include <list>
#include "utils.h"
#include "../CCServer.hpp"

static int TIMESTAMP_TABLE_NUM = 10;

struct TimestampEntry{
    std::string key, value;
    int lastRead, lastWrite;
};

class TimestampEntryBatch{
public:
    TimestampEntry get(int index);
    int find(std::string key);
    void insert(std::string key, TimestampEntry entry);
private:
    std::vector<TimestampEntry> table;
};


class TimestampDatabase {
public:
    static std::hash<std::string> chash;

    TimestampDatabase();
    TimestampEntry* get(std::string key);
    void set(std::string key, TimestampEntry entry);
    void show();
private:
    TimestampEntryBatch getEntryTableBatchByHash(size_t hash);
    void updateEntryTableBatchByHash(size_t hash, TimestampEntryBatch batch);
    // just for local test, this tables can be extended to multiple machine
    std::vector<TimestampEntryBatch> tables;
};

int getCurrentTimeStamp();

class TimestampServer : public CCServer {

public:
    TimestampServer(){};
    TransactionResult handle(Transaction transaction);
    void show();
private:
    TimestampDatabase database;
};


#endif //RDMA_MEASURE_TIMESTAMP_H
