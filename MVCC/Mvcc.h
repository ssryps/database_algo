//
// Created by mason on 10/1/18.
//

#ifndef RDMA_MEASURE_MVCC_H
#define RDMA_MEASURE_MVCC_H


#include <map>
#include <list>
#include "../utils.h"

struct TableEntry {
    long id;
    long start, end;
    long pointer;
    std::string content;
};

class MvccDatabase{
public:
    int startPos(std::string key);
    void insertStartPos(std::string key, int pos);
    long newTableEntry(TableEntry tableEntry);
    TableEntry getEntry(int pos);
    void updateEntry(int pos, TableEntry tableEntry);

private:
    std::map<std::string, int> keyStartPos;
    std::vector<TableEntry> table;
};


class MvccServer : public Server{
public:
    MvccServer(){};
    TransactionResult handle(Transaction transaction);
    void show();
private:
    std::atomic<long> curTimeStamp;
    MvccDatabase database;
};


#endif //RDMA_MEASURE_MVCC_H
