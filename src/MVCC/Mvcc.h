//
// Created by mason on 10/1/18.
//

#ifndef RDMA_MEASURE_MVCC_H
#define RDMA_MEASURE_MVCC_H


#include <map>
#include <list>
#include <atomic>
#include "../utils.h"
#include "../Storage/index_table.hpp"

struct MvccTableEntry {
    long id;
    long start, end;
    long pointer;
    std::string content;
};

//class MvccDatabase{
//public:
//    MvccDatabase();
//    int startPos(std::string key);
//    void insertStartPos(std::string key, int pos);
//    long newMvccTableEntry(MvccTableEntry MvccTableEntry);
//    MvccTableEntry getEntry(int pos);
//    void updateEntry(int pos, MvccTableEntry MvccTableEntry);
//
//private:
//    std::map<std::string, int> keyStartPos;
//};

int getCurrentTimeStamp();


class MvccServer : public Server{
public:
    MvccServer(){};
    TransactionResult handle(Transaction transaction);
    void show();
private:
    TableIndex<MvccTableEntry> database;
};


#endif //RDMA_MEASURE_MVCC_H
