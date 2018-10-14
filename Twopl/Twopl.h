//
// Created by mason on 9/30/18.
//

#ifndef INC_2PL_TWOPL_H
#define INC_2PL_TWOPL_H


#include <map>
#include <mutex>
#include <set>
#include <vector>
#include "index_hashtable.hpp"
#include "../utils.h"


static int TWOPL_TABLE_NUM = 10;
struct TwoplEntry{
    std::mutex* mu;
    std::string key, value;
};


class TwoplEntryBatch {
public:
    TwoplEntry get(int index);
    int find(std::string key);
    void insert(std::string key, TwoplEntry entry);
private:
    std::vector<TwoplEntry> table;
};

class TwoplDatabase {
public:
    static std::hash<std::string> chash;
    TwoplDatabase(){};
    void show();
    void insert(std::string key, TwoplEntry entry);
    TwoplEntry* get(std::string key);
private:
    TwoplEntryBatch getEntryTableBatchByHash(size_t t);
    void updateEntryTableBatchByHash(size_t t, TwoplEntryBatch batch);
    std::vector<TwoplEntryBatch> tables;
};


class TwoplServer : Server{

public:
    TwoplServer(){}
    TransactionResult handle(Transaction transaction);
    void show();
private:
    void insert(std::string key, TwoplEntry value);
    TwoplEntry* get(std::string key);
    TwoplDatabase database;
}
;




#endif //INC_2PL_TWOPL_H
