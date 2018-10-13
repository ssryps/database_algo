//
// Created by mason on 9/30/18.
//

#ifndef INC_2PL_TWOPL_H
#define INC_2PL_TWOPL_H


#include <map>
#include <mutex>
#include <set>
#include <vector>
#include "../utils.h"


const static int TABLE_NUL = 10;
struct TwoplEntry{
    std::mutex mu;
    std::string key, value;
};


class TwoplDatabase {
public:
    static std::hash<std::string> chash;
    TwoplDatabase(){};
    void show();
    void insert(std::string key, std::string value);
    std::string get(std::string key);
private:
    std::vector<TwoplEntry>* getEntryTableByHash(size_t t);
    std::vector<std::vector<TwoplEntry>> tables;
};


class TwoplServer : Server{

public:
    TwoplServer(){}
    TransactionResult handle(Transaction transaction);
    void show();
private:
    void insert(std::string key, std::string value);
    std::string get(std::string key);
    TwoplDatabase database;
}
;




#endif //INC_2PL_TWOPL_H
