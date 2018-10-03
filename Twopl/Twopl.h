//
// Created by mason on 9/30/18.
//

#ifndef INC_2PL_TWOPL_H
#define INC_2PL_TWOPL_H


#include <map>
#include <mutex>
#include "../utils.h"

class Twopl : Server{

public:
    Twopl(){}
    TransactionResult handle(Transaction transaction);
    void show();
private:
    void insert(std::string key, std::string value);
    std::string get(std::string key);
    std::map<std::string, std::string> database;
    std::map<std::string, std::mutex*> mutexs;
}
;




#endif //INC_2PL_TWOPL_H
