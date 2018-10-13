//
// Created by mason on 9/30/18.
//

#include "Twopl.h"
#include <set>

void Twopl::show() {
    printf("database: **********\n");
    for(auto i = database.begin(); i != database.end(); i++){
        printf("%s: %s\n", (*i).first.c_str(), (*i).second.c_str());
    }
}

void Twopl::insert(std::string key, std::string value) {
    this->database[key] = value;
}

std::string Twopl::get(std::string key) {
    if(this->database.count(key) > 0){
        return this->database[key];
    } else {
        return "NULL";
    }

}

TransactionResult Twopl::handle(Transaction transaction) {
    TransactionResult results;
    std::set<std::string> keys;
    for (Command command : transaction.commands) {
        if (this->database.count(command.key) > 0) {
            keys.insert(command.key);
        } else {
            keys.insert(command.key);
            std::mutex *mu = new std::mutex;
            this->mutexs.insert(std::pair<std::string, std::mutex*>(command.key, mu));
        }
    }

    for(auto i = keys.begin(); i != keys.end(); i++){
        this->mutexs[*i]->lock();
    }


    for (Command command : transaction.commands) {
        if(command.operation == WRITE){
            this->insert(command.key, command.value);
            results.results.push_back(command.value);
        } else{
            results.results.push_back(this->get(command.key));
        }
    }
    for(Command command : transaction.commands){
        this->mutexs[command.key]->unlock();
    }
    results.isSuccess = true;

    return results;
}