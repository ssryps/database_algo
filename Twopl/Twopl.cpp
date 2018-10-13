//
// Created by mason on 9/30/18.
//

#include "Twopl.h"



void TwoplDatabase::show() {
    printf("database: **********\n");
    for(auto table : tables){
        for(TwoplEntry entry: table){
            printf("%s: %s\n", entry.key.c_str(), entry.value.c_str());
        }
    }
}

void TwoplDatabase::insert(std::string key, std::string value) {
    size_t t = chash(key);
    std::vector<TwoplEntry> table = getEntryTableByHash(t);
}

std::vector<TwoplEntry> TwoplDatabase::getEntryTableByHash(size_t t) {
    return tables[t % ]
}


void TwoplServer::show() { database.show(); }

void TwoplServer::insert(std::string key, std::string value) { database.insert(key, value); }

std::string TwoplServer::get(std::string key) { return database.get(key); }


TransactionResult TwoplServer::handle(Transaction transaction) {
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