//
// Created by mason on 9/30/18.
//
#include <chrono>
#include "Occ.h"
#include <sys/time.h>

void OccServer::show() {
    printf("database: **********\n");
    for(auto i = database.begin(); i != database.end(); i++){
        printf("%s: %s\n", (*i).first.c_str(), (*i).second.c_str());
    }
}


TransactionResult OccServer::handle(Transaction transaction){
    TransactionResult results;
    std::map<std::string, std::string> tempDatabase;

    timeval tp;
    gettimeofday(&tp, NULL);
    long start = tp.tv_sec * 1000 + tp.tv_usec / 1000;
    // local submit
    for (Command command : transaction.commands) {
        if(command.operation == WRITE){
            tempDatabase[command.key] = command.value;
            results.results.push_back(command.value);
        }
        if(command.operation == READ){
            if(tempDatabase.count(command.key) > 0) {
                results.results.push_back(tempDatabase[command.key]);
            } else if(database.count(command.key) > 0) {
                results.results.push_back(database[command.key]);
            } else {
                results.results.push_back("NULL");
            }
        }
    }

    // validation phase
    this->mu.lock();
    if(endtime.size() >= 1) {
        printf("%i size \n",  endtime.size());
        if(endtime[endtime.size() - 1] > start) {
            results.isSuccess = false;
            results.results.clear();
            return results;
        }
    }

    gettimeofday(&tp, NULL);
    long end = tp.tv_sec * 1000 + tp.tv_usec / 1000;

    endtime.push_back(end);
    for(auto i = tempDatabase.begin(); i != tempDatabase.end(); i++){
        this->database[i->first] = i->second;
    }

    this->mu.unlock();
    // write phase
    results.isSuccess = true;
    return results;
}
