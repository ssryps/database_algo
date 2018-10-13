//
// Created by mason on 10/12/18.
//

#include <atomic>
#include <iostream>
#include "Timestamp.h"

TransactionResult TimestampServer::handle(Transaction transaction) {
    TransactionResult result;
    int timeStamp = getCurrentTimeStamp();
    std::vector<TimestampEntry> tempEntries;
    bool abort = false;

    std::map<std::string, TimestampEntry> tempMap;
    for(Command command : transaction.commands){
        if(command.operation == READ){

            // get timestampEntry first from local map, then database. if NULL, create a new one
            if(tempMap.count(command.key) == 0){
                TimestampEntry *entry = this->database.get(command.key);
                if (entry != NULL)tempMap[command.key] = *entry;
                else tempMap[command.key] = TimestampEntry{command.key, "NULL", -1, -1};
            }

            // check if conflict happens
            if(tempMap[command.key].lastWrite > timeStamp){
                abort = true;
                break;
            } else {
                // update local map, store new entries to a temp host
                result.results.push_back(tempMap[command.key].value);
                int readTime = std::max(tempMap[command.key].lastRead, timeStamp),
                    writeTime = tempMap[command.key].lastWrite;
                TimestampEntry entry1 = TimestampEntry{command.key, tempMap[command.key].value, readTime, writeTime};
                tempMap[command.key] = entry1;
                tempEntries.push_back(entry1);
            }
        }
        if(command.operation == WRITE){
            if(tempMap.count(command.key) == 0) {
                TimestampEntry *entry = this->database.get(command.key);
                if (entry != NULL)tempMap[command.key] = *entry;
                else tempMap[command.key] = TimestampEntry{command.key, "NULL", -1, -1};
            }

            // check if conflict happens
            if(tempMap[command.key].lastRead > timeStamp || tempMap[command.key].lastWrite > timeStamp){
                // if so, abort this transaction
                abort = true;
                break;
            } else {
                // update local map, store new entries to a temp host
                result.results.push_back(command.value);
                int readTime = tempMap[command.key].lastRead,
                    writeTime = timeStamp;
                TimestampEntry entry1 = TimestampEntry{command.key, command.value, readTime, writeTime};
                tempMap[command.key] = entry1;
                tempEntries.push_back(entry1);
            }
        }
    }
    if(abort){ std::cout << "*********************************************\n";
        return handle(transaction);
    }

    for(TimestampEntry entry: tempEntries) {
        this->database.set(entry.key, entry);
    }
    return result;

}

void TimestampServer::show() {
    database.show();
}



std::hash<std::string> TimestampDatabase::chash = std::hash<std::string>();

TimestampDatabase::TimestampDatabase() {
    tables = std::vector<std::vector<TimestampEntry>>(TABLE_NUM);
}

void TimestampDatabase::set(std::string key, TimestampEntry entry) {
    size_t t = chash(key) % TABLE_NUM;
    TimestampEntryBatch table = getEntryTableBatchByHash(t);

    TimestampEntry entry1 = table.find(key);
    bool in = false;
    for(int i = 0; i < table.size(); i++){
        if((*table)[i]key == key){
            (*table)[i] = entry;
            in = true;
        }
    }
    if(!in)table->push_back(entry);
}

TimestampEntry* TimestampDatabase::get(std::string key) {
    size_t t = chash(key.c_str()) % TABLE_NUM;
    for(TimestampEntry entry: tables[t]){
        if(entry.key == key){
            TimestampEntry *result = new TimestampEntry(entry);
            return result;
        }
    }
    return NULL;
}

void TimestampDatabase::show() {
    std::cout << "\nPRINT DATABASE\n";
    for(auto table: tables){
        if(table.size() > 0)std::cout << "new table \n";
        for(auto entry: table){
            std::cout << entry.key << " : " << entry.value << " last read : " << entry.lastRead
                        << " write : " << entry.lastWrite << std::endl;
        }
    }

}

std::atomic<int> cur(0);
int getCurrentTimeStamp(){
    int oldValue, newValue;
    do {
        oldValue = cur.load(std::memory_order_relaxed);
        newValue = oldValue + 1;
    } while (!std::atomic_compare_exchange_weak(&cur, &oldValue, newValue));
    return newValue;
}