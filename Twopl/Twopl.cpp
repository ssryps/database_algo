//
// Created by mason on 9/30/18.
//

#include "Twopl.h"

TwoplEntry TwoplEntryBatch::get(int index) {
    return table[index];
}

int TwoplEntryBatch::find(std::string key) {
    for(int i = 0; i < table.size(); i++){
        if(table[i].key == key)return i;
    }
    return -1;
}

void TwoplEntryBatch::insert(std::string key, TwoplEntry entry) {
    int index = find(key);
    if(index != -1){
        table[index] = entry;
    } else {
        table.push_back(entry);
    }
}

std::hash<std::string> TwoplDatabase::chash = std::hash<std::string>();

void TwoplDatabase::show() {
    printf("database: **********\n");
//    for(auto table : tables){
//        for(TwoplEntry entry: table){
//            printf("%s: %s\n", entry.key.c_str(), entry.value.c_str());
//        }
//    }
}

void TwoplDatabase::insert(std::string key, TwoplEntry value) {
    size_t t = chash(key);
    TwoplEntryBatch table = getEntryTableBatchByHash(t);
    table.insert(key, value);
    updateEntryTableBatchByHash(t, table);
}

TwoplEntry* TwoplDatabase::get(std::string key) {
    size_t t = chash(key);
    TwoplEntryBatch table = getEntryTableBatchByHash(t);
    int index = table.find(key);
    if(index != -1) {
        TwoplEntry *entry = new TwoplEntry(table.get(index));
        return entry;
    }
    return NULL;
}

void TwoplDatabase::updateEntryTableBatchByHash(size_t hash, TwoplEntryBatch batch){
    tables[hash % TWOPL_TABLE_NUM] = batch;
}

TwoplEntryBatch TwoplDatabase::getEntryTableBatchByHash(size_t hash) {
    return tables[hash % TWOPL_TABLE_NUM];
}



// TwoplServer functions
void TwoplServer::show() { database.show(); }

void TwoplServer::insert(std::string key, TwoplEntry value) { database.insert(key, value); }

TwoplEntry* TwoplServer::get(std::string key) { return database.get(key); }


TransactionResult TwoplServer::handle(Transaction transaction) {
    TransactionResult results;
    std::set<TwoplEntry*> keys;
    for (Command command : transaction.commands) {
        TwoplEntry *entry = database.get(command.key);
        if(entry != NULL){
            keys.insert(entry);
        } else {
            TwoplEntry* entry1 = new TwoplEntry{new std::mutex, command.key, "NULL"};
            insert(command.key, *entry1);
            keys.insert(entry1);
        }
    }

    for(auto i = keys.begin(); i != keys.end(); i++){
        (*i)->mu->lock();
    }


    for (Command command : transaction.commands) {
        TwoplEntry *entry = get(command.key);
        if(command.operation == WRITE){
            entry->value = command.value;
            insert(command.key, *entry);
            results.results.push_back(command.value);
        } else{
            results.results.push_back(get(command.key)->value);
        }
    }
    for(Command command : transaction.commands){
        TwoplEntry *entry = get(command.key);
        entry->mu->unlock();
    }
    results.isSuccess = true;

    return results;
}