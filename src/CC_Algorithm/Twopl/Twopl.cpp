//
// Created by mason on 9/30/18.
//

#include "Twopl.h"


bool TwoplServer::rdma_insert(int mach_id, idx_key_t key, TwoplEntry entry){

}

bool TwoplServer::rdma_get(int mach_id, idx_key_t key, TwoplEntry entry){
    
}


bool TwoplServer::rdma_lock(int mach_id, idx_key_t key, TwoplEntry entry){
    
}

bool TwoplServer::rdma_unlock(int mach_id, idx_key_t key, TwoplEntry entry){
    
}


bool TwoplServer::insert(std::string key, TwoplEntry value) {
    return rdma_insert(get_machine_index(key), key, value);
}

bool TwoplServer::get(std::string key, TwoplEntry* entry) {
    return rdma_get(get_machine_index(key), key, entry);
}

bool TwoplServer::lock(idx_key_t key) {
    return rdma_lock(get_machine_index(key), key);
}

bool TwoplServer::unlock(idx_key_t key) {
    return rdma_unlock(get_machine_index(key), key);
}


TransactionResult TwoplServer::handle(Transaction transaction) {
    TransactionResult results;
    std::set<idx_key_t > keys;
    for (Command command : transaction.commands) { keys.insert(command.key); }
    for(auto i = keys.begin(); i != keys.end(); i++){ lock(*i); }

    for (Command command : transaction.commands) {
        TwoplEntry *entry;
        get(command.key, entry);
        if(command.operation == WRITE){
            entry->value = command.value;
            insert(command.key, *entry);
            results.results.push_back(command.value);
        } else{
            results.results.push_back(get(command.key)->value);
        }
    }

    for(auto i = keys.begin(); i != keys.end(); i++){ unlock(*i); }
    results.isSuccess = true;

    return results;
}