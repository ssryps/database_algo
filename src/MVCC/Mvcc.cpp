//
// Created by mason on 10/1/18.
//

#include <cstdlib>
#include <atomic>
#include <bits/atomic_base.h>
#include "Mvcc.h"

//MvccDatabase::MvccDatabase() {
//    indexs.init();
//}
//
//void MvccDatabase::updateEntry(int pos, MvccTableEntry MvccTableEntry) {
//    indexs.index_put();
//}
//
//void MvccDatabase::insertStartPos(std::string key, int pos) {
//    keyStartPos[key] = pos;
//}
//
//int MvccDatabase::startPos(std::string key) {
//    if(keyStartPos.count(key) > 0){
//        return keyStartPos[key];
//    } else {
//        return -1;
//    }
//}
//
//long MvccDatabase::newMvccTableEntry(MvccTableEntry MvccTableEntry) {
//    table.push_back(MvccTableEntry);
//    return table.size();
//}
//
//MvccTableEntry MvccDatabase::getEntry(int pos) {
//    return table[pos];
//}

static std::atomic<int> cur(0);
int getCurrentTimeStamp(){
    // this functions is currently local, and need to be extended to global time later
    int oldValue, newValue;
    do {
        oldValue = cur.load(std::memory_order_relaxed);
        newValue = oldValue + 1;
    } while (!std::atomic_compare_exchange_weak(&cur, &oldValue, newValue));
    return newValue;
}

TransactionResult MvccServer::handle(Transaction transaction){
    TransactionResult results;

    int startStamp = getCurrentTimeStamp();
    
    std::vector<long> lasterCheck;
    for (Command command : transaction.commands) {
        if (command.operation == WRITE) {
            idx_key_t curPos;
            if(!database.key_index_start(command.key, curPos)){
                MvccTableEntry *newEntry = new MvccTableEntry{
                        startStamp, startStamp, INT32_MAX, -1, command.value
                };
                idx_key_t pos;
                database.new_index(newEntry, pos);
                database.key_index_insert(command.key, pos);
            } else {
                while (curPos != -1) {
                    MvccTableEntry mvccTableEntry = database.getEntry(curPos);
                    if (mvccTableEntry.pointer == -1) {
                        if (mvccTableEntry.start == startStamp) {
                            MvccTableEntry *newEntry = new MvccTableEntry{mvccTableEntry.id, mvccTableEntry.start,
                                                                  mvccTableEntry.end, mvccTableEntry.pointer, command.value};
                            database.updateEntry(curPos, *newEntry);
                        } else if (mvccTableEntry.start < startStamp && mvccTableEntry.id == -1) {
                            MvccTableEntry *newEntry = new MvccTableEntry{
                                    startStamp, startStamp, INT32_MAX, -1, command.value
                            };
                            int pos = database.newMvccTableEntry(*newEntry);
                            mvccTableEntry.end = startStamp;
                            mvccTableEntry.pointer = pos;
                            database.updateEntry(curPos, mvccTableEntry);
                        } else {
                            results.isSuccess = false;
                            return results;
                        }
                        break;
                    }
                    curPos = mvccTableEntry.pointer;
                }
            }
        }
        if (command.operation == READ) {
            int curPos = database.startPos(command.key);
            if(curPos == -1)results.results.push_back("NULL");
            else {
                while (curPos != -1) {
                    MvccTableEntry mvccTableEntry = database.getEntry(curPos);
                    if (mvccTableEntry.start <= startStamp && startStamp < mvccTableEntry.end) {
                        results.results.push_back(mvccTableEntry.content);
                        if (mvccTableEntry.id != startStamp && mvccTableEntry.id != -1) {
                            lasterCheck.push_back(mvccTableEntry.id);
                        }
                        break;
                    }
                    curPos = mvccTableEntry.pointer;
                }
            }
        }
    }
}

