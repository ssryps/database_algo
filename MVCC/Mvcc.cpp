//
// Created by mason on 10/1/18.
//

#include <cstdlib>
#include <atomic>
#include <bits/atomic_base.h>
#include "Mvcc.h"

void MvccDatabase::updateEntry(int pos, MvccTableEntry MvccTableEntry) {
    table[pos] = MvccTableEntry;
}

void MvccDatabase::insertStartPos(std::string key, int pos) {
    keyStartPos[key] = pos;
}

int MvccDatabase::startPos(std::string key) {
    if(keyStartPos.count(key) > 0){
        return keyStartPos[key];
    } else {
        return -1;
    }
}

long MvccDatabase::newMvccTableEntry(MvccTableEntry MvccTableEntry) {
    table.push_back(MvccTableEntry);
    return table.size();
}

MvccTableEntry MvccDatabase::getEntry(int pos) {
    return table[pos];
}


TransactionResult MvccServer::handle(Transaction transaction){
    TransactionResult results;
    long oldValue, startStamp;
    do {
        oldValue = this->curTimeStamp.load(std::memory_order_relaxed);
        startStamp = oldValue + 1;
    } while (!std::atomic_compare_exchange_weak(&curTimeStamp, &oldValue, startStamp));

    
    std::vector<long> lasterCheck;
    for (Command command : transaction.commands) {
        if (command.operation == WRITE) {
            int curPos = database.startPos(command.key);
            if(curPos == -1){
                MvccTableEntry *newEntry = new MvccTableEntry{
                        startStamp, startStamp, INT32_MAX, -1, command.value
                };
                int pos = database.newMvccTableEntry(*newEntry);
                database.insertStartPos(command.key, pos);
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
