//
// Created by mason on 10/1/18.
//

#include <cstdlib>
#include <atomic>
#include <bits/atomic_base.h>
#include "Mvcc.h"

void Mvcc::show() {
    printf("database: **********\n");
    for(auto i = database.begin(); i != database.end(); i++){
        printf("%s: %s\n", (*i).first.c_str(), (*i).second.c_str());
    }
}


TransactionResult Mvcc::handle(Transaction transaction){
    long oldValue, myIndex;
    do {
        oldValue = this->curTimeStamp.load(std::memory_order_relaxed);
        myIndex = oldValue + 1;
    } while (std::atomic_compare_exchange_weak(&curTimeStamp, &oldValue, myIndex));

    for (Command command : transaction.commands) {
        if (command.operation == WRITE) {


        } else {

        }
    }
}
