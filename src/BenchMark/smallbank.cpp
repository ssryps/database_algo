//
// Created by mason on 3/28/19.
//

#include <utils.h>


#define ACCOUNT_SZ 10
#define ACCOUNT_INIT_BALANCE 1000

#define TRANSFER_SZ 10

#define READ_BALANCE(txn, cmd_idx, k) \
{ \
    Command c; \
    c.operation = ALGO_READ; \
    c.key = k; \
    txn->commands.push_back(c);\
    cmd_idx ++; \
}


#define WRITE_BALANCE(txn, cmd_idx, k) \
{ \
    Command c; \
    c.operation = ALGO_WRITE; \
    c.key = k; \
    c.read_result_index_1 = -1; \
    c.imme_1 = ACCOUNT_INIT_BALANCE; \
    txn->commands.push_back(c);\
    cmd_idx ++; \
}

#define TRANSFER_BALANCE(txn, idx, from, to) \
{ \
    Command c1, c2, c3, c4, c5, c6, c7; \
    c1.operation = ALGO_READ; \
    c1.key = from; \
    txn->commands.push_back(c1);\
    \
    int transfer_amount = rand() % 100; \
    c2.operation = ALGO_TIMES; \
    c2.read_result_index_1 = idx; \
    c2.read_result_index_2 = -1; \
    c2.imme_2 = transfer_amount; \
    txn->commands.push_back(c2);\
    \
    c3.operation = ALGO_SUB; \
    c3.read_result_index_1 = idx; \
    c3.read_result_index_2 = idx + 1; \
    txn->commands.push_back(c3);\
    \
    c4.operation = ALGO_WRITE; \
    c4.key = from; \
    c4.read_result_index_1 = idx + 2; \
    txn->commands.push_back(c4);\
    \
    c5.operation = ALGO_READ; \
    c5.key = to; \
    txn->commands.push_back(c5);\
    \
    c6.operation = ALGO_ADD; \
    c6.read_result_index_1 = idx + 4; \
    c6.read_result_index_2 = idx + 1; \
    txn->commands.push_back(c6);\
    \
    c7.operation = ALGO_WRITE; \
    c7.key = to; \
    c7.read_result_index_1 = idx + 5; \
    txn->commands.push_back(c7);\
    \
    cmd_idx += 7; \
}

void smallbank_benchmark_init();
Transaction* smallbank_benchmark(int id);
Transaction* smallbank_benchmark_validation_txn();
bool smallbank_benchmark_varify_rst(TransactionResult* rst);


void smallbank_benchmark_init(){
    srand(time(NULL));
}


Transaction* smallbank_benchmark(int id){
    Transaction *txn = new Transaction;

    if(~id == 0){
        // if id == 0, then the process is intended for account creation
        txn->commands.reserve(ACCOUNT_SZ);

        int cmd_idx = 0;
        for(int i = 0; i < ACCOUNT_SZ; i++){
            WRITE_BALANCE(txn, cmd_idx, i);
        }
    } else {
        // if id != 0, this process is used for transferring money among accounts
        txn->commands.reserve(TRANSFER_SZ);

        int cmd_idx = 0;
        for(int i = 0; i < TRANSFER_SZ; i++){
            int from = rand() % ACCOUNT_SZ, to = rand() % ACCOUNT_SZ;
            TRANSFER_BALANCE(txn, cmd_idx, from, to);
        }
    }

    return txn;
}

Transaction* smallbank_benchmark_validation_txn(){
    Transaction *txn = new Transaction;

    int cmd_idx = 0;
    for(int i = 0; i < ACCOUNT_SZ; i++){
        READ_BALANCE(txn, cmd_idx, i);
    }

    return txn;
}

bool smallbank_benchmark_varify_rst(TransactionResult* txn_rst){
    int money_sum = 0;
    for(int i = 0; i < txn_rst->results.size(); i++){
        money_sum += txn_rst->results[i];
    }
    return (money_sum == ACCOUNT_SZ * ACCOUNT_INIT_BALANCE);
}
