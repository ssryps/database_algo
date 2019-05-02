//
// Created by mason on 5/2/19.
//

#include <utils.h>

#define READ_KEY_VALUE(txn, cmd_idx, k) \
{ \
    Command c; \
    c.operation = ALGO_READ; \
    c.key = k; \
    txn->commands.push_back(c);\
    cmd_idx ++; \
}


void self_made_benchmark_init();
Transaction* self_made_benchmark(int id);
Transaction* self_made_benchmark_validation_txn();
bool self_made_benchmark_varify_rst(TransactionResult* rst);


void self_made_benchmark_init() {}

Transaction* self_made_benchmark(int id) {
    Command *command = new Command[100];
    int idx = 0;

    command[idx].operation = ALGO_WRITE;
    command[idx].key = 0;
    command[idx].read_result_index_1 = -1;
    command[idx].imme_1 = 500;
    idx++;

    command[idx].operation = ALGO_READ;
    command[idx].key = 0;
    idx++;

    command[idx].operation = ALGO_READ;
    command[idx].key = 1;
    idx++;

    command[idx].operation = ALGO_SUB;
    command[idx].read_result_index_1 = idx - 2;
    command[idx].read_result_index_2 = -1;
    command[idx].imme_2 = 100;
    idx++;

    command[idx].operation = ALGO_WRITE;
    command[idx].key = 0;
    command[idx].read_result_index_1 = idx - 1;
    idx++;

    command[idx].operation = ALGO_WRITE;
    command[idx].key = 1;
    command[idx].read_result_index_1 = idx - 4;
    idx++;

    command[idx].operation = ALGO_READ;
    command[idx].key = 0;
    idx++;

    command[idx].operation = ALGO_READ;
    command[idx].key = 1;
    idx++;

    command[idx].operation = ALGO_WRITE;
    command[idx].key = 1 + MAX_DATA_PER_MACH;
    command[idx].read_result_index_1 = -1;
    command[idx].imme_1 = 1000;
    idx++;

    command[idx].operation = ALGO_READ;
    command[idx].key = 0 + MAX_DATA_PER_MACH;
    idx++;

    command[idx].operation = ALGO_READ;
    command[idx].key = 1 + MAX_DATA_PER_MACH;
    idx++;

    command[idx].operation = ALGO_ADD;
    command[idx].read_result_index_1 = idx - 1;
    command[idx].read_result_index_2 = idx - 1;
    idx++;

    command[idx].operation = ALGO_WRITE;
    command[idx].key = 0 + MAX_DATA_PER_MACH;
    command[idx].read_result_index_1 = idx - 1;
    idx++;


    command[idx].operation = ALGO_SUB;
    command[idx].read_result_index_1 = idx - 3;
    command[idx].read_result_index_2 = -1;
    command[idx].imme_2 = 100;
    idx++;

    command[idx].operation = ALGO_WRITE;
    command[idx].key = 1 + MAX_DATA_PER_MACH;
    command[idx].read_result_index_1 = idx - 1;
    idx++;


    command[idx].operation = ALGO_READ;
    command[idx].key = 0 + MAX_DATA_PER_MACH;
    idx++;

    command[idx].operation = ALGO_READ;
    command[idx].key = 1 + MAX_DATA_PER_MACH;
    idx++;

    Transaction* transaction = new Transaction;
    for (int i = 0; i < idx; i++)transaction->commands.push_back(command[i]);

    return transaction;
}


// first server:
// result should be     500 500 0 400 400 500 400 500
//           then       1000 0 1000 2000 2000 900 900 2000 900

// the other result is  500 500 500 400 400 500 400 500
//           then       1000 2000 1000 2000 2000 900 900 2000 900

Transaction* self_made_benchmark_validation_txn(){
    Transaction *txn = new Transaction;

    int cmd_idx = 0;
    READ_KEY_VALUE(txn, cmd_idx, 0);
    READ_KEY_VALUE(txn, cmd_idx, 1);
    READ_KEY_VALUE(txn, cmd_idx, MAX_DATA_PER_MACH);
    READ_KEY_VALUE(txn, cmd_idx, 1 + MAX_DATA_PER_MACH);

    return txn;
}


bool self_made_benchmark_varify_rst(TransactionResult* txn_rst){
    return txn_rst->is_success && (txn_rst->results[0] == 400) && (txn_rst->results[1] == 500) &&
            (txn_rst->results[2] == 2000) && (txn_rst->results[3] == 900);
}
