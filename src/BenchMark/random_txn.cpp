//
// Created by mason on 5/2/19.
//

#include <utils.h>

#define TXN_COMMAND_NUM 4
#define KEY_RANGE 3
#define VALUE_RANGE 5

void random_benchmark_init();
Transaction* random_benchmark(int id);
Transaction* random_benchmark_validation_txn();
bool random_benchmark_varify_rst(TransactionResult* rst);


#define READ_KEY_VALUE(txn, cmd_idx, k) \
{ \
    Command c; \
    c.operation = ALGO_READ; \
    c.key = k; \
    txn->commands.push_back(c);\
    cmd_idx ++; \
}
extern bool random_benchmark_varify_rst(TransactionResult* rst);

#define GENERATE_RANDOM_CMD(txn, cmd_idx, k) { \
    Command c; \
    Operation operation = (rand() % 2 == 0? ALGO_WRITE: ALGO_READ); \
    idx_key_t key = rand() % KEY_RANGE + (rand() % client_thread_num) * MAX_DATA_PER_MACH; \
    idx_value_t value = rand() % VALUE_RANGE; \
    c.operation = operation; \
    c.key = key; \
    c.imme_1 = value; \
    c.read_result_index_1 = -1; \
    c.read_result_index_2 = -1; \
    txn->commands.push_back(c); \
    cmd_idx ++; \
}

void random_benchmark_init(){
    srand(time(NULL));
}

Transaction* random_benchmark(int id){
    Transaction *txn = new Transaction;
    int cmd_idx = 0;
    for (int i = 0; i < TXN_COMMAND_NUM; i++) {
        GENERATE_RANDOM_CMD(txn, cmd_idx, i);
    }
    return txn;
}

Transaction* random_benchmark_validation_txn(){
    Transaction *txn = new Transaction;
    int cmd_idx = 0;
    for(int i = 0; i < client_thread_num; i++){
        for(int j = 0; j < KEY_RANGE; j++){
            int key = i * MAX_DATA_PER_MACH + j;
            READ_KEY_VALUE(txn, cmd_idx, key);
        }
    }
    return txn;
}

bool random_benchmark_varify_rst(TransactionResult* rst){
    return true;
}

