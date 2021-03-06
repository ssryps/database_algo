//
// Created by mason on 11/16/18.
//

#include <utils.h>
#include <assert.h>

const int COMMMAND_PER_TRANSACTION = 4;
const int KEY_RANGE = 3;
const int VALUE_RANGE = 5;


int benchmark_flag = 0;
#define RAMDOM_BENCHMARK 0
#define SELF_MADE_BENCHMARK 1
#define SMALL_BANK_BENCHMARK 2


Command generateRandomCommand();
void self_made_benchmark(Transaction *transaction);
void random_benchmark(Transaction *transaction);
void smallbank_benchmark(Transaction *transaction);

Transaction* generataTransaction(int id) {
    Transaction *transaction = new Transaction;

    switch (benchmark_flag) {
        case RAMDOM_BENCHMARK: {
            random_benchmark(transaction);
            break;
        }

        case SELF_MADE_BENCHMARK: {
            self_made_benchmark(transaction);
            break;
        }

        case SMALL_BANK_BENCHMARK: {

            break;
        }

        default: {
            assert(false);
            break;
        }
    }

    return transaction;
}

void self_made_benchmark(Transaction* transaction) {
    Command *command = new Command[100];
    int idx = 0;

    // first server:
    // result should be                     500 500 0 400 400 500 400 500
    //                              then    1000 0 1000 2000 2000 900 900 2000 900

    // another server, result should be     500 500 500 400 400 500 400 500
    //                              then    1000 2000 1000 2000 2000 900 900 2000 900

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

    for (int i = 0; i < idx; i++)transaction->commands.push_back(command[i]);

}

void random_benchmark(Transaction *transaction){

    char* output_buf = new char[COMMMAND_PER_TRANSACTION * 100];
    memset(output_buf, 0, COMMMAND_PER_TRANSACTION * 100 * sizeof(char));

    int offset = 0;
    for (int i = 0; i < COMMMAND_PER_TRANSACTION; i++) {
        Command command = generateRandomCommand();
        transaction->commands.push_back(command);
        offset += sprintf(output_buf + offset, "Txn > %s: key: %llu, imm1: %llu, index: %i, imm2: %llu, index: %i \n",
                          command.operation == ALGO_WRITE? "Write": "Read", command.key, command.imme_1,
                          command.read_result_index_1, command.imme_2, command.read_result_index_2);
    }

    printf(output_buf);
}

void smallbank_benchmark(Transaction *transaction){

}


Command generateRandomCommand(){
    Command command;
    Operation operation = (rand() % 2 == 0? ALGO_WRITE: ALGO_READ);
    idx_key_t key = rand() % KEY_RANGE + (rand() % CLIENT_THREAD_NUM) * MAX_DATA_PER_MACH;
    idx_value_t value = rand() % VALUE_RANGE;
    command.operation = operation;
    command.key = key;
    command.imme_1 = value;
    command.read_result_index_1 = -1;
    command.read_result_index_2 = -1;
    return command;
}





char* putBuffer(char* des, char* src, size_t sz){
    assert(sz > 0);
    for(int i = 0; i < sz; i ++){
        des[i] = src[i];
    }
    //des[sz] = '\t';
    return des + sz;
}

char* getBuffer(char* des, char* src, size_t sz){
    assert(sz > 0);
    for(int i = 0; i < sz; i ++){
        des[i] = src[i];
    }
}


size_t putTransactionToBuffer(Transaction* transaction, char* buf){
    char* start = buf;
    for(auto command: transaction->commands) {
        start = putBuffer(start, (char*)(&(command.operation)), sizeof(command.operation));
        start = putBuffer(start, (char*)(&(command.key)), sizeof(command.key));
        start = putBuffer(start, (char*)(&(command.imme_1)), sizeof(command.imme_1));
        start = putBuffer(start, (char*)(&(command.imme_2)), sizeof(command.imme_2));
        start = putBuffer(start, (char*)(&(command.read_result_index_1)), sizeof(command.read_result_index_1));
        start = putBuffer(start, (char*)(&(command.read_result_index_2)), sizeof(command.read_result_index_2));
        start = putBuffer(start, "\n", 1);
    }
    return start - buf;
}

Transaction* getTransactionFromBuffer(char* buf, size_t length){
    Transaction *transaction = new Transaction;
    char* start = buf;
    while(true){
        Command *command = new Command;
        start = getBuffer((char*)(&(command->operation)), start, sizeof(command->operation));
        start = getBuffer((char*)(&(command->key)), start, sizeof(command->key));
        start = getBuffer((char*)(&(command->imme_1)), start, sizeof(command->imme_1));
        start = getBuffer((char*)(&(command->imme_2)), start, sizeof(command->imme_2));
        start = getBuffer((char*)(&(command->read_result_index_1)), start, sizeof(command->read_result_index_1));
        start = getBuffer((char*)(&(command->read_result_index_2)), start, sizeof(command->read_result_index_2));
        char t;
        start = getBuffer(&t, start, 1);
        assert(t == '\n');
        transaction->commands.push_back(*command);
        if(start - buf > length - sizeof(Command) - 1)break;
    }

    return transaction;
}

size_t putResultToBuffer(TransactionResult result, char* buf){
    char *start = buf;
    for(auto re: result.results){
        start = putBuffer(start, reinterpret_cast<char*>(&(re)), sizeof(re));
        start = putBuffer(start, "\n", 1);
    }
    return start - buf;
}

TransactionResult* getResultFromBuffer(char* buf, size_t length){
    TransactionResult *result = new TransactionResult;
    char *start = buf;
    while(true){
        idx_value_t va;
        start = getBuffer(reinterpret_cast<char*>(&(va)), start, sizeof(va));
        char t;
        start = getBuffer(&t, start, 1);
        result->results.push_back(va);
        if(start - buf > length - sizeof(idx_value_t) - 1) break;
    }
    return result;

}
