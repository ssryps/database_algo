//
// Created by mason on 11/16/18.
//

#include <utils.h>
#include <assert.h>

const int COMMMAND_PER_TRANSACTION = 4;
const int KEY_RANGE = 5;
const int VALUE_RANGE = 5;

Command generateCommand(){
    Command command;
    Operation operation = (rand() % 2 == 0? WRITE: READ);
    idx_key_t key = rand() % KEY_RANGE;
    idx_value_t value = rand() % VALUE_RANGE;
    command.operation = operation;
    command.key = key;
    command.imme_1 = value;
    command.read_result_index_1 = -1;
    command.read_result_index_2 = -1;
    return command;
}

Transaction* generataTransaction() {
    Transaction *transaction = new Transaction;
#ifdef DEBUG
    char* output_buf = new char[COMMMAND_PER_TRANSACTION * 100];
    memset(output_buf, 0, COMMMAND_PER_TRANSACTION * 100 * sizeof(char));
#endif
    for (int i = 0; i < COMMMAND_PER_TRANSACTION; i++) {
        Command command = generateCommand();
        transaction->commands.push_back(command);
#ifdef DEBUG
        sprintf(output_buf, "%ld> %llu: key: %llu, imm1: %llu, index: %i, imm2: %llu, index: %i \n", i, command.operation, command.key,
                command.imme_1, command.read_result_index_1, command.imme_2, command.read_result_index_2);
#endif
    }

#ifdef DEBUG
    printf(output_buf);
#endif
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
        start = putBuffer(start, reinterpret_cast<char*>(&(command.operation)), sizeof(command.operation));
        start = putBuffer(start, reinterpret_cast<char*>(&(command.key)), sizeof(command.key));
        start = putBuffer(start, reinterpret_cast<char*>(&(command.imme_1)), sizeof(command.imme_1));
        start = putBuffer(start, reinterpret_cast<char*>(&(command.imme_2)), sizeof(command.imme_2));
        start = putBuffer(start, reinterpret_cast<char*>(&(command.read_result_index_1)), sizeof(command.read_result_index_1));
        start = putBuffer(start, reinterpret_cast<char*>(&(command.read_result_index_2)), sizeof(command.read_result_index_2));
        start = putBuffer(start, "\n", 1);
    }
    return start - buf;
}

Transaction getTransactionFromBuffer(char* buf, size_t length){
    Transaction *transaction = new Transaction;
    char* start = buf;
    while(true){
        Command *command = new Command;
        start = getBuffer(reinterpret_cast<char*>(&(command->operation)), start, sizeof(command->operation));
        start = getBuffer(reinterpret_cast<char*>(&(command->key)), start, sizeof(command->key));
        start = getBuffer(reinterpret_cast<char*>(&(command->imme_1)), start, sizeof(command->imme_1));
        start = getBuffer(reinterpret_cast<char*>(&(command->imme_2)), start, sizeof(command->imme_2));
        start = getBuffer(reinterpret_cast<char*>(&(command->read_result_index_1)), start, sizeof(command->read_result_index_1));
        start = getBuffer(reinterpret_cast<char*>(&(command->read_result_index_2)), start, sizeof(command->read_result_index_2));
        char t;
        start = getBuffer(&t, start, 1);
        assert(t == '\n');
        transaction->commands.push_back(*command);
        if(start - buf > length - sizeof(Command) - 1)break;
    }

    return *transaction;
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
