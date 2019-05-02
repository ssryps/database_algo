//
// Created by mason on 11/16/18.
//

#include <utils.h>
#include <assert.h>
#include "BenchMark/random_txn.cpp"
//#include "BenchMark/tatp.cpp"
#include "BenchMark/specific_example.cpp"
#include "BenchMark/smallbank.cpp"


int benchmark_flag ;
#define RAMDOM_BENCHMARK 0
#define SELF_MADE_BENCHMARK 1
#define SMALLBANK_BENCHMARK 2

const char* benchmark_name[] = {"random benchmark", "self-made", "smallbank"};


extern void self_made_benchmark_init();
extern Transaction* self_made_benchmark(int id);
extern Transaction* self_made_benchmark_validation_txn();
extern bool self_made_benchmark_varify_rst(TransactionResult* rst);


extern void random_benchmark_init();
extern Transaction* random_benchmark(int id);
extern Transaction* random_benchmark_validation_txn();
extern bool random_benchmark_varify_rst(TransactionResult* rst);


extern void smallbank_benchmark_init();
extern Transaction* smallbank_benchmark(int id);
extern Transaction* smallbank_benchmark_validation_txn();
extern bool smallbank_benchmark_varify_rst(TransactionResult* rst);


void benchmark_init(int flag);
Transaction* benchmark_txns(int id);
Transaction* benchmark_validation_txn();
bool benchmark_varify_rst(TransactionResult* rst);

void txn_print(Transaction* txn);
void txn_rst_print(TransactionResult* result);

void benchmark_init(int flag) {
    benchmark_flag = flag;

    switch (benchmark_flag) {
        case RAMDOM_BENCHMARK: {
            random_benchmark_init();
            break;
        }

        case SELF_MADE_BENCHMARK: {
            self_made_benchmark_init();
            break;
        }

        case SMALLBANK_BENCHMARK: {
            smallbank_benchmark_init();
            break;
        }

        default: {
            assert(false);
            break;
        }
    }
}



Transaction* benchmark_txns(int id) {
    Transaction *txn;

    switch (benchmark_flag) {
        case RAMDOM_BENCHMARK: {
            txn = random_benchmark(id);
            break;
        }

        case SELF_MADE_BENCHMARK: {
            txn = self_made_benchmark(id);
            break;
        }

        case SMALLBANK_BENCHMARK: {
            txn = smallbank_benchmark(id);
            break;
        }

        default: {
            assert(false);
            break;
        }
    }
#ifdef DEBUG
    txn_print(txn);
#endif

    return txn;
}

Transaction* benchmark_validation_txn() {
    Transaction *txn;
    switch (benchmark_flag) {
        case RAMDOM_BENCHMARK: {
            txn = random_benchmark_validation_txn();
            break;
        }

        case SELF_MADE_BENCHMARK: {
            txn = self_made_benchmark_validation_txn();
            break;
        }

        case SMALLBANK_BENCHMARK: {
            txn = smallbank_benchmark_validation_txn();
            break;
        }

        default: {
            assert(false);
            break;
        }
    }
    return txn;
}


bool benchmark_varify_rst(TransactionResult* txn_rst){
    switch (benchmark_flag) {
        case RAMDOM_BENCHMARK: {
            return random_benchmark_varify_rst(txn_rst);
        }

        case SELF_MADE_BENCHMARK: {
            return self_made_benchmark_varify_rst(txn_rst);
        }

        case SMALLBANK_BENCHMARK: {
            return smallbank_benchmark_varify_rst(txn_rst);
        }

        default: {
            assert(false);
            break;
        }
    }

}



char output_buf[10000];

void txn_print(Transaction* txn){
    memset(output_buf, 0, 1000 * sizeof(char));
    int offset = 0;

    offset += sprintf(output_buf + offset, "Txn: \n");

    for(int i = 0; i < txn->commands.size(); i++){
        offset += sprintf(output_buf + offset, "    ");

        Command c = txn->commands[i];
        if(c.operation == ALGO_READ){
            offset += sprintf(output_buf + offset, "Read key: %lu \n", c.key);
        }

        if(c.operation == ALGO_WRITE){
            if(c.read_result_index_1 < 0){
                offset += sprintf(output_buf + offset, "Write key: %lu with imm1: %lu \n", c.key, c.imme_1);
            } else {
                offset += sprintf(output_buf + offset, "Write key: %lu with %i's operation result \n", c.key,
                        c.read_result_index_1);
            }

        }

        if(c.operation == ALGO_ADD){
            offset += sprintf(output_buf + offset, "Add two value: ");
            if(c.read_result_index_1 < 0) {
                offset += sprintf(output_buf + offset, "imm1 %lu ", c.imme_1);
            } else {
                offset += sprintf(output_buf + offset, "%i's operation result", c.read_result_index_1);
            }

            offset += sprintf(output_buf + offset, " + ");

            if(c.read_result_index_2 < 0) {
                offset += sprintf(output_buf + offset, "imm2 %lu \n", c.imme_2);
            } else {
                offset += sprintf(output_buf + offset, "%i's operation result \n", c.read_result_index_1);
            }

        }

        if(c.operation == ALGO_SUB){
            offset += sprintf(output_buf + offset, "Sub two value: ");
            if(c.read_result_index_1 < 0) {
                offset += sprintf(output_buf + offset, "imm1 %lu", c.imme_1);
            } else {
                offset += sprintf(output_buf + offset, "%i's operation result ", c.read_result_index_1);
            }

            offset += sprintf(output_buf + offset, " - ");

            if(c.read_result_index_2 < 0) {
                offset += sprintf(output_buf + offset, "imm2 %lu \n", c.imme_2);
            } else {
                offset += sprintf(output_buf + offset, "%i's operation result \n", c.read_result_index_1);
            }

        }

        if(c.operation == ALGO_TIMES){
            offset += sprintf(output_buf + offset, "Multiply two value: ");
            if(c.read_result_index_1 < 0) {
                offset += sprintf(output_buf + offset, "imm1 %lu ", c.imme_1);
            } else {
                offset += sprintf(output_buf + offset, "%i's operation result", c.read_result_index_1);
            }

            offset += sprintf(output_buf + offset, " * ");

            if(c.read_result_index_2 < 0) {
                offset += sprintf(output_buf + offset, "imm2 0.%lu \n", c.imme_2);
            } else {
                offset += sprintf(output_buf + offset, "%i's operation result \n", c.read_result_index_1);
            }

        }
    }

    printf(output_buf);
}

void txn_rst_print(TransactionResult* rst){
    int offset = 0;
    char* output_buf = new char[1000];
    offset += sprintf(output_buf + offset, "DB state: \n");
    for (auto i : rst->results)
        offset += sprintf(output_buf + offset, "    %ld", i);
    sprintf(output_buf + offset, "\n");

    printf(output_buf);
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
