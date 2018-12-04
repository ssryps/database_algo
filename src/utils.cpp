//
// Created by mason on 11/20/18.
//
#include "utils.h"

idx_value_t value_from_command(Command command, idx_value_t* temp_result){
    idx_value_t result = 0;

    if(command.operation == WRITE) {
        if (command.read_result_index_1 < 0)result += command.imme_1;
        else result += temp_result[command.read_result_index_1];
    }
    if(command.operation == ALGO_ADD){
        if (command.read_result_index_1 < 0)result += command.imme_1;
        else result += temp_result[command.read_result_index_1];

        if (command.read_result_index_2 < 0)result += command.imme_2;
        else result += temp_result[command.read_result_index_2];
    }

    if(command.operation == ALGO_SUB){
        if (command.read_result_index_1 < 0)result += command.imme_1;
        else result += temp_result[command.read_result_index_1];

        if (command.read_result_index_2 < 0)result -= command.imme_2;
        else result -= temp_result[command.read_result_index_2];
    }
    return result;
}


int get_machine_index(idx_key_t key){
    return (key % (MAX_DATA_PER_MACH * SERVER_THREAD_NUM)) / MAX_DATA_PER_MACH;
}

