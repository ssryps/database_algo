#include <iostream>
#include "OCC/Occ.h"
#include "Twopl/Twopl.h"
#include "pthread.h"
#include "MVCC/Mvcc.h"
#include "Timestamp/Timestamp.h"
#include <cstdlib>

Twopl* twoplServer;
OccServer* occServer;
MvccServer* mvccServer;
TimestampServer* timestampServer;

const int COMMMAND_PER_TRANSACTION = 2;
const int KEY_RANGE = 5;
const int VALUE_RANGE = 5;


Command generateCommand(){
    Command command;
    Operation operation = (rand() % 2 == 0? WRITE: READ);
    std::string key = std::to_string(rand() % KEY_RANGE), value = std::to_string(rand() % VALUE_RANGE);
    command.operation = operation;
    command.key = key;
    command.value = value;
    return command;
}

void* TwoplClient(void* args){
    long index = (long)args;
    Transaction *transaction = new Transaction;
    std::string outputString;
    for(int i = 0; i < COMMMAND_PER_TRANSACTION; i ++){
        Command command = generateCommand();
        transaction->commands.push_back(command);
        char buffer[1024];
        sprintf(buffer, "%ld> %s: , key: %s, value: %s \n", index, (command.operation == WRITE? "WRITE": "READ"), command.key.c_str(), command.value.c_str());
        outputString += std::string(buffer);
    }
    printf("%s", outputString.c_str());
    outputString.clear();
    TransactionResult result = twoplServer->handle(*transaction);
    for(auto i : result.results){
        char buffer[1024];
        sprintf(buffer, "%s ", i.c_str());
        outputString += std::string(buffer);
    }

    printf("%li result: %s \n", index, outputString.c_str());
}

void TwoplTest(){
    srand(time(NULL));
    const int thread_num = 4;
    twoplServer = new Twopl;
    pthread_t* threads = new pthread_t[thread_num];
    for(long i = 0; i < thread_num; i++){
        pthread_create(&threads[i], NULL, TwoplClient, (void*)i);
    }
    for(int i = 0; i < thread_num; i++){
        pthread_join(threads[i], NULL);
    }
    twoplServer->show();

}

void* OccClient(void* args){
    long index = (long)args;
    Transaction *transaction = new Transaction;
    std::string outputString;
    for(int i = 0; i < COMMMAND_PER_TRANSACTION; i ++){
        Command command = generateCommand();
        transaction->commands.push_back(command);
        char buffer[1024];
        sprintf(buffer, "%ld> %s: , key: %s, value: %s \n", index, (command.operation == WRITE? "WRITE": "READ"), command.key.c_str(), command.value.c_str());
        outputString += std::string(buffer);
    }
    printf("%s", outputString.c_str());
    outputString.clear();
    TransactionResult result = occServer->handle(*transaction);
    for(auto i : result.results){
        char buffer[1024];
        sprintf(buffer, "%s ", i.c_str());
        outputString += std::string(buffer);
    }

    printf("%li result: %s \n", index, outputString.c_str());
}

void OccTest(){
    srand(time(NULL));
    const int thread_num = 4;
    occServer = new OccServer;
    pthread_t* threads = new pthread_t[thread_num];
    for(long i = 0; i < thread_num; i++){
        pthread_create(&threads[i], NULL, OccClient, (void*)i);
    }
    for(int i = 0; i < thread_num; i++){
        pthread_join(threads[i], NULL);
    }
    occServer->show();

}

//
//void* MvccClient(void* args){
//    long index = (long)args;
//    Transaction *transaction = new Transaction;
//    std::string outputString;
//    for(int i = 0; i < COMMMAND_PER_TRANSACTION; i ++){
//        Command command = generateCommand();
//        transaction->commands.push_back(command);
//        char buffer[1024];
//        sprintf(buffer, "%ld> %s: , key: %s, value: %s \n", index, (command.operation == WRITE? "WRITE": "READ"), command.key.c_str(), command.value.c_str());
//        outputString += std::string(buffer);
//    }
//    printf("%s", outputString.c_str());
//    outputString.clear();
//    TransactionResult result = mvccServer->handle(*transaction);
//    for(auto i : result.results){
//        char buffer[1024];
//        sprintf(buffer, "%s ", i.c_str());
//        outputString += std::string(buffer);
//    }
//
//    printf("%li result: %s \n", index, outputString.c_str());
//}
//
//void MvccTest(){
//    srand(time(NULL));
//    const int thread_num = 4;
//    mvccServer = new MvccServer;
//    pthread_t* threads = new pthread_t[thread_num];
//    for(long i = 0; i < thread_num; i++){
//        pthread_create(&threads[i], NULL, MvccClient, (void*)i);
//    }
//    for(int i = 0; i < thread_num; i++){
//        pthread_join(threads[i], NULL);
//    }
//    occServer->show();
//
//}
//

void* TimeStampClient(void* args){
    long index = (long)args;
    Transaction *transaction = new Transaction;
    std::string outputString;
    for(int i = 0; i < COMMMAND_PER_TRANSACTION; i ++){
        Command command = generateCommand();
        transaction->commands.push_back(command);
        char buffer[1024];
        sprintf(buffer, "%ld> %s: , key: %s, value: %s \n", index, (command.operation == WRITE? "WRITE": "READ"), command.key.c_str(), command.value.c_str());
        outputString += std::string(buffer);
    }
    printf("%s", outputString.c_str());
    outputString.clear();
    TransactionResult result = timestampServer->handle(*transaction);
    for(auto i : result.results){
        char buffer[1024];
        sprintf(buffer, "%s ", i.c_str());
        outputString += std::string(buffer);
    }

    printf("%li result: %s \n", index, outputString.c_str());
}

void TimeStampTest(){
    srand(time(NULL));
    const int thread_num = 4;
    timestampServer = new TimestampServer;
    pthread_t* threads = new pthread_t[thread_num];
    for(long i = 0; i < thread_num; i++){
        pthread_create(&threads[i], NULL, TimeStampClient, (void*)i);
    }
    for(int i = 0; i < thread_num; i++){
        pthread_join(threads[i], NULL);
    }
    timestampServer->show();
}



int main() {
    //TwoplTest();
    //OccTest();
    //MvccTest();
    TimeStampTest();
    return 0;
}

