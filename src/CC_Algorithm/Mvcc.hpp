//
// Created by mason on 10/1/18.
//

#ifndef RDMA_MEASURE_MVCC_HPP
#define RDMA_MEASURE_MVCC_HPP


#include <map>
#include <list>
#include <atomic>
#include <Storage/index_table.hpp>
#include "utils.h"
#include "defines.h"
#include "index_hashtable.hpp"
#include "CCServer.hpp"



struct MvccEntry {
    long id;
    long start, end;
    long next;
    idx_value_t content;
};

static std::atomic<int> cur(0);

class MvccServer : public CCServer{
public:
    MvccServer(){}
    bool init(int id, char* buf, int buf_sz){
        this->id = id;
        this->buf = buf;
        database_init =  buf;
        data_mutex_init = buf + buf_sz / 3;
        index_init = buf + buf_sz * 2 / 3;
        database.init(database_init);
        data_mutex.init(data_mutex_init);
        index_database.init(index_init);
        return OK;
    }

    TransactionResult handle(Transaction transaction){
        TransactionResult results;
        int start_time = getCurrentTimeStamp();

        std::vector<long> lasterCheck;
        for (Command command : transaction.commands) {
            if (command.operation == WRITE) {
                int key = command.key, key_start_pos;
                acquire_lock(key);
                get_key_start(command.key, &key_start_pos);
                if(key_start_pos == -1){
                    MvccEntry *newEntry = new MvccEntry{
                            start_time, start_time, INT32_MAX, -1, command.value
                    };
                    int idx;
                    new_entry(key, &idx);
                    put_entry(key, idx, newEntry);
                } else {
                    int key_cur_pos = key_start_pos;
                    while (key_cur_pos != -1) {
                        MvccEntry mvcc_entry;
                        get_entry(key, key_cur_pos, &mvcc_entry);
                        if (mvcc_entry.next == -1) {
                            if (mvcc_entry.start == start_time) {
                                /*
                                 * so the entry is created by me, just overwrite it
                                 */
                                MvccEntry *entry = new MvccEntry{
                                    mvcc_entry.id, mvcc_entry.start, mvcc_entry.end, mvcc_entry.next, command.value
                                };
                                put_entry(key, key_cur_pos, entry);
                            } else if (mvcc_entry.start < start_time && mvcc_entry.id == -1) {
                                /*
                                 * add a serial record
                                 */
                                MvccEntry *entry= new MvccEntry{
                                        start_time, start_time, INT32_MAX, -1, command.value
                                };
                                int next_idx;
                                new_entry(key, &next_idx);
                                put_entry(key, next_idx, entry);
                                MvccEntry *old_entry = new MvccEntry{
                                        mvcc_entry.id, mvcc_entry.start, next_idx, mvcc_entry.next, mvcc_entry.value
                                };
                                put_entry(key, key_cur_pos, old_entry);
                            } else {
                                results.is_success = false;
                                return results;
                            }
                            break;
                        }
                        key_cur_pos = mvcc_entry.next;
                    }
                }
                release_lock(key);
            }
            if (command.operation == READ) {
                int key = command.key, key_start_pos;
                acquire_lock(key);
                get_key_start(key, &key_start_pos);
                if(key_start_pos == -1)results.results.push_back(NULL);
                else {
                    int key_cur_pos = key_start_pos;
                    while (key_cur_pos != -1) {
                        MvccEntry mvcc_entry;
                        get_entry(key, key_cur_pos, &mvcc_entry);
                        if (mvcc_entry.start <= start_time && start_time < mvcc_entry.end) {
                            results.results.push_back(mvcc_entry.content);
                            if (mvcc_entry.id != start_time && mvcc_entry.id != -1) {
                                lasterCheck.push_back(mvcc_entry.id);
                            }
                            break;
                        }
                        key_cur_pos = mvcc_entry.next;
                    }
                }
                release_lock(key);
            }
        }
    }

private:
    int id;
    char* buf;
    char *database_init, *data_mutex_init, *index_init;
    // store the data which rmda will access directly
    TableIndex<MvccEntry> database;
    TableIndex<bool> data_mutex;
    TableIndex<idx_index_t> index_database;


    bool acquire_lock(idx_key_t key){
        return true;
    }

    bool release_lock(idx_key_t key){
        return true;
    }
    // all the following functions should acquire the lock first
    bool get_key_start(idx_key_t key, idx_index_t *index){
        return true;
    }

    bool put_key_start(idx_key_t key, idx_index_t index){
        return true;
    }

    bool get_entry(idx_key_t key, idx_index_t index, MvccEntry* entry){
        return true;
    }

    bool put_entry(idx_key_t key, idx_index_t pos, MvccEntry* entry){
        return true;
    }

    bool new_entry(idx_key_t key, idx_index_t* pos){
        // need to do compare and swap
        return true;
    }

    int getCurrentTimeStamp(){
        // this functions is currently local, and need to be extended to global time later

        int oldValue, newValue;
        do {
            oldValue = cur.load(std::memory_order_relaxed);
            newValue = oldValue + 1;
        } while (!std::atomic_compare_exchange_weak(&cur, &oldValue, newValue));
        return newValue;
    }
};


#endif //RDMA_MEASURE_MVCC_HPP
