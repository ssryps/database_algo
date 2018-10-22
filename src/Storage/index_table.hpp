#ifndef INDEX_TABLE_HPP
#define INDEX_TABLE_HPP

#include <unordered_map>
#include <ext/hash_map>
#include "index.hpp"

template<typename T>
class TableIndex: Index<T> {
public:
    typedef std::shared_ptr<T> T_ptr;

    bool init() { return OK; }
    bool init(uint64_t size) {
        table_index_ins.resize(size);
        return true;
    }

    bool index_exist(idx_key_t index) {
        return index < table_index_ins.size() && index >= 0;
    }

    bool index_put(idx_key_t index, T_ptr item) {
        table_index_ins[index] = item;
        return true;
    }

    bool index_get(idx_key_t index, T_ptr &item) {
        item = table_index_ins[index];
        return true;
    }

    bool index_new(T_ptr item, idx_key_t* index){
        table_index_ins.push_back(item);
        *index = table_index_ins.size() - 1;
        return true;
    }

private:
    std::vector<T_ptr> table_index_ins;

};

#endif // INDEX_HASHTABLE_HPP
