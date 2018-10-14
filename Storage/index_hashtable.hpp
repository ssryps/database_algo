#ifndef INDEX_HASHTABLE_HPP
#define INDEX_HASHTABLE_HPP

#include <unordered_map>
#include <ext/hash_map>
#include "index.hpp"

template<typename T>	
class HashTableIndex: Index<T> {
public:
	typedef std::shared_ptr<T> T_ptr;

	bool init() { return OK; }
	bool init(uint64_t size) { 
		hash_index_ins.resize(size);
		return true;
	}

	bool index_exist(idx_key_t key) {
		auto res = hash_index_ins.find(key);
		if (res == hash_index_ins.end()) {
			return false;
		}
		return true;
	}

	bool index_put(idx_key_t key, T_ptr item) {
		hash_index_ins[key] = item;
		return true;
	}

	bool index_get(idx_key_t key, T_ptr &item) {
		item = hash_index_ins[key];
		return true;
	}
	
private:
	__gnu_cxx::hash_map<idx_key_t, itemid_t> hash_index_ins;

};

#endif // INDEX_HASHTABLE_HPP
