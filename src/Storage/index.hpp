#ifndef INDEX_HPP
#define INDEX_HPP

#include <iostream>
#include <memory>

#include "utils.h"
#include "defines.h"


template<typename T>
class Index {
public:
	typedef std::shared_ptr<T> T_ptr;
	virtual bool			init();
	virtual bool 			init(uint64_t size);

	virtual bool 			index_exist(idx_key_t key); // check if the key exist.

	virtual bool 			index_put(idx_key_t key, T_ptr item);

	virtual bool 			index_put(idx_key_t key, T_ptr item, int part_id);

	virtual bool	 		index_get(idx_key_t key, T_ptr &item);

	virtual bool	 		index_get(idx_key_t key, T_ptr &item,int part_id);
	
	// TODO implement index_remove
	virtual bool 			index_remove(idx_key_t key);
	
	// the index in on "table". The key is the merged key of "fields"
	// table_t * 			table;
};

#endif // INDEX_HPP
