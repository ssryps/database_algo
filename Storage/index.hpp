#ifndef INDEX_HPP
#define INDEX_HPP

#include "utils.h"
#include "defines.h"

class index_base {
public:
	virtual  			init() { return OK; };
	virtual RC 			init(uint64_t size) { return OK; };

	virtual bool 		index_exist(idx_key_t key)=0; // check if the key exist.

	virtual RC 			index_put(idx_key_t key, 
							itemid_t * item, 
							int part_id=-1)=0;

	virtual RC	 		index_get(idx_key_t key, 
							itemid_t * &item,
							int part_id=-1)=0;
	
	// TODO implement index_remove
	virtual RC 			index_remove(idx_key_t key) { return OK; };
	
	// the index in on "table". The key is the merged key of "fields"
	table_t * 			table;
};

#endif // INDEX_HPP
