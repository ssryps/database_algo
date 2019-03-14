#ifndef DEFINES_H
#define DEFINES_H

#include <sys/socket.h>

typedef uint64_t idx_key_t; // key id for index
typedef int64_t idx_value_t;
typedef int idx_index_t;
typedef int idx_trans_t;


#ifdef RDMA

#else
    typedef int comm_identifer;
    typedef sockaddr comm_addr;
    typedef socklen_t comm_length;
#endif


enum Data_type {DT_table, DT_page, DT_row };

class itemid_t {
public:
	itemid_t() { };
	itemid_t(Data_type type, void * loc) {
        this->type = type;
        this->location = loc;
    };
	Data_type type;
	void * location; // points to the table | page | row
	itemid_t * next;
	bool valid;
	void init();
	bool operator==(const itemid_t &other) const;
	bool operator!=(const itemid_t &other) const;
	void operator=(const itemid_t &other);
};

#endif // DEFINES_H
