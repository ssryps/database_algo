#ifndef DEFINES_H
#define DEFINES_H

typedef uint64_t idx_key_t; // key id for index
typedef uint64_t idx_value_t;


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
