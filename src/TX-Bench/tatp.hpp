#ifndef TATP_HPP
#define TATP_HPP

#include <thread>
#include <unistd.h>
#include "../Storage/index_hashtable.hpp"

struct Subscriber{
	long s_id;
	long sub_nbr[2];  // char[15]
	long bit_x;   // bool[10]   0-9 bit is used;
	long hex_x;   	  // bit[10][4]
	long byte2_x[2];   // char[10]
	long msc_location;  //  int 
	long vlr_location;  //  int
};

struct TatpBenchmark{
	class HashTableIndex<Subscriber> subscriber_table;
	long population;
};

#define PSIZE (1<<30)
#define VSIZE (1<<30)
#define POPULATION 100000

#define TX_START(num) do{}while(0);
#define TX_STORE(ptr, val) { \
	*ptr = val;\
}; 
#define TX_COMMIT() do{}while(0);

void tatpInit(struct TatpBenchmark* tatp, int population){
	tatp->population = population;

	tatp->subscriber_table = HashTableIndex();
	tatp->subscriber_table.init();

	struct Subscriber* sr = (struct Subscriber*) malloc(population*sizeof(struct Subscriber));
	for(int i=0; i<population; i++){
		TX_START(0);

		TX_STORE(&sr[i].s_id, i);
		TX_STORE(&sr[i].vlr_location, rand());

		// hash_table_put(tatp->subscriber_table, i, (long)&sr[i]);
		tatp->subscriber_table.index_put(i, (long)&sr[i]);
		
		TX_COMMIT();
	}
}

inline
void updateLocation(struct TatpBenchmark* tatp, long s_id, long n_loc){

	struct Subscriber* sr;
	tatp->subscriber_table.index_get(s_id, &sr);

	assert(sr);
	TX_STORE(&sr->vlr_location, n_loc);
}

int main(int argc, char** argv){
	global_init();

	TX_INIT();
	TX_INIT_THREAD();

	struct TatpBenchmark* tatp = new TatpBenchmark;
	tatpInit(tatp, POPULATION);

	volatile int done = 0;
	int nthreads = 2;
	if (argc > 1)
		nthreads = atoi(argv[1]);
	std::thread pid[nthreads];
	unsigned long ops[nthreads];
	for(int i=0; i<nthreads; i++){
		ops[i] = 0;
		pid[i] = std::thread([&](int x){
			dune_enter();
			TX_INIT_THREAD();
			unsigned short seed[3];
			seed[0] = rand();
			seed[1] = rand();
			seed[2] = rand();

			while(done == 0){
				long s_id = (int)(erand48(seed)*tatp->population);
				long n_loc = (int)(erand48(seed)*0x7fffffff);
				TX_START(0);
				updateLocation(tatp, s_id, n_loc);
				TX_COMMIT();

				ops[x]++;
			}
			TX_EXIT_THREAD();

		}, i);
	}

	double begin = get_wall_time();
	sleep(5);
	done = 1;
	unsigned long total = 0;
	for(int i=0; i<nthreads; i++){
		pid[i].join();
		total += ops[i];
	}
	double end = get_wall_time();
	double del = end - begin; 
	printf("total time: %.4lf, throughput: %.4lf Mtps\n",del, (double)(total)/1000000/del);

	TX_EXIT_THREAD();
	TX_EXIT();
}


#endif // TATP_HPP

	