#ifndef TATP_HPP
#define TATP_HPP

#include <thread>
#include <unistd.h>

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
	struct Hashtable<Subscriber> subscriber_table;
	long population;
};

#define PSIZE (1<<30)
#define VSIZE (1<<30)

#define POPULATION 100000

void tatpInit(struct TatpBenchmark* tatp, int population){
	tatp->population = population;

	tatp->subscriber_table = hash_table_init(1<<20);

	struct Subscriber* sr = (struct Subscriber*) PMALLOC(population*sizeof(struct Subscriber));
	for(int i=0; i<population; i++){
		TM_START(0);
		TM_STORE(&sr[i].s_id, i);
		TM_STORE(&sr[i].vlr_location, rand());

		#if INDEX_TYPE == BPLUSTREE_INDEX
		bplus_tree_put(tatp->subscriber_table, i, (long)&sr[i]);
		#elif INDEX_TYPE == HASHTABLE_INDEX
		hash_table_put(tatp->subscriber_table, i, (long)&sr[i]);
		#endif
		
		TM_COMMIT();
	}
}

inline
void updateLocation(struct TatpBenchmark* tatp, long s_id, long n_loc){

	struct Subscriber* sr = (struct Subscriber*)
#if INDEX_TYPE == BPLUSTREE_INDEX
	bplus_tree_get(tatp->subscriber_table, s_id);
#elif INDEX_TYPE == HASHTABLE_INDEX
	hash_table_get(tatp->subscriber_table, s_id);
#endif

	assert(sr);
	TM_STORE(&sr->vlr_location, n_loc);
}

int main(int argc, char** argv){
	global_init();

	TM_INIT();
	TM_INIT_THREAD();

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
			TM_INIT_THREAD();
			unsigned short seed[3];
			seed[0] = rand();
			seed[1] = rand();
			seed[2] = rand();

			while(done == 0){
				long s_id = (int)(erand48(seed)*tatp->population);
				long n_loc = (int)(erand48(seed)*0x7fffffff);
				TM_START(0);
				updateLocation(tatp, s_id, n_loc);
				TM_COMMIT();

				ops[x]++;
			}
			TM_EXIT_THREAD();

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

	TM_EXIT_THREAD();
	TM_EXIT();
}


#endif // TATP_HPP

	