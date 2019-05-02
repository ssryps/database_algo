#include <cstdarg>
#include <cstdio>
#include <cstdlib>
#include <argp.h>
#include "test/test_framework.cpp"


char *algo_name;
int benchmark_idx;

extern int server_thread_num;
extern int client_thread_num;

const char *doc = "USAGE: -a algo_name -b benchmark_idx -c client_num -s server_num";
argp_option options[] = {
		{"algo", 'a', "", 0, "CC algorithm to use, four options are twopl, occ, mvcc, timestamp", 0},
		{"bench",'b', "bench_idx", 0, "benchmark to use, 0 for random benchmark, 1 for a specific case, 2 for smallbank", 0},
		{"client",'c', "client_number", 0, "client number", 0},
		{"server",'s', "server_number", 0, "server number", 0},
		{ 0, 0, 0, 0, 0, 0 }
};

error_t parseArgs(int key, char *arg, struct argp_state*) {
	switch (key) {
		case 'a':
			if (!arg) {
				std::cerr << "ERROR: Wrong argument for -a, algorithm name is required" << std::endl;
				exit(-1);
			} else {
				algo_name = arg;
			}
			break;
        case 'b':
            if (!arg) {
            	benchmark_idx = SMALLBANK_BENCHMARK;
            } else {
				benchmark_idx = atoi(arg);
			}

            break;

		case 'c':
			if (!arg) {
				client_thread_num = DEFAULT_CLIENT_NUM;
			} else {
				client_thread_num = atoi(arg);
			}
			break;
		case 's':
			if (!arg) {
				server_thread_num = DEFAULT_SERVER_NUM;
			} else {
				server_thread_num = atoi(arg);
			}
			break;
		default:
			return ARGP_ERR_UNKNOWN;
	}
	return 0;
}

int main(int argc, char *argv[]) {

	argp argpConf = { options, parseArgs, NULL, doc, NULL, NULL, NULL };
	argp_parse(&argpConf, argc, argv, ARGP_NO_ARGS, NULL, NULL);
	if(algo_name == NULL){
		std::cout << doc << std::endl;
		exit(0);
	}
	std::cout << "using " << algo_name << "algorithm, " << benchmark_name[benchmark_idx] << " benchmark " << std::endl;

	if (strcmp(algo_name, "twopl") == 0 ) {
		#ifdef RDMA
				RdmaTest(argc, argv, ALGO_TWOPL);
        #else
			if(benchmark_idx < 0) {
				PthreadTest(ALGO_TWOPL);
			} else {
				PthreadTest(ALGO_TWOPL, benchmark_idx);
			}
		#endif

	} else if(strcmp(algo_name, "occ") == 0){
		#ifdef RDMA
				RdmaTest(argc, argv, ALGO_OCC);
		#else
			PthreadTest(ALGO_OCC, benchmark_idx);
		#endif

	} else if(strcmp(algo_name, "mvcc") == 0) {
#ifdef RDMA
				RdmaTest(argc, argv, ALGO_MVCC);
#else
		if(benchmark_idx < 0) {
			PthreadTest(ALGO_MVCC);
		} else {
			PthreadTest(ALGO_MVCC, benchmark_idx);
		}
#endif

	} else if(strcmp(algo_name, "timestamp") == 0) {
#ifdef RDMA
		RdmaTest(ALGO_TIMESTAMP);
#else
		if(benchmark_idx < 0) {
			PthreadTest(ALGO_TIMESTAMP);
		} else {
			PthreadTest(ALGO_TIMESTAMP, benchmark_idx);
		}
#endif

	}



    return 0;
}

