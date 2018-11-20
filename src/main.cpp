#include <cstdarg>
#include <cstdio>
#include <cstdlib>
#include <getopt.h>
#include "test/test_pthread.cpp"
//#include "test/test_rdma.cpp"

int main(int argc, char *argv[]) {

	int choose;
    char option;
	char *algo_name;
	while((choose = getopt(argc, argv, "a:")) != -1) {
		switch(choose) {
			case 'a': {
                algo_name = optarg;
                break;
            }
			default: {
                printf("USAGE: -a algo_name\n");
                exit(0);
            }
		}
	}
    if(!algo_name) {
        printf("USAGE: -a algo_name\n");
        exit(0);
    }

	if (strcmp(algo_name, "twopl") == 0 ) {
		#ifdef RDMA
				RdmaTest(argc, argv, ALGO_TWOPL);
		#else
				PthreadTest(argc, argv, ALGO_TWOPL);
		#endif

	} else if(strcmp(algo_name, "occ") == 0){
		#ifdef RDMA
				RdmaTest(argc, argv, ALGO_OCC);
		#else
				PthreadTest(argc, argv, ALGO_OCC);
		#endif

	} else if(strcmp(algo_name, "mvcc") == 0) {
		#ifdef RDMA
				RdmaTest(argc, argv, ALGO_MVCC);
		#else
				PthreadTest(argc, argv, ALGO_MVCC);
		#endif

	} else if(strcmp(algo_name, "timestamp") == 0) {
		#ifdef RDMA
				RdmaTest(argc, argv, ALGO_TIMESTAMP);
		#else
				PthreadTest(argc, argv, ALGO_TIMESTAMP);
		#endif

	}

    return 0;
}

