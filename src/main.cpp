#include "rdma_test.cpp"
#include "Twopl.h"
#include "Occ.h"
#include "Timestamp.h"
#include "Mvcc.hpp"
#include <cstdarg>
#include <cstdio>
#include <cstdlib>
#include <getopt.h>

extern void RdmaTwoplTest(int, char*[]);

int main(int argc, char *argv[]) {
    //MvccTest();
    //TimeStampTest();

	int choose;
    char option;
	char *algo_name;
	while((choose = getopt(argc, argv, "a:")) != -1) {
		switch(choose) {
			case 'a':
				algo_name = optarg;
				break;
			default:
				printf("USAGE: XXXX\n");
				exit(1);
		}
	}

	if (algo_name == "2PL") {

	    RdmaTwoplTest(argc, argv);
	    // TwoplTest();
	} else if (algo_name == "OCC") {

	    // OccTest();
	} else {
		// TODO
	}

    return 0;
}

