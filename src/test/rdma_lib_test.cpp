#include <iostream>
#include <cassert>
#include <getopt.h>

//#include "../Network/wrap_rdma.hpp"

const int mem_size = 1 << 10;

void client_test() {

    char *l_dram_mem = new char[1024];

    WrapRdma *wrap_rdma = new WrapRdma(1, false);
    wrap_rdma->establish_conn(l_dram_mem, mem_size, 10085);

    std::fill(l_dram_mem, l_dram_mem + 31, 'a');
    l_dram_mem[31] = '\0'; 
    wrap_rdma->wrap_post_write_offset(l_dram_mem, 32, 0, 0);
    while(l_dram_mem[0] != 'b');
    printf("[Res]%s\n", l_dram_mem);

}

void server_all() {

    system("memcached -l 0.0.0.0 -p 10086 &");

    char *l_nvm_mem = new char[1024];

    WrapRdma *wrap_rdma = new WrapRdma(1, true);
    wrap_rdma->establish_conn(l_nvm_mem, mem_size, 10085);

    while(l_nvm_mem[0] != 'a');
    printf("[Res]%s\n", l_nvm_mem);
    std::fill(l_nvm_mem, l_nvm_mem + 31, 'b');
    l_nvm_mem[31] = '\0'; 
    wrap_rdma->wrap_post_write_offset(l_nvm_mem, 32, 0, 0);
}

int main(int argc, char **argv)
{

    int choose;
	char option;

	while((choose = getopt(argc, argv, "lfbhtri:s:")) != -1) {
		switch(choose) {
			case 'c':
				option = 'c';
				break;
			case 's':
				option = 's';
				break;
			case 'h':
				printf("usage: -[fbi[]h[server]s[?]]\n");
				break;
			default:
				printf("usage: -[fbi[]h[server]s[?]]\n");
				exit(1);
		}
	}

	if (option == 'c') {
		
	    client_test();

	} else if (option == 's') {

		server_all();
		
	} else {
		assert(-1);
	}

    /* code */
    return 0;
}