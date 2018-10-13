//#include "rdma_test/rdma_twopl.cpp"

extern void RdmaTwoplTest(int, char*[]);

int main(int args, char *argv[]) {
    //TwoplTest();
    //OccTest();
    //MvccTest();
    //TimeStampTest();
    //
    RdmaTwoplTest(args, argv);
    return 0;
}

