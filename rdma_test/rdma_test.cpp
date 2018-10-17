//
// Created by mason on 10/09/18.
//

#include "../Twopl/Twopl.h"
#include "../Occ/Occ.h"
#include "../Mvcc/Mvcc.h"
#include "../Timestamp/Timestamp.h"
#include "rdma_utils.h"


void handleTransaction(struct ibv_qp *qp, struct ibv_mr *mr, struct ibv_cq *txcq, char *txbuf, size_t txbufsize, TwoplServer* twoplServer, Transaction transaction){
    TransactionResult result = twoplServer->handle(transaction);
    size_t t = putResultToBuffer(result, txbuf);
    ibPostSendAndWait(qp, mr, txbuf, t, txcq);
}

static void RdmaClient(struct ibv_qp *qp, struct ibv_mr *mr, struct ibv_cq *rxcq, char *rxbuf, size_t rxbufsize, struct ibv_cq *txcq, char *txbuf, size_t txbufsize)
{
    Transaction *transaction = new Transaction;
    std::string outputString;
    srand(time(NULL));
    for(int i = 0; i < COMMMAND_PER_TRANSACTION; i ++){
        Command command = generateCommand();
        transaction->commands.push_back(command);
        char buffer[1024];
        sprintf(buffer, " %s: , key: %s, value: %s \n", (command.operation == WRITE? "WRITE": "READ"), command.key.c_str(), command.value.c_str());
        outputString += std::string(buffer);
    }
    printf("%s", outputString.c_str());

    int size = putTransactionToBuffer(transaction, txbuf);
    ibPostSendAndWait(qp, mr, txbuf, size, txcq);
    struct ibv_wc iwc;
    while (ibv_poll_cq(rxcq, 1, &iwc) < 1)
        ;
    std::cout << "GET RESULT" << std::endl;
    if (iwc.status != IBV_WC_SUCCESS) {
        fprintf(stderr, "ibv_poll_cq returned failure\n");
        exit(1);
    }
    TransactionResult result = getResultFromBuffer(rxbuf, iwc.byte_len);
    for(auto i : result.results){
        std::printf("result: %s \n", i.c_str());
    }

}



static void RdmaServer(struct ibv_qp *qp, struct ibv_mr *mr, struct ibv_cq *rxcq, char *rxbuf, size_t rxbufsize,
        struct ibv_cq *txcq, char *txbuf, size_t txbufsize, char* serverName)
{
    Server *server;
    if(strcmp(serverName, "twopl") == 0) {
        server = new TwoplServer;
    } else if(strcmp(serverName, "occ") == 0) {
        server = new OccServer;
    } else if(strcmp(serverName, "mvcc") == 0) {
        server = new MvccServer;
    } else if(strcmp(serverName, "timestamp") == 0) {
        server = new TimestampServer;
    } else {
        std::cout << "wrong server name";
        exit(0);
    }

    while (1) {
        // receive first
        struct ibv_wc iwc;
        while (ibv_poll_cq(rxcq, 1, &iwc) < 1)
            ;
        if (iwc.status != IBV_WC_SUCCESS) {
            fprintf(stderr, "ibv_poll_cq returned failure\n");
            exit(1);
        }

        // now respond
        std::cout << "receive : \n" << rxbuf;
        Transaction transaction = getTransactionFromBuffer(rxbuf, iwc.byte_len);
        std::thread t1(handleTransaction, qp, mr, txcq, txbuf, txbufsize, server, transaction);
        t1.detach();
    }

    printf("server exiting...\n");
}

void rdma_test (
        int argc,
        char **argv,
        void (*server)(struct ibv_qp*, struct ibv_mr*, struct ibv_cq*, char*, size_t , struct ibv_cq*, char*, size_t, char* testServer),
        void (*client)(struct ibv_qp*, struct ibv_mr*, struct ibv_cq*, char*, size_t , struct ibv_cq*, char*, size_t),
        char* testServer
)
{
    struct ibv_device *dev = NULL;
    int tcp_port = 18515;
    int ib_port  = 1;
    const char *servername = argv[1];

    srand48(time(NULL) * getpid());

    dev = ibFindDevice(NULL);
    if (dev == NULL) {
        fprintf(stderr, "failed to find infiniband device\n");
        exit(1);
    }

    printf("Using ib device `%s'.\n", dev->name);

    struct ibv_context *ctxt = ibv_open_device(dev);
    if (ctxt == NULL) {
        fprintf(stderr, "failed to open infiniband device\n");
        exit(1);
    }

    // allocate a protection domain for our memory region
    struct ibv_pd *pd = ibv_alloc_pd(ctxt);
    if (pd == NULL) {
        fprintf(stderr, "failed to allocate infiniband pd\n");
        exit(1);
    }

    void *buf;
    const size_t bufsize = 8 * 1024 * 1024;
    if (posix_memalign(&buf, 4096, bufsize)) {
        fprintf(stderr, "posix_memalign failed\n");
        exit(1);
    }
    char *txbuf = (char *)buf;
    size_t txbufsize = bufsize/2;
    char *rxbuf = &txbuf[bufsize/2];
    size_t rxbufsize = bufsize/2;

    // register our userspace buffer with the HCA
    struct ibv_mr *mr = ibv_reg_mr(pd, buf, bufsize,
                                   IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_LOCAL_WRITE);
    if (mr == NULL) {
        fprintf(stderr, "failed to register memory region\n");
        exit(1);
    }

    // create completion queues for receive and transmit
    struct ibv_cq *rxcq = ibv_create_cq(ctxt, DEPTH, NULL, NULL, 0);
    if (rxcq == NULL) {
        fprintf(stderr, "failed to create receive completion queue\n");
        exit(1);
    }
    struct ibv_cq *txcq = ibv_create_cq(ctxt, DEPTH, NULL, NULL, 0);
    if (txcq == NULL) {
        fprintf(stderr, "failed to create receive completion queue\n");
        exit(1);
    }

    // fill in a big struct of queue pair parameters
    struct ibv_qp_init_attr qpia;
    memset(&qpia, 0, sizeof(qpia));
    qpia.send_cq = txcq;
    qpia.recv_cq = rxcq;
    qpia.cap.max_send_wr  = DEPTH;	// max outstanding send requests
    qpia.cap.max_recv_wr  = DEPTH;	// max outstanding recv requests
    qpia.cap.max_send_sge = 1;	// max send scatter-gather elements
    qpia.cap.max_recv_sge = 1;	// max recv scatter-gather elements
    qpia.cap.max_inline_data = 0;	// max bytes of immediate data on send q
    qpia.qp_type = IBV_QPT_RC;	// RC, UC, UD, or XRC
    qpia.sq_sig_all = 0;		// only generate CQEs on requested WQEs

    // create the queue pair
    struct ibv_qp *qp = ibv_create_qp(pd, &qpia);
    if (qp == NULL) {
        fprintf(stderr, "failed to create queue pair\n");
        exit(1);
    }

    // move from RESET to INIT state
    struct ibv_qp_attr qpa;
    memset(&qpa, 0, sizeof(qpa));
    qpa.qp_state   = IBV_QPS_INIT;
    qpa.pkey_index = 0;
    qpa.port_num   = ib_port;
    qpa.qp_access_flags = IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_LOCAL_WRITE;
    if (ibv_modify_qp(qp, &qpa,  IBV_QP_STATE |
                                 IBV_QP_PKEY_INDEX |
                                 IBV_QP_PORT |
                                 IBV_QP_ACCESS_FLAGS)) {
        fprintf(stderr, "failed to modify qp state\n");
        exit(1);
    }

    // exchange qp info over TCP so we can move to the RTR and RTS states
    int psn = lrand48() & 0xffffff;	// seems to be the standard way?
    struct exchange_params params = { ibGetLID(ctxt, ib_port), qp->qp_num, psn };
    printf("Local  LID 0x%x, QPN 0x%x, PSN 0x%x\n", params.lid, params.qpn, params.psn);
    if (servername)
        params = client_exchange(servername, tcp_port, &params);
    else
        params = server_exchange(tcp_port, &params);
    printf("Remote LID 0x%x, QPN 0x%x, PSN 0x%x\n", params.lid, params.qpn, params.psn);

    // add receive buf so we can --> RTR
    // XXX: we add the same buf multiple times. latency is _much_ better with
    //      more RX bufs.
    for (int j = 0; j < DEPTH; j++)
        ibPostReceive(qp, mr, rxbuf, rxbufsize);

    // now connect up the qps and switch to RTR
    memset(&qpa, 0, sizeof(qpa));
    qpa.qp_state = IBV_QPS_RTR;
    qpa.path_mtu = IBV_MTU_1024;
    qpa.dest_qp_num = params.qpn;
    qpa.rq_psn = params.psn;
    qpa.max_dest_rd_atomic = 1;
    qpa.min_rnr_timer = 12;
    qpa.ah_attr.is_global = 0;
    qpa.ah_attr.dlid = params.lid;
    qpa.ah_attr.sl = 0;
    qpa.ah_attr.src_path_bits = 0;
    qpa.ah_attr.port_num = ib_port;

    if (ibv_modify_qp(qp, &qpa, IBV_QP_STATE |
                                IBV_QP_AV |
                                IBV_QP_PATH_MTU |
                                IBV_QP_DEST_QPN |
                                IBV_QP_RQ_PSN |
                                IBV_QP_MIN_RNR_TIMER |
                                IBV_QP_MAX_DEST_RD_ATOMIC)) {
        fprintf(stderr, "failed to modify qp state\n");
        exit(1);
    }

    // now move to RTS
    qpa.qp_state = IBV_QPS_RTS;
    qpa.timeout = 14;
    qpa.retry_cnt = 7;
    qpa.rnr_retry = 7;
    qpa.sq_psn = psn;
    qpa.max_rd_atomic = 1;
    if (ibv_modify_qp(qp, &qpa, IBV_QP_STATE |
                                IBV_QP_TIMEOUT |
                                IBV_QP_RETRY_CNT |
                                IBV_QP_RNR_RETRY |
                                IBV_QP_SQ_PSN |
                                IBV_QP_MAX_QP_RD_ATOMIC)) {
        fprintf(stderr, "failed to modify qp state\n");
        exit(1);
    }

    // XXX- should handshake again over TCP socket to synchronise
    if (servername)
        sleep(1);	// have client sleep for server to poll on rx

    if (servername)
        client(qp, mr, rxcq, rxbuf, rxbufsize, txcq, txbuf, txbufsize);
    else
        server(qp, mr, rxcq, rxbuf, rxbufsize, txcq, txbuf, txbufsize, testServer);
}


void RdmaTest(int argv, char* args[], char* testServer){
    // server name: twopl, mvcc, occ, timestamp
    rdma_test(argv, args, RdmaServer, RdmaClient, testServer);
}