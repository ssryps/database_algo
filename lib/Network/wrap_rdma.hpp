#ifndef WRAP_RDMA_HPP
#define WRAP_RDMA_HPP


/***  LIB USAGE   ***
 
 1. system("memcached -l 0.0.0.0 -p 10086 &");
 2. wrap_rdma = new WrapRdma(1, true);
  + para 1: connection Numbers
  + para 2: is_server => is server?
 3. mem = malloc(max_size);
 4. wrap_rdma->establish_conn(mem, max_size, 10085);
  + para 1: memory addr
  + para 2: memory size
  + para 3: paired key
 5. wrap_rdma->wrap_post_write_offset(mem, 32, 0, 0);
  + para 1: local sender address
  + para 2: sender's size
  + para 3: remote memory offset
  + para 4: choose a connection to send
 ********************/

/* RDMA parameters */
// const int POLL_DIS = 1;
const int W_POLL_DIS = 64;
const int R_POLL_DIS = 64;

extern "C"{
#include "libmrdma.h"
}

class WrapRdma{
public:
    bool is_server;
    int qp_sum;

    long sig_counter;
    m_ibv_res *ibv_res;
    int max_inline_size;
    int counter[5];
    unsigned long long start_time;
    long long nvm_mem_max_size;
    char *nvm_mem;
    // NVMConfig* nvm = nullptr;

    WrapRdma(int qp_sum_ = 1, bool is_server_ = true): qp_sum(qp_sum_), is_server(is_server_) {
        ibv_res = new m_ibv_res();

        // printf("[QP_SUM]%d\t[IS_SERVER]%d\n", qp_sum, is_server);

        FILL(*ibv_res);
        ibv_res->is_server = is_server;

        m_init_parameter(ibv_res, 1, 7000, 0xdeadbeaf, M_RC, qp_sum);

        max_inline_size = MAX_INLINE_DATA;
        sig_counter = 1;
        printf("/====rdmaer created success====/\n");
    }
    virtual ~WrapRdma() {

    }

    forceinline void establish_conn(char *nvm_mem_, long long nvm_mem_max_size_, int tcp_port) {

        nvm_mem = nvm_mem_;
        nvm_mem_max_size = nvm_mem_max_size_;

        m_open_device_and_alloc_pd(ibv_res);
        printf("[REG_MEM_SIZE]%ld\n", nvm_mem_max_size);
        m_reg_buffer(ibv_res, nvm_mem, nvm_mem_max_size);
        m_create_cq_and_qp(ibv_res, MAX_QP_LEN, IBV_QPT_RC);

        ibv_res->port = tcp_port;

        m_sync(ibv_res, "", nvm_mem);

        m_modify_qp_to_rts_and_rtr(ibv_res);
    }

    forceinline uint64_t wrap_post_cas(char *buffer, uint64_t compare, uint64_t swap, long offset, int qp_index) {
        m_post_cas_offset(ibv_res, buffer, compare, swap, offset, qp_index);
        m_poll_send_cq(ibv_res, qp_index);
        return *((volatile uint64_t *)buffer);
    }

    forceinline uint64_t wrap_post_faa(char *buffer, uint64_t add, long offset, int qp_index) {
        m_post_faa_offset(ibv_res, buffer, add, offset, qp_index);
        m_poll_send_cq(ibv_res, qp_index);
        return *((uint64_t *)buffer);
    }

    forceinline void wrap_post_write_offset(char *buffer, size_t size, long offset, int qp_index) {

        if (size > max_inline_size) {
            wrap_post_write_offset_without_inline(buffer, size, offset, qp_index);
            return;
        }
        if (sig_counter % W_POLL_DIS == 0) {
            m_post_write_offset_sig_inline(ibv_res, buffer, size, offset, qp_index);
            m_poll_send_cq(ibv_res, qp_index);
        } else {
            m_post_write_offset_inline(ibv_res, buffer, size, offset, qp_index);
        }
        sig_counter ++;
        counter[0] ++;
    }

    forceinline void  wrap_post_read_offset(char *buffer, size_t size, long offset, int qp_index) {

        if (sig_counter % R_POLL_DIS == 0) {
            m_post_read_offset_sig(ibv_res, buffer, size, offset, qp_index);
            m_poll_send_cq(ibv_res, qp_index);
        } else {
            m_post_read_offset(ibv_res, buffer, size, offset, qp_index);
        }

        sig_counter ++;
        counter[1] ++;

    } 

    forceinline void wrap_post_write_offset_without_inline(char *buffer, size_t size, long offset, int qp_index) {
        
        if (sig_counter % W_POLL_DIS == 0) {
            m_post_write_offset_sig(ibv_res, buffer, size, offset, qp_index);
            m_poll_send_cq(ibv_res, qp_index);
        } else {
            m_post_write_offset(ibv_res, buffer, size, offset, qp_index);
        }
        sig_counter ++;
        counter[2] ++;      
    }


    forceinline void wrap_post_write_offset_force(char *buffer, size_t size, long offset, int qp_index) {
        
        m_post_write_offset_sig(ibv_res, buffer, size, offset, qp_index);
        m_poll_send_cq(ibv_res, qp_index);
        counter[3] ++;      
        
    }

#if (defined USE_RDMA_READ_OPT)
    forceinline void wrap_post_read_offset_force(char *buffer, size_t size, long offset, int qp_index) {

        int *flag = (int *)(buffer + size - 5);
        *flag = 0xdeadbeaf;

        if (sig_counter % R_POLL_DIS == 0) {
            m_post_read_offset_sig(ibv_res, buffer, size, offset, qp_index);
            m_poll_send_cq(ibv_res, qp_index);
        } else {
            m_post_read_offset(ibv_res, buffer, size, offset, qp_index);
        }

        while(*(volatile int *)flag == 0xdeadbeaf); 
        sig_counter ++;
        counter[4] ++;      
    }

#else
    forceinline void wrap_post_read_offset_force(char *buffer, size_t size, long offset, int qp_index) {
        m_post_read_offset_sig(ibv_res, buffer, size, offset, qp_index);
        m_poll_send_cq(ibv_res, qp_index);
        counter[4] ++;      
    } 
#endif

};

#endif // WRAP_RDMA_HPP
