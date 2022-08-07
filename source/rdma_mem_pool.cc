#include "rdma_mem_pool.h"

namespace kv {

// /* return 0 if success, otherwise fail to get the mem */
// int RDMAMemPool::get_mem(uint64_t size, uint64_t &addr, uint32_t &rkey) {
//   if (size > RDMA_ALLOCATE_SIZE) return -1;

//   /* One of the optimizations is to support the concurrent mem allocation,
//    * otherwise it could be the bottleneck */
//   std::lock_guard<std::mutex> lk(m_mutex_);

// retry:
//   if (m_pos_ + size <= RDMA_ALLOCATE_SIZE &&
//       m_current_mem_ != 0) { /* local mem is enough */
//     // std::cout << "Alloc success " << std::endl;
//     // printf("alloc success\n");
//     addr = m_current_mem_ + m_pos_;
//     rkey = m_rkey_;
//     m_pos_ += size;
//     return 0;
//   }
//   /* allocate mem from remote */

//   /* 1. store the current mem to the used list */
//   rdma_mem_t rdma_mem;
//   rdma_mem.addr = m_current_mem_;
//   rdma_mem.rkey = m_rkey_;
//   m_used_mem_.push_back(rdma_mem);

//   /* 2. allocate and register the new mem from remote */
//   /* Another optimization is to move this remote memory registration to the
//    * backgroud instead of using it in the critical path */
//   int ret = m_rdma_conn_->register_remote_memory(m_current_mem_, m_rkey_,
//                                                  RDMA_ALLOCATE_SIZE);
//   // printf("allocate mem %lld %ld\n", m_current_mem_, m_rkey_);
//   if (ret) return -1;
//   m_pos_ = 0;
//   /* 3. try to allocate again */
//   goto retry;
// }

// 每次固定分配128B
int RDMAMemPool::get_mem(uint64_t size, uint8_t &mem_id, uint8_t *offset, uint64_t &mem_addr, uint32_t &rkey) {
    if (size > RDMA_ALLOCATE_SIZE) return -1;

  /* One of the optimizations is to support the concurrent mem allocation,
   * otherwise it could be the bottleneck */
  std::lock_guard<std::mutex> lk(m_mutex_);

retry:
  if (m_pos_ + size <= RDMA_ALLOCATE_SIZE &&
      m_current_mem_ != 0) { /* local mem is enough */
    // std::cout << "Alloc success " << std::endl;
    // printf("alloc success\n");
    {
      ReadGuard rg(m_used_mem_lock_);
      mem_id = m_used_mem_.size() - 1;
    }
    mem_addr = m_current_mem_;
    rkey = m_rkey_;
    uint32_t t = m_pos_ / 128;
    memcpy(offset, &t, 3);
    m_pos_ += 128;
    return 0;
  }
  /* allocate mem from remote */

  /* 1. allocate and register the new mem from remote */
  /* Another optimization is to move this remote memory registration to the
   * backgroud instead of using it in the critical path */
  int ret = m_rdma_conn_->register_remote_memory(m_current_mem_, m_rkey_,
                                                 RDMA_ALLOCATE_SIZE);
  // printf("allocate mem %lld %ld\n", m_current_mem_, m_rkey_);
  if (ret) return -1;

  /* 2. store the current mem to the used list */
  rdma_mem_t rdma_mem;
  rdma_mem.addr = m_current_mem_;
  rdma_mem.rkey = m_rkey_;
  {
    WriteGuard wg(m_used_mem_lock_);
    m_used_mem_.push_back(rdma_mem);
  }
  m_pos_ = 0;
  /* 3. try to allocate again */
  goto retry;
}

// 得到mem_id对应的mem起始地址和rkey
int RDMAMemPool::get_mem_info(uint8_t mem_id, uint64_t &mem_addr, uint32_t &rkey) {
  ReadGuard rg(m_used_mem_lock_);
  if (mem_id >= m_used_mem_.size()) {
    return -1;
  }

  mem_addr = m_used_mem_[mem_id].addr;
  rkey =  m_used_mem_[mem_id].rkey;
  return 0;
}

void RDMAMemPool::destory() {
  // TODO: release allocated resources
}

}  // namespace kv