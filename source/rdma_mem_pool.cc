#include "rdma_mem_pool.h"

namespace kv {

/* return 0 if success, otherwise fail to get the mem */
// int RDMAMemPool::get_mem(uint64_t size, uint64_t &addr, uint32_t &rkey) {
//   if (size > RDMA_ALLOCATE_SIZE) return -1;

//   /* One of the optimizations is to support the concurrent mem allocation,
//    * otherwise it could be the bottleneck */
//   std::lock_guard<std::mutex> lk(m_mutex_);

// retry:
//   if (m_pos_ + size <= RDMA_ALLOCATE_SIZE && m_current_mem_ != 0) { /* local mem is enough */
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
//   // m_used_mem_.push_back(rdma_mem);

//   /* 2. allocate and register the new mem from remote */
//   /* Another optimization is to move this remote memory registration to the
//    * backgroud instead of using it in the critical path */
//   int ret = m_rdma_conn_->register_remote_memory(m_current_mem_, m_rkey_, RDMA_ALLOCATE_SIZE);
//   // printf("allocate mem %lld %ld\n", m_current_mem_, m_rkey_);
//   if (ret) return -1;
//   m_pos_ = 0;
//   /* 3. try to allocate again */
//   goto retry;
// }

int RDMAMemPool::get_remote_mem(uint64_t size, uint64_t &start_addr, uint32_t &offset) {
  if (size > RDMA_ALLOCATE_SIZE) return -1;
  std::lock_guard<std::mutex> lk(m_mutex_);

retry:
  /* 注册过的内存还够本次分配 */
  if (m_pos_ + size <= RDMA_ALLOCATE_SIZE && m_current_mem_ != 0) {
    offset = m_pos_ % CACHE_ENTRY_MEM_SIZE; /* 和 CACHE_ENTRY_MEM_SIZE 对齐 */
    start_addr = m_current_mem_ + m_pos_ - offset;
    m_pos_ += size;
    /* 保存rkey，不用返回给上层，需要用的时候可以过来查，因为有大量value的rkey重复，可以减少rkey占用的内存开销。
     */
    
    {
      std::lock_guard<std::mutex> kl(m_mem_rkey_lock_);
      m_mem_rkey_[start_addr] = m_rkey_;
    }
    return 0;
  }

  /* 保存已经注册的内存，主要是为了释放， 暂时也可以不考虑释放的问题 */
  // rdma_mem_t rdma_mem;
  // rdma_mem.addr = m_current_mem_;
  // rdma_mem.rkey = m_rkey_;
  // m_used_remote_mem_.push_back(rdma_mem);

  /* 向remote注册一片内存，一次多注册点，避免小块内存注册，慢慢按需分配 */
  int ret = m_rdma_conn_->register_remote_memory(m_current_mem_, m_rkey_, RDMA_ALLOCATE_SIZE); // 一次分配1个G
  if (ret) {
    printf("注册失败\n");
    return -1;
  }
  m_pos_ = 0;
  {
    std::lock_guard<std::mutex> kl(m_mem_rkey_lock_);
    m_mem_rkey_[m_current_mem_] = m_rkey_;
  }

  goto retry;
}

void RDMAMemPool::destory() {
  // TODO: release allocated resources
}

}  // namespace kv