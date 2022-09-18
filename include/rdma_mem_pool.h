#pragma once

#include "page.h"
#include "rwlock.h"
#include "rdma_conn_manager.h"
#include "conqueue.h"

#define STATIC_REMOTE_MEM_USE
#define PAGE_LEVELS 64

namespace kv {

typedef struct internal_value_t {
  page_id_t page_id; // 每个pool内部的page_id不会重复,16位足够
	cache_id_t cache_line_id; // cacheline_id, 2M / 16K = 128, 够用
  slot_id_t slot_id; // cacheline内slot_id, 16K / 80B = 204
  uint16_t size; // value size
  internal_value_t() : page_id(0), cache_line_id(0), slot_id(0), size(0) {}
} internal_value_t;

// const int internal_value_t_size = sizeof(internal_value_t);

#define MAX_PAGE_NUMS 256 // 每个pool256个page应该足够用

class RDMAMemPool {
 public:
  RDMAMemPool(ConnectionManager *conn_manager): m_rdma_conn_(conn_manager), alloc_page_id_(0) 
#ifdef STATIC_REMOTE_MEM_USE
         , remote_mem_use(0) 
#endif
  {
    for (int i = 0; i < PAGE_LEVELS; i++) {
      is_using_page_list_[i] = nullptr;
    }
    page_map_ = (Page**)malloc(sizeof(Page*) * MAX_PAGE_NUMS);
  }

  ~RDMAMemPool() { destory(); }

  bool get_remote_mem(internal_value_t &iv, uint64_t &page_start_addr, uint32_t &rkey, uint16_t &slot_size);

  bool free_slot_in_page(const internal_value_t &iv);

  bool get_page_info(page_id_t page_id, uint64_t &start_addr, uint32_t &rkey, uint16_t &slot_size);

#ifdef STATIC_REMOTE_MEM_USE
  uint64_t get_remote_mem_use() { return remote_mem_use.load(); }
#endif

 private:
  void destory();

  ConnectionManager *m_rdma_conn_;     /* rdma connection manager */
 
  std::atomic<page_id_t> alloc_page_id_; // 分配page_id
  std::atomic<Page *> is_using_page_list_[PAGE_LEVELS]; /* is using page list */ // page每16B分一级， 
  moodycamel::ConcurrentQueue<Page *> notfull_page_list_[PAGE_LEVELS]; // page 16B 分级放入不同的queue后续使用
  moodycamel::ConcurrentQueue<Page *> empty_page_list; // 空page

  Page **page_map_; // 目前设置大小为256，应该足够 读写不需要加锁(alloc_page_id_顺序加锁分配到)
#ifdef STATIC_REMOTE_MEM_USE
  std::atomic<uint64_t> remote_mem_use; // 单位为MB
#endif
  rw_spin_lock page_info_lock_;
};
}  // namespace kv