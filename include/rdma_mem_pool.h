#pragma once

#include "page.h"
#include "rwlock.h"
#include "rdma_conn_manager.h"

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
  RDMAMemPool(ConnectionManager *conn_manager)
      : m_rdma_conn_(conn_manager), alloc_page_id_(0) {
    memset(is_using_page_list_, 0, sizeof(is_using_page_list_));
    page_map_ = (Page**)malloc(sizeof(Page*) * MAX_PAGE_NUMS);
  }

  ~RDMAMemPool() { destory(); }

  bool get_remote_mem(internal_value_t &iv, uint64_t &page_start_addr, uint32_t &rkey, uint16_t &slot_size);

  bool free_slot_in_page(const internal_value_t &iv);

  bool get_page_info(page_id_t page_id, uint64_t &start_addr, uint32_t &rkey, uint16_t &slot_size);

 private:
  void destory();

  ConnectionManager *m_rdma_conn_;     /* rdma connection manager */
 
  page_id_t alloc_page_id_; // 分配page_id
  Page *is_using_page_list_[64]; /* is using page list */ // page每16B分一级， 
  rw_spin_lock page_info_lock_; // for above page_info struct lock

  std::queue<Page *>free_page_list_; /* free page list */ // TODO: 改成无锁队列

  Page **page_map_; // 目前设置大小为256，应该足够 读写不需要加锁(alloc_page_id_顺序加锁分配到)
};
}  // namespace kv