#include "rdma_mem_pool.h"

namespace kv {

// int RDMAMemPool::get_remote_mem(uint64_t size, uint64_t &start_addr, uint32_t &offset) {
//   if (size > RDMA_ALLOCATE_SIZE) return -1;
//   // std::lock_guard<std::mutex> lk(m_mutex_);
//   m_mutex_.lock();
//   int page_index = (size == 80) ? 0 : (size - 81) / 16;
//   Page *page = nullptr;
// retry:
//   // get slot from page
//   page = is_using_page_list_[page_index];
//   if (page == nullptr) {
//     // alloc remote Mem and new page
//     uint64_t m_current_mem_;
//     uint32_t m_rkey_;
//     int ret = m_rdma_conn_->register_remote_memory(m_current_mem_, m_rkey_, RDMA_ALLOCATE_SIZE);
//     if (ret) {
//       printf("register memory fail\n");
//       m_mutex_.unlock();
//       return -1;
//     }
//     page = new Page(m_current_mem_, (page_index+1)*16 + 80, m_rkey_);
//     is_using_page_list_[page_index] = page;
//     used_pages_[m_current_mem_] = page;
//     bool ret = page->get_free_slot(size, start_addr, offset);
//     assert(ret = true);
//     goto end;
//   } else {
//     if (page->get_free_slot(size, start_addr, offset) == false) {
//       is_using_page_list_[page_index] = nullptr;
//       goto retry;
//     }
//   }

// end:
//   if (offset == 0) {
//     // std::lock_guard<std::mutex> kl(m_mem_rkey_lock_);
//     WriteLock wl(m_mem_rkey_lock_);
//     // m_mem_rkey_lock_.lock_writer();
//     m_mem_rkey_[start_addr] = page->get_rkey();
//     // m_mem_rkey_lock_.unlock_writer();
//   }
//   m_mutex_.unlock();
//   return 0;
// }

/**
 * @brief get mem from remote host
 * 
 * @param iv {return} kv metadata
 * @param page_start_addr {return} page start addr, for remote addr calculate
 * @param slot_size {return} slot_size
 * @return true 
 * @return false 
 */
bool RDMAMemPool::get_remote_mem(internal_value_t &iv, uint64_t &page_start_addr, uint32_t &rkey, uint16_t &slot_size) {
  uint16_t size = iv.size;
  if (size > RDMA_ALLOCATE_SIZE) 
    return false;

  int page_index = (size == 80) ? 0 : (size - 81) / 16;
  Page *page = nullptr;
  page_info_lock_.lock_writer();
  slot_size = (page_index+1)*16 + 80;

retry:
  // get slot from page
  page = is_using_page_list_[page_index];
  if (page == nullptr) {
    // alloc remote Mem and new page
    int ret = m_rdma_conn_->register_remote_memory(page_start_addr, rkey, RDMA_ALLOCATE_SIZE);
    if (ret) {
      printf("register memory fail\n");
      page_info_lock_.unlock_writer();
      return -1;
    }
    page_id_t page_id = alloc_page_id_++;
    assert(page_id < MAX_PAGE_NUMS);
    page = new Page(page_id, page_start_addr, slot_size, rkey);
    is_using_page_list_[page_index] = page;
    page_map_[page_id] = page;
    bool res = page->get_free_slot(iv.page_id, iv.cache_line_id, iv.slot_id);
    assert(res);
  } else {
    if (false == page->get_free_slot(iv.page_id, iv.cache_line_id, iv.slot_id)) {
      if (free_page_list_.empty()) {
        is_using_page_list_[page_index] = nullptr;
      } else {
        // TODO: 移动代码，减少锁占用
        Page *p = free_page_list_.front();
        free_page_list_.pop();
        p->format_page(slot_size);
        is_using_page_list_[page_index] = p;
      }
      goto retry;
    }
    page_start_addr = page->get_start_addr();
    rkey = page->get_rkey();
  }

  page_info_lock_.unlock_writer();
  return true;
}

bool RDMAMemPool::free_slot_in_page(const internal_value_t &iv) {
  Page *page = page_map_[iv.page_id];
  if (nullptr == page) {
    return false;
  }
  page_info_lock_.lock_writer();
  page->free_slot(iv.cache_line_id, iv.slot_id);
  if (page->is_empty() && 
      (page != is_using_page_list_[(page->get_slot_size()-80)/16 - 1])) {
    free_page_list_.push(page);
  }
  page_info_lock_.unlock_writer();
  return true;
}

bool RDMAMemPool::get_page_info(page_id_t page_id, uint64_t &start_addr, uint32_t &rkey, uint16_t &slot_size) {
  Page *page = page_map_[page_id];
  if (nullptr == page) {
    return false;
  }
  start_addr = page->get_start_addr();
  rkey = page->get_rkey();
  slot_size = page->get_slot_size();
  return true;
}

void RDMAMemPool::destory() {
  // TODO: release allocated resources
}

}  // namespace kv