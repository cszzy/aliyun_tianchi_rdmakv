#include "rdma_mem_pool.h"
#include "kv_engine.h"
namespace kv {

extern thread_local int my_thread_id;
extern std::queue<Page*> *page_pool_[THREAD_NUM];

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
  slot_size = (page_index+1)*16 + 80;
  page_info_lock_.lock_writer();

retry:
  // get slot from page
  page = is_using_page_list_[page_index];
  if (page == nullptr) {
    // alloc remote Mem and new page
    // int ret = m_rdma_conn_->register_remote_memory(page_start_addr, rkey, RDMA_ALLOCATE_SIZE);
    // if (ret) {
    //   printf("register memory fail\n");
    //   page_info_lock_.unlock_writer();
    //   return -1;
    // }
    page_id_t page_id = alloc_page_id_++;
    assert(page_id < MAX_PAGE_NUMS);
    // page = new Page(page_id, page_start_addr, slot_size, rkey);
    page = page_pool_[my_thread_id]->front();
    assert(page);
    page_pool_[my_thread_id]->pop();
    page->format_newpage(page_id, slot_size);
    page_map_[page_id] = page;
    page_start_addr = page->get_start_addr();
    rkey = page->get_rkey();
#ifdef STATIC_REMOTE_MEM_USE
    remote_mem_use += RDMA_ALLOCATE_SIZE; 
#endif
    Page *old = nullptr;
    bool res = is_using_page_list_[page_index].compare_exchange_strong(old, page, std::memory_order_acquire);
    if (false == res){
      assert(false); // tmp不可能出现
      // CAS失败，放入empty_page_list
      empty_page_list.enqueue(page);
      page = is_using_page_list_[page_index];
      assert(page);
    }
    
    res = page->get_free_slot(iv.page_id, iv.cache_line_id, iv.slot_id);
    assert(res);
  } else {
    if (false == page->get_free_slot(iv.page_id, iv.cache_line_id, iv.slot_id)) {
      // first. lookup notfull_page_list_
      // second. lookup empty_page_list
redo2:
      Page *pp = nullptr;
      if (notfull_page_list_[page_index].try_dequeue(pp)) {
        assert(pp);
        if (pp->is_empty()) {
          bool res = empty_page_list.enqueue(pp);
          assert(res);
          goto redo2;
        } else {
          bool res = is_using_page_list_[page_index].compare_exchange_strong(page, pp, std::memory_order_acquire);
          if (false == res){
            assert(false); // tmp不可能出现
            // CAS失败，放回notfull_page_list_
            res = notfull_page_list_[page_index].enqueue(pp);
            assert(res);
          }
        }
      } else if (empty_page_list.try_dequeue(pp)) {
        assert(pp);
        pp->format_page(slot_size);
        bool res = is_using_page_list_[page_index].compare_exchange_strong(page, pp, std::memory_order_acquire);
         if (false == res){
          assert(false); // tmp不可能出现
          // CAS失败，放回empty_page_list
          res = empty_page_list.enqueue(pp);
          assert(res);
        }
      } else {
        assert(pp == nullptr);
        is_using_page_list_[page_index].compare_exchange_strong(page, pp, std::memory_order_acquire);
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
  int page_index = (page->get_slot_size()-80)/16 - 1;
  bool ret = page->free_slot(iv.cache_line_id, iv.slot_id);
  //页空余达到比例且不为正在使用的page, 放入not_full_page_list备用
  if (true == ret && page != is_using_page_list_[page_index]) {
    notfull_page_list_[page_index].enqueue(page);
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