#include <iostream>
#include "assert.h"
#include "atomic"
#include "kv_engine.h"
namespace kv {

thread_local struct slot_bitmap* cur_slot_bitmap_ = nullptr;

thread_local int my_thread_id = -1;

std::queue<Page*> *page_pool_[THREAD_NUM]; 

/**
 * @description: start local engine service
 * @param {string} addr    the address string of RemoteEngine to connect
 * @param {string} port   the port of RemoteEngine to connect
 * @return {bool} true for success
 */
bool LocalEngine::start(const std::string addr, const std::string port) {
  auto time_start = TIME_NOW;
  m_rdma_conn_ = new ConnectionManager();
  if (m_rdma_conn_ == nullptr) return -1;
  if (m_rdma_conn_->init(addr, port, 4, 20)) {
    printf("m_rdma_conn failed\n");
    return false;
  }

  // for (int i = 0; i < SLOT_BITMAP_NUMS; i++) {
  //   bitmap *p = create_bitmap(KV_NUMS/SLOT_BITMAP_NUMS);
  //   assert(p);
  //   slot_bitmap *sb = new slot_bitmap(p, i);
  //   slot_map_[i] = sb;
  //   slot_queue_.enqueue(sb);
  // }

  // for (int i = 0; i < SHARDING_NUM; i++) {
  //   m_hash_map_[i].set_global_slot_array(m_hash_slot_array_);
  // }

  // for (int i = 0; i < SHARDING_NUM; i++) {
  //   m_mem_pool_[i] = new RDMAMemPool(m_rdma_conn_);
  // }
  
  // for (int i = 0; i < SHARDING_NUM; i++) {
  //   m_cache_[i] = new LRUCache((uint64_t)CACHELINE_NUMS, m_rdma_conn_, m_mem_pool_[i]);
  // }

  // for (int i = 0; i < THREAD_NUM; i++) {
  //   page_pool_[i] = new std::queue<Page *>();
  // }

  // for (int i = 0; i < THREAD_NUM; i++) {
  //   size_t kk = 2;
  //   for (size_t k = 0; k < kk; k++) {
  //     uint64_t mem_start_addr = 0;
  //     uint32_t rkey;
  //     int ret = m_rdma_conn_->register_remote_memory(mem_start_addr, rkey, PER_ALLOC_SIZE); // 一次分配1G
  //     if (ret) {
  //       printf("register memory fail\n");
  //       return -1;
  //     }
  //     uint64_t page_start_addr = mem_start_addr;
  //     for (int j = 0; j < 1024; j++) {
  //       page_pool_[i]->push(new Page(page_start_addr, rkey));
  //       page_start_addr += RDMA_ALLOCATE_SIZE;
  //     }
  //   }
  // }

  // std::cout << "LocalEngine size:" << sizeof (LocalEngine) / 1024.0 / 1024.0 / 1024.0 << "GB" << std::endl; 

  std::vector<std::thread> threads;
  // multi thread init
  for (int t = 0; t < THREAD_NUM; t++) {
    threads.emplace_back(
      [&](int thread_id) {
        {
          int per_thead_work = SLOT_BITMAP_NUMS / THREAD_NUM;
          int start_pos = thread_id * per_thead_work;
          int end_pos = (thread_id == THREAD_NUM-1) ? SLOT_BITMAP_NUMS : (thread_id+1) * per_thead_work;
          for (int i = start_pos; i < end_pos; i++) {
            bitmap *p = create_bitmap(KV_NUMS/SLOT_BITMAP_NUMS);
            assert(p);
            slot_bitmap *sb = new slot_bitmap(p, i);
            slot_map_[i] = sb;
            slot_queue_.enqueue(sb);
          }
        }

        {
          int per_thead_work = SHARDING_NUM / THREAD_NUM;
          int start_pos = thread_id * per_thead_work;
          int end_pos = (thread_id == THREAD_NUM-1) ? SHARDING_NUM : (thread_id+1) * per_thead_work;

          for (int i = start_pos; i < end_pos; i++) {
            m_hash_map_[i].set_global_slot_array(m_hash_slot_array_);
          }

          for (int i = start_pos; i < end_pos; i++) {
            m_mem_pool_[i] = new RDMAMemPool(m_rdma_conn_);
          }
          
          for (int i = start_pos; i < end_pos; i++) {
            m_cache_[i] = new LRUCache((uint64_t)CACHELINE_NUMS, m_rdma_conn_, m_mem_pool_[i]);
          }

          page_pool_[thread_id] = new std::queue<Page *>();
        }
      }, t
    );
  }

  for (auto &th : threads) {
    th.join();
  }

  for (int i = 0; i < THREAD_NUM; i++) {
    page_pool_[i] = new std::queue<Page *>();
  }

  for (int i = 0; i < THREAD_NUM; i++) {
    size_t kk = 2;
    for (size_t k = 0; k < kk; k++) {
      uint64_t mem_start_addr = 0;
      uint32_t rkey;
      int ret = m_rdma_conn_->register_remote_memory(mem_start_addr, rkey, PER_ALLOC_SIZE); // 一次分配1G
      if (ret) {
        printf("register memory fail\n");
        return -1;
      }
      uint64_t page_start_addr = mem_start_addr;
      for (int j = 0; j < 1024; j++) {
        page_pool_[i]->push(new Page(page_start_addr, rkey));
        page_start_addr += RDMA_ALLOCATE_SIZE;
      }
    }
  }

  auto time_end = TIME_NOW;
  auto time_delta = time_end - time_start;
  auto count = std::chrono::duration_cast<std::chrono::microseconds>(time_delta).count();
  std::cout << "Total time:" << count * 1.0 / 1000 / 1000 << "s" << std::endl;
  return true;
}

/**
 * @description: stop local engine service
 * @return {void}
 */
void LocalEngine::stop(){
    // TODO
};

/**
 * @description: get engine alive state
 * @return {bool}  true for alive
 */
bool LocalEngine::alive() { return true; }

#ifdef USE_AES
/**
 * @description: provide message about the aes_ecb mode
 * @return {bool}  true for success
 */
bool LocalEngine::set_aes() {
  m_aes_.algo = CTR;

  Ipp8u key[16] = {0xff, 0xee, 0xdd, 0xcc, 0xbb, 0xaa, 0x99, 0x88, 0x77, 0x66, 0x55, 0x44, 0x33, 0x22, 0x11, 0x00};
  m_aes_.key_len = 16;
  m_aes_.key = (Ipp8u *)malloc(sizeof(Ipp8u) * m_aes_.key_len);
  memcpy(m_aes_.key, key, m_aes_.key_len);

  m_aes_.blk_size = 16;
  m_aes_.counter_len = 16;
  Ipp8u ctr[] = {0x1f, 0x1e, 0x1d, 0x1c, 0x1b, 0x1a, 0x19, 0x18, 0x17, 0x16, 0x15, 0x14, 0x13, 0x12, 0x11, 0x10};
  m_aes_.counter = (Ipp8u *)malloc(sizeof(Ipp8u) * m_aes_.blk_size);
  memcpy(m_aes_.counter, ctr, sizeof(ctr));

  m_aes_.piv = nullptr;
  m_aes_.piv_len = 0;

  m_aes_.counter_bit = 64;
  return true;
}

/**
 * @description: use aes_cbc mode to encrypt value
 * @param {string} value            to be encrypted
 * @param {string} encrypt_value    encrypted value
 * @return {bool}  true for success
 */
bool LocalEngine::encrypted(const std::string value, std::string &encrypt_value) {
  assert(value.size() % 16 == 0);
  /*! Size for AES context structure */
  int m_ctxsize = 0;
  /*! Pointer to AES context structure */
  IppsAESSpec *m_pAES = nullptr;
  /*! Error status */
  IppStatus m_status = ippStsNoErr;
  /*! Pointer to encrypted plain text*/
  Ipp8u *m_encrypt_val = nullptr;
  m_encrypt_val = new Ipp8u[value.size()];
  if (nullptr == m_encrypt_val) return false;

  /* 1. Get size needed for AES context structure */
  m_status = ippsAESGetSize(&m_ctxsize);
  if (ippStsNoErr != m_status) return false;
  /* 2. Allocate memory for AES context structure */
  m_pAES = (IppsAESSpec *)(new Ipp8u[m_ctxsize]);
  if (nullptr == m_pAES) return false;
  /* 3. Initialize AES context */
  m_status = ippsAESInit(m_aes_.key, m_aes_.key_len, m_pAES, m_ctxsize);
  if (ippStsNoErr != m_status) return false;
  /* 4. counter bits */
  Ipp8u ctr[m_aes_.blk_size];
  memcpy(ctr, m_aes_.counter, m_aes_.counter_len);
  /* 5. Encryption */
  m_status = ippsAESEncryptCTR((Ipp8u *)value.c_str(), m_encrypt_val, value.size(), m_pAES, ctr, m_aes_.counter_bit);
  if (ippStsNoErr != m_status) return false;
  /* 6. Remove secret and release resources */
  ippsAESInit(0, m_aes_.key_len, m_pAES, m_ctxsize);

  if (m_pAES) delete[](Ipp8u *) m_pAES;
  m_pAES = nullptr;
  std::string tmp(reinterpret_cast<const char *>(m_encrypt_val), value.size());
  encrypt_value = tmp;

  if (m_encrypt_val) delete[] m_encrypt_val;
  m_encrypt_val = nullptr;
  return true;
}

// char *LocalEngine::decrypt(const char *value, size_t len) {
//   crypto_message_t *aes_get = get_aes();
//   Ipp8u *ciph = (Ipp8u *)malloc(sizeof(Ipp8u) * len);
//   memset(ciph, 0, len);
//   memcpy(ciph, value, len);

//   int ctxSize;               // AES context size
//   ippsAESGetSize(&ctxSize);  // evaluating AES context size
//   // allocate memory for AES context
//   IppsAESSpec *ctx = (IppsAESSpec *)(new Ipp8u[ctxSize]);
//   ippsAESInit(aes_get->key, aes_get->key_len, ctx, ctxSize);
//   Ipp8u ctr[aes_get->blk_size];
//   memcpy(ctr, aes_get->counter, aes_get->counter_len);

//   Ipp8u deciph[len];
//   ippsAESDecryptCTR(ciph, deciph, len, ctx, ctr, aes_get->counter_bit);
//   memcpy(ciph, deciph, len);
//   return (char *)ciph;
// }

#endif

/**
 * @description: put a key-value pair to engine
 * @param {string} key
 * @param {string} value
 * @param {bool} use aes or not for value
 * @return {bool} true for success
 */
// 需要考虑update操作
bool LocalEngine::write(const std::string &key, const std::string &value, bool use_aes) {
  if (unlikely(-1 == my_thread_id)) {
    my_thread_id = alloc_thread_id_++;
    my_thread_id %= THREAD_NUM;
  }
  // hash 分区
  int index = std::hash<std::string>()(key) % SHARDING_NUM;

  internal_value_t internal_value;
  internal_value.size = value.size();
  uint64_t start_addr = 0; // Page起始地址
  uint64_t remote_addr = 0; // CACHE_LINE起始地址
  uint32_t offset = 0;
  uint32_t rkey = 0;
  uint16_t slot_size = 0;
  bool found = false;

  /* check whether this key exist */
  hash_map_slot *it = m_hash_map_[index].find(key);
  if (!it) {
    if (m_mem_pool_[index]->get_remote_mem(internal_value, start_addr, rkey, slot_size) == false) {
      assert(false);
      return false;
    }
    remote_addr = start_addr + ((uint32_t)internal_value.cache_line_id) * CACHELINE_SIZE;
    offset = ((uint32_t)internal_value.slot_id) * ((uint32_t)slot_size);
  } else {
    found = true;
    /* if new_value_size <= old_value_size, 直接用原来的 addr 和 offset */
    if (internal_value.size <= it->internal_value.size) {
      bool ret = m_mem_pool_[index]->get_page_info(it->internal_value.page_id, start_addr, rkey, slot_size);
      assert(ret);
      it->internal_value.size = internal_value.size;
    } else {
      // othrerwise, free old space and alloc new space
      bool ret = m_mem_pool_[index]->free_slot_in_page(it->internal_value);
      assert(ret);
      it->internal_value.size = internal_value.size;
      if (m_mem_pool_[index]->get_remote_mem(it->internal_value, start_addr, rkey, slot_size) == false) {
        assert(false);
        return false;
      }
    }
    remote_addr = start_addr + ((uint32_t)it->internal_value.cache_line_id) * CACHELINE_SIZE;
    offset = ((uint32_t)it->internal_value.slot_id) * ((uint32_t)slot_size);
  }

#ifdef USE_AES
  /* 写入缓存，由缓存负责写入到remote */
  if (use_aes) {
    /* Use CBC mode to encryt value */
    std::string encrypt_value;
    encrypted(value, encrypt_value);
    assert(internal_value.size == encrypt_value.size());
    m_cache_[index]->Insert(remote_addr, rkey, offset, internal_value.size, encrypt_value.c_str());
  } else {
    m_cache_[index]->Insert(remote_addr, rkey, offset, internal_value.size, value.c_str());
  }
#else
  bool ret = m_cache_[index]->Insert(remote_addr, rkey, offset, internal_value.size, value.c_str());
  assert(ret);
#endif

  if (found) {
    return true; /* no need to update hash map */
  }

  /* Fetch a new slot from slot_array, do not need to new. */
  /* Update the hash_map. */
  int slot;
  if (unlikely(nullptr == cur_slot_bitmap_)) {
    bool ret = slot_queue_.try_dequeue(cur_slot_bitmap_);
    assert(ret);
  }

  for(;;) {
    slot = get_free(cur_slot_bitmap_->bitmap_);
    if (-1 == slot) {
      bool ret = slot_queue_.try_dequeue(cur_slot_bitmap_);
      assert(ret);
    } else {
      slot = cur_slot_bitmap_->bitmap_id * (KV_NUMS/SLOT_BITMAP_NUMS) + slot;
      break;
    }
  }
  

  // for (int i = 0; i < SLOT_BITMAP_NUMS; i++) {
  //   slot = get_free(slot_array_bitmap_[i]);
  //   if (-1 == slot)
  //     continue;
  //   else {
  //     slot = i * (KV_NUMS/SLOT_BITMAP_NUMS) + slot;
  //     break;
  //   }
  // }
  assert(slot >= 0);
  m_hash_map_[index].insert(key, internal_value, slot);
  return true;
}

/**
 * @description: read value from engine via key
 * @param {string} key
 * @param {string} &value
 * @return {bool}  true for success
 */
bool LocalEngine::read(const std::string &key, std::string &value) {
  int index = std::hash<std::string>()(key) % SHARDING_NUM;

  /* 从hash表查 start_addr 和 offset */
  hash_map_slot *it = m_hash_map_[index].find(key);
  if (!it) {
    return false;
  }

  uint64_t start_addr = 0; // Page起始地址
  uint64_t remote_addr = 0; // CACHE_LINE起始地址
  uint32_t offset = 0;
  uint32_t rkey = 0;
  uint16_t slot_size = 0;
  bool ret = m_mem_pool_[index]->get_page_info(it->internal_value.page_id, start_addr, rkey, slot_size);
  assert(ret);
  remote_addr = start_addr + ((uint32_t)it->internal_value.cache_line_id) * CACHELINE_SIZE;
  offset = ((uint32_t)it->internal_value.slot_id) * ((uint32_t)slot_size);
  value.resize(it->internal_value.size, '0');
  /* 从cache读数据，如果cache miss，cache会remote read把数据读到本地再返回 */
  if (!m_cache_[index]->Find(remote_addr, rkey, offset, it->internal_value.size, (char *)value.c_str())) {
    return false;
  }
  return true;
}

/** The delete interface */
bool LocalEngine::deleteK(const std::string &key) {
  internal_value_t internal_value;
  uint64_t remote_addr;
  uint32_t rkey;

  int index = std::hash<std::string>()(key) % SHARDING_NUM;

  /* Use the corresponding shard hash map to look for key. */

  // 1.delte link-list node, reutur the delete slot into bitmap
  int kv_slot_id = m_hash_map_[index].remove(key);
  if (-1 == kv_slot_id)
    return false;
  hash_map_slot *delete_node = &(m_hash_slot_array_[kv_slot_id]);
  internal_value_t iv = delete_node->internal_value;

  int bitmap_id = kv_slot_id / (KV_NUMS/SLOT_BITMAP_NUMS);
  int slot_id = kv_slot_id % (KV_NUMS/SLOT_BITMAP_NUMS);
  put_back(slot_map_[bitmap_id]->bitmap_, slot_id);
  if (slot_map_[bitmap_id]->bitmap_->free_cnt * 10 == KV_NUMS/SLOT_BITMAP_NUMS)
    slot_queue_.enqueue(slot_map_[bitmap_id]);
  
  // update page's bitmap and kv num, choose whether push empty page into free_list
  bool ret = m_mem_pool_[index]->free_slot_in_page(iv);

  return ret;
}

}  // namespace kv