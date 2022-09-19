#include <iostream>
#include "assert.h"
#include "atomic"
#include "kv_engine.h"
namespace kv {

/**
 * @description: start local engine service
 * @param {string} addr    the address string of RemoteEngine to connect
 * @param {string} port   the port of RemoteEngine to connect
 * @return {bool} true for success
 */
bool LocalEngine::start(const std::string addr, const std::string port) {
  m_rdma_conn_ = new ConnectionManager();
  if (m_rdma_conn_ == nullptr) return -1;
  if (m_rdma_conn_->init(addr, port, 4, 20)) {
    printf("m_rdma_conn failed\n");
    return false;
  }

  for (int i = 0; i < SLOT_BITMAP_NUMS; i++) {
    slot_array_bitmap_[i] = create_bitmap(KV_NUMS/SLOT_BITMAP_NUMS);
  }

  for (int i = 0; i < SHARDING_NUM; i++) {
    m_hash_map_[i].set_global_slot_array(m_hash_slot_array_);
  }

  for (int i = 0; i < SHARDING_NUM; i++) {
    m_mem_pool_[i] = new RDMAMemPool(m_rdma_conn_);
  }
  
  for (int i = 0; i < SHARDING_NUM; i++) {
    m_cache_[i] = new LRUCache((uint64_t)CACHELINE_NUMS, m_rdma_conn_, m_mem_pool_[i]);
  }

  // std::cout << "LocalEngine size:" << sizeof (LocalEngine) / 1024.0 / 1024.0 / 1024.0 << "GB" << std::endl; 

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
  //Current algorithm is not supported, just for demonstration.
  m_aes_.algo = CFB;
  m_aes_.key_len = 16;
  m_aes_.key = new Ipp8u[16]{0x60, 0x3d, 0xeb, 0x10,
  0x15, 0xca, 0x71, 0xbe, 0x2b, 0x73, 0xae, 0xf0,
  0x85, 0x7d, 0x77, 0x81};
  if(nullptr == m_aes_.key) return false;
  return true;
  //The other algorithms may need to set piv、counter, etc. you need to supply it.
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
  IppsAESSpec* m_pAES = nullptr;
  /*! Error status */
  IppStatus m_status = ippStsNoErr;
  /*! Pointer to encrypted plain text*/
  Ipp8u* m_encrypt_val = nullptr;
  m_encrypt_val = new Ipp8u[value.size()];
  if (nullptr == m_encrypt_val) return false;

  /* 1. Get size needed for AES context structure */
  m_status = ippsAESGetSize(&m_ctxsize);
  if (ippStsNoErr != m_status) return false;
  /* 2. Allocate memory for AES context structure */
  m_pAES = (IppsAESSpec*)(new Ipp8u[m_ctxsize]);
  if (nullptr == m_pAES) return false;
  /* 3. Initialize AES context */
  m_status = ippsAESInit(m_aes_.key, m_aes_.key_len, m_pAES, m_ctxsize);
  if (ippStsNoErr != m_status) return false;
  /* 4. Encryption */
  m_status = ippsAESEncryptECB((Ipp8u *)value.c_str(), m_encrypt_val, value.size(), m_pAES);
  if (ippStsNoErr != m_status) return false;
  /* 5. Remove secret and release resources */
  ippsAESInit(0,m_aes_.key_len, m_pAES, m_ctxsize);

  if (m_pAES) delete [] (Ipp8u*)m_pAES;
  m_pAES = nullptr;
  std::string tmp(reinterpret_cast<const char*>(m_encrypt_val), value.size());
  encrypt_value = tmp;

  if (m_encrypt_val) delete [] m_encrypt_val;
    m_encrypt_val = nullptr;
  return true;
}
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
  for (int i = 0; i < SLOT_BITMAP_NUMS; i++) {
    slot = get_free(slot_array_bitmap_[i]);
    if (-1 == slot)
      continue;
    else {
      slot = i * (KV_NUMS/SLOT_BITMAP_NUMS) + slot;
      break;
    }
  }
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
  put_back(slot_array_bitmap_[bitmap_id], slot_id);
  
  // update page's bitmap and kv num, choose whether push empty page into free_list
  bool ret = m_mem_pool_[index]->free_slot_in_page(iv);

  return ret;
}

}  // namespace kv