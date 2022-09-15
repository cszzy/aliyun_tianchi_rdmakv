#include <iostream>
#include "assert.h"
#include "atomic"
#include "kv_engine.h"

namespace kv {

thread_local int my_thread_id = -1;

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

  for (int i = 0; i < SHARDING_NUM; i++) {
    // RDMAConnection *conn = new RDMAConnection();
    // if (conn->init(addr, port)) {
    //   printf("conn init failed\n");
    //   return false;
    // }
    m_mem_pool_[i] = new RDMAMemPool(m_rdma_conn_);
    m_cache_[i] = new LRUCache((uint64_t)CACHE_ENTRY_SIZE, m_rdma_conn_, m_mem_pool_[i]);
  }
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
// /**
//  * @description: put a key-value pair to engine
//  * @param {string} key
//  * @param {string} value
//  * @return {bool} true for success
//  */
// bool LocalEngine::write(const std::string &key, const std::string &value) {
//   // hash 分区
//   int index = std::hash<std::string>()(key) % SHARDING_NUM;
//   // printf("index: %d\n", index);

//   internal_value_t internal_value;
//   uint64_t remote_addr = 0;
//   uint32_t offset = 0;
//   bool found = false;

//   // m_mutex_[index].lock();
//   /* 先看看这个数据是不是之前写过 */
//   hash_map_slot *it = m_hash_map_[index].find(key);
//   if (!it) {
//     /* 新写入的话就申请一个 addr 和 offset */
//     if (m_mem_pool_[index]->get_remote_mem(value.size(), remote_addr, offset)) {
//       // m_mutex_[index].unlock();
//       return false;
//     }
//   } else {
//     /* 直接用原来的 addr 和 offset */
//     remote_addr = READ_PTR(it->internal_value.remote_addr);
//     offset = it->internal_value.offset;
//     found = true;
//   }
//   // m_mutex_[index].unlock();

//   /* 写入缓存，由缓存负责写入到remote */
//   m_cache_[index]->Insert(remote_addr, offset, value.size(), value.c_str());

//   if (found) {
//     return true; /* no need to update hash map */
//   }

//   memcpy(internal_value.remote_addr, &remote_addr, 6);
//   // internal_value.remote_addr = remote_addr;
//   internal_value.offset = offset;
//   // m_mutex_[index].lock();
//   /* Fetch a new slot from slot_array, do not need to new. */
//   hash_map_slot *new_slot = &m_hash_slot_array_[m_slot_cnt_.fetch_add(1)];
//   /* Update the hash_map. */
//   m_hash_map_[index].insert(key, internal_value, new_slot);
//   // m_mutex_[index].unlock();
//   return true;
// };

// /**
//  * @description: put a key-value pair to engine
//  * @param {string} key
//  * @param {string} value
//  * @return {bool} true for success
//  */
// bool LocalEngine::write(const std::string &key, const std::string &value) {
//   // hash 分区
//   int index = std::hash<std::string>()(key) % SHARDING_NUM;
//   // printf("index: %d\n", index);

//   internal_value_t internal_value;
//   uint64_t remote_addr = 0;
//   uint32_t offset = 0;
//   bool found = false;

//   // m_mutex_[index].lock();
//   /* 先看看这个数据是不是之前写过 */
//   hash_map_slot *it = m_hash_map_[index].find(key);
//   if (!it) {
//     /* 新写入的话就申请一个 addr 和 offset */
//     if (m_mem_pool_[index]->get_remote_mem(value.size(), remote_addr, offset)) {
//       // m_mutex_[index].unlock();
//       return false;
//     }
//   } else {
//     /* 直接用原来的 addr 和 offset */
//     remote_addr = READ_PTR(it->internal_value.remote_addr);
//     offset = it->internal_value.offset;
//     found = true;
//   }
//   // m_mutex_[index].unlock();

//   /* 写入缓存，由缓存负责写入到remote */
//   m_cache_[index]->Insert(remote_addr, offset, value.size(), value.c_str());

//   if (found) {
//     return true; /* no need to update hash map */
//   }

//   memcpy(internal_value.remote_addr, &remote_addr, 6);
//   // internal_value.remote_addr = remote_addr;
//   internal_value.offset = offset;
//   // m_mutex_[index].lock();
//   /* Fetch a new slot from slot_array, do not need to new. */
//   hash_map_slot *new_slot = &m_hash_slot_array_[m_slot_cnt_.fetch_add(1)];
//   /* Update the hash_map. */
//   m_hash_map_[index].insert(key, internal_value, new_slot);
//   // m_mutex_[index].unlock();
//   return true;
// };

/**
 * @description: put a key-value pair to engine
 * @param {string} key
 * @param {string} value
 * @param {bool} use aes or not for value
 * @return {bool} true for success
 */
bool LocalEngine::write(const std::string &key, const std::string &value, bool use_aes) {
#ifdef STATISTIC_KV
  if (unlikely(my_thread_id == -1)) {
    thread_id_counter_lock.lock_writer();
    my_thread_id = thread_id_counter++;
    thread_id_counter_lock.unlock_writer();
  }

  s_kv_[my_thread_id].kv_nums[(key.size() - 80) / 16]++;
#endif

  if (unlikely(need_rebuild)) {
    rebuild_index();
  }
  // hash 分区
  int index = std::hash<std::string>()(key) % SHARDING_NUM;
  // printf("index: %d\n", index);

  internal_value_t internal_value;
  uint64_t remote_addr = 0;
  uint32_t offset = 0;
  bool found = false;

  // m_mutex_[index].lock();
  /* 先看看这个数据是不是之前写过 */
  hash_map_slot *it = m_hash_map_[index].find(key);
  if (!it) {
    /* 新写入的话就申请一个 addr 和 offset */
    if (m_mem_pool_[index]->get_remote_mem(value.size(), remote_addr, offset)) {
      // m_mutex_[index].unlock();
      return false;
    }
  } else {
    /* 直接用原来的 addr 和 offset */
    remote_addr = READ_PTR(it->internal_value.remote_addr);
    offset = it->internal_value.offset;
    found = true;
  }
  // m_mutex_[index].unlock();

#ifdef USE_AES
  /* 写入缓存，由缓存负责写入到remote */
  if (use_aes) {
    /* Use CBC mode to encryt value */
    std::string encrypt_value;
    encrypted(value, encrypt_value);
    assert(value.size() == encrypt_value.size());
    m_cache_[index]->Insert(remote_addr, offset, encrypt_value.size(), encrypt_value.c_str());
  } else {
    m_cache_[index]->Insert(remote_addr, offset, value.size(), value.c_str());
  }
#else
  m_cache_[index]->Insert(remote_addr, offset, value.size(), value.c_str());
#endif

  if (found) {
    return true; /* no need to update hash map */
  }

  memcpy(internal_value.remote_addr, &remote_addr, 6);
  // internal_value.remote_addr = remote_addr;
  internal_value.offset = offset;
  // m_mutex_[index].lock();
  /* Fetch a new slot from slot_array, do not need to new. */
  hash_map_slot *new_slot = &m_hash_slot_array_[m_slot_cnt_.fetch_add(1)];
  /* Update the hash_map. */
  m_hash_map_[index].insert(key, internal_value, new_slot);
  // m_mutex_[index].unlock();
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
  // printf("index: %d\n", index);

  /* 保存 start_addr 和 offset 的结构体  */
  internal_value_t inter_val;
  /* 从hash表查 start_addr 和 offset */
  // m_mutex_[index].lock();
  hash_map_slot *it = m_hash_map_[index].find(key);
  if (!it) {
    // m_mutex_[index].unlock();
    return false;
  }
  inter_val = it->internal_value;
  // m_mutex_[index].unlock();

  value.resize(VALUE_LEN, '0');
  /* 从cache读数据，如果cache miss，cache会remote read把数据读到本地再返回 */
  if (!m_cache_[index]->Find(READ_PTR(inter_val.remote_addr), inter_val.offset, VALUE_LEN, (char *)value.c_str())) {
    return false;
  }
  return true;
}

/** The delete interface */
bool LocalEngine::deleteK(const std::string &key) {
  if (unlikely(!need_rebuild)) {
    rebuild_lock.lock_writer();
    
    if (need_rebuild == false) {
      need_rebuild = true;
      std::cout << "start delete" << std::endl;
#ifdef STATISTIC_KV
      // print statistic info
      statistic_kv statistic_;
      for (int i = 0; i < THREAD_NUM; i++) {
        for (int j = 0; j < 64; j++) {
          statistic_.kv_nums[j] += s_kv_[i].kv_nums[j];
        }
      }

      for (int j = 0; j < 64; j++) {
        std::cout << 80 + j * 16 << "B - " << 80 + (j+1) * 16 << "B : " <<
           statistic_.kv_nums[j] << " , " << (double)statistic_.kv_nums[j] / (12000000 * 16)  << std::endl; 
      }
#endif
    }

    
    rebuild_lock.unlock_writer();
  }
  internal_value_t internal_value;
  uint64_t remote_addr;
  uint32_t rkey;

  int index = std::hash<std::string>()(key) % SHARDING_NUM;

  /* Use the corresponding shard hash map to look for key. */
  return m_hash_map_[index].remove(key);
}

/* When we delete a number of KV pairs, we should rebuild the index to
 * reallocate remote addr for each KV to recycle fragments. This will block
 * front request processing. This solution should be optimized. */
void LocalEngine::rebuild_index() {
  /** rebuild all the index to recycle the fragment */
  /** step-1: block the database and not allowe any writes
   *  step-2: transverse the index to read each value
   *  step-3: read the value from the remote and write it to a new remote addr
   *  step-4: free all old addr
   */
  // 1. step1
  rebuild_lock.lock_writer();


  need_rebuild = false;
  rebuild_lock.unlock_writer();
}

}  // namespace kv