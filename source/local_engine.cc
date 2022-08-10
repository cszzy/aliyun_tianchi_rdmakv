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

/**
 * @description: put a key-value pair to engine
 * @param {string} key
 * @param {string} value
 * @return {bool} true for success
 */
bool LocalEngine::write(const std::string &key, const std::string &value) {
  // hash 分区
  int index = std::hash<std::string>()(key) & (SHARDING_NUM - 1);
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

  /* 写入缓存，由缓存负责写入到remote */
  m_cache_[index]->Insert(remote_addr, offset, value.size(), value.c_str());

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
};

/**
 * @description: read value from engine via key
 * @param {string} key
 * @param {string} &value
 * @return {bool}  true for success
 */
bool LocalEngine::read(const std::string &key, std::string &value) {
  int index = std::hash<std::string>()(key) & (SHARDING_NUM - 1);
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

  value.resize(VALUE_LEN, 'a');
  /* 从cache读数据，如果cache miss，cache会remote read把数据读到本地再返回 */
  if (!m_cache_[index]->Find(READ_PTR(inter_val.remote_addr), inter_val.offset, VALUE_LEN, (char *)value.c_str())) {
    return false;
  }
  return true;
}

}  // namespace kv