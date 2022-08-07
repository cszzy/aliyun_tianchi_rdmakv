#include "assert.h"
#include "atomic"
#include "kv_engine.h"
#include <iostream>

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
  if (m_rdma_conn_->init(addr, port, 4, 20)) return false;
  m_rdma_mem_pool_ = new RDMAMemPool(m_rdma_conn_);
  if (m_rdma_mem_pool_) return true;
  return false;
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
bool LocalEngine::write(const std::string key, const std::string value) {
  assert(m_rdma_conn_ != nullptr);
  internal_value_t internal_value;
  uint64_t remote_addr;
  uint32_t rkey;

  int index = std::hash<std::string>()(key) & (SHARDING_NUM - 1);
  m_mutex_[index].lock();
  bool found = false;

  /* Use the corresponding shard hash map to look for key. */
  hash_map_slot *it = m_hash_map[index].find(key);

  if (it) {
    /* Reuse the old addr. In this case, the new value size should be the same
     * with old one. TODO: Solve the situation that the new value size is larger
     * than the old  one */
    uint64_t mem_addr;
    if (m_rdma_mem_pool_->get_mem_info(it->internal_value.mt_.mem_id, mem_addr, rkey))
      return false;
    remote_addr = mem_addr + GET_OFFSET(it->internal_value.mt_.offset);
    found = true;
  } else {
    /* Not written yet, get the memory first. */
    uint64_t mem_addr;
    if (m_rdma_mem_pool_->get_mem(value.size(), internal_value.mt_.mem_id,
                           internal_value.mt_.offset, mem_addr, rkey)) {
      m_mutex_[index].unlock();
      return false;
    }
    
    remote_addr = mem_addr + GET_OFFSET(internal_value.mt_.offset);
    // printf("get mem %lld %d\n", remote_addr, rkey);
  }
  m_mutex_[index].unlock();

  /* Optimization: local node could cache some hot data. No need to write the
   * data to the remote for each insertion. Moving local data to the remote
   * could be done in the background instead of the critical path. */
  /* Also, we can batch some KV pairs together, writing them to remote in one
   * RDMA write in the background */

  int ret = m_rdma_conn_->remote_write((void *)value.c_str(), value.size(),
                                       remote_addr, rkey);
  if (ret) return false;
  // printf("write key: %s, value: %s, %lld %d\n", key.c_str(), value.c_str(),
  //        remote_addr, rkey);
  if (found) return true; /* no need to update hash map */

  /* Optimization: To support concurrent insertion */
  m_mutex_[index].lock();

  /* Fetch a new slot from slot_array, do not need to new. */
  hash_map_slot *new_slot = &hash_slot_array[slot_cnt.fetch_add(1)];

  /* Update the hash_map. */
  m_hash_map[index].insert(key, internal_value, new_slot);
  
  m_mutex_[index].unlock();
  return true;
}

/**
 * @description: read value from engine via key
 * @param {string} key
 * @param {string} &value
 * @return {bool}  true for success
 */
bool LocalEngine::read(const std::string key, std::string &value) {
  int index = std::hash<std::string>()(key) & (SHARDING_NUM - 1);
  internal_value_t inter_val;
  m_mutex_[index].lock();
  hash_map_slot *it = m_hash_map[index].find(key);
  if (!it) {
    m_mutex_[index].unlock();
    return false;
  }
  inter_val = it->internal_value;
  m_mutex_[index].unlock();
  value.resize(128, '0');
  uint64_t remote_addr;
  uint64_t mem_addr;
  uint32_t rkey;
  if (m_rdma_mem_pool_->get_mem_info(inter_val.mt_.mem_id, mem_addr, rkey))
    return false;
  remote_addr = mem_addr + GET_OFFSET(inter_val.mt_.offset);
  if (m_rdma_conn_->remote_read((void *)value.c_str(), 128, remote_addr, rkey))
    return false;
  // printf("read key: %s, value: %s, size:%d, %lld %d\n", key.c_str(),
  //        value.c_str(), value.size(), inter_val.remote_addr, inter_val.rkey);
  return true;
}

}  // namespace kv