#pragma once

#include <arpa/inet.h>
#include <netdb.h>
#include <rdma/rdma_cma.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>
#include <atomic>
#include "lru_cache.h"
#include "msg.h"
#include "rdma_conn_manager.h"
#include "rdma_mem_pool.h"
#include "string"
#include "thread"
#include <unordered_map>
#include "spinlock.h"
#include "rwlock.h"

#define VALUE_LEN 128
#define SHARDING_NUM 64
#define BUCKET_NUM 1048575
static_assert(((SHARDING_NUM & (~SHARDING_NUM + 1)) == SHARDING_NUM),
              "RingBuffer's size must be a positive power of 2");

namespace kv {

// calculate fingerprint
  static inline char hashcode1B(const char *x)
  {
      uint64_t a = *(uint64_t*)x;
      uint64_t b = *(uint64_t*)(x + 8);
      a ^= b;
      a ^= a >> 32;
      a ^= a >> 16;
      a ^= a >> 8;
      return (char)(a & 0xffUL);
  }

typedef struct __attribute__((packed)) internal_value_t {
  // uint64_t remote_addr;
  uint16_t offset;
  uint8_t remote_addr[6];
} internal_value_t;

/* One slot stores the key and the meta info of the value which
   describles the remote addr, size, remote-key on remote end. */
class  hash_map_slot {
 public:
  char key[16];
  internal_value_t internal_value;
  // hash_map_slot *next;
  // char finger; // key的finger，加速比较
  uint8_t next[6];
};

// const int a = sizeof(hash_map_slot)

#define READ_PTR(addr) ((*(uint64_t*)addr) & 0x0000FFFFFFFFFFFFUL)

// const int hash_map_slot_size = sizeof(hash_map_slot);

class __attribute__((packed)) hash_map_t {
 public:
  hash_map_slot *m_bucket[BUCKET_NUM];
  // rw_spin_lock m_bucket_lock[BUCKET_NUM];
  MyLock m_bucket_lock[BUCKET_NUM/4 + 1]; // 每8行共用一个读写锁
  // char m_bucket_lock[56][BUCKET_NUM/4];
  /* Initialize all the pointers to nullptr. */
  hash_map_t() {
    for (int i = 0; i < BUCKET_NUM; ++i) {
      m_bucket[i] = nullptr;
    }
  }

  // /* Find the corresponding key. */
  // hash_map_slot *find(const std::string &key) {
  //   int index = std::hash<std::string>()(key) & (BUCKET_NUM - 1);
  //   // char key_finger = hashcode1B(key.c_str());
  //   m_bucket_lock[index].lock_reader();
  //   hash_map_slot *cur = m_bucket[index];
  //   if (!cur) {
  //     m_bucket_lock[index].unlock_reader();
  //     return nullptr;
  //   }
    
  //   while (cur) {
  //     if (
  //       // key_finger == cur->finger && 
  //       memcmpx86_64(cur->key, key.c_str(), 16) == 0) {
  //       m_bucket_lock[index].unlock_reader();
  //       return cur;
  //     }
  //     cur = (hash_map_slot *)READ_PTR(cur->next);
  //   }
  //   m_bucket_lock[index].unlock_reader();
  //   return nullptr;
  // }

  // /* Insert into the head of the list. */
  // void insert(const std::string &key, internal_value_t internal_value, hash_map_slot *new_slot) {
  //   int index = std::hash<std::string>()(key) & (BUCKET_NUM - 1);
  //   memcpy(new_slot->key, key.c_str(), 16);
  //   // new_slot->finger = hashcode1B(new_slot->key);
  //   new_slot->internal_value = internal_value;
  //   m_bucket_lock[index].lock_writer();
  //   if (!m_bucket[index]) {
  //     m_bucket[index] = new_slot;
  //   } else {
  //     /* Insert into the head. */
  //     hash_map_slot *tmp = m_bucket[index];
  //     m_bucket[index] = new_slot;
  //     // new_slot->next = tmp;
  //     memcpy(new_slot->next, &tmp, 6);
  //   }
  //   m_bucket_lock[index].unlock_writer();
  // }

  /* Find the corresponding key. */
  hash_map_slot *find(const std::string &key) {
    int index = std::hash<std::string>()(key) % BUCKET_NUM;
    // char key_finger = hashcode1B(key.c_str());
    ReadLock rl(m_bucket_lock[index/4]);
    hash_map_slot *cur = m_bucket[index];
    if (!cur) {
      return nullptr;
    }
    
    while (cur) {
      if (
        // key_finger == cur->finger && 
        memcmp(cur->key, (void*)key.c_str(), 16) == 0) {
        return cur;
      }
      cur = (hash_map_slot *)READ_PTR(cur->next);
    }
    return nullptr;
  }

  /* Insert into the head of the list. */
  void insert(const std::string &key, internal_value_t internal_value, hash_map_slot *new_slot) {
    int index = std::hash<std::string>()(key) % BUCKET_NUM;
    memcpy(new_slot->key, key.c_str(), 16);
    // new_slot->finger = hashcode1B(new_slot->key);
    new_slot->internal_value = internal_value;
    WriteLock wl(m_bucket_lock[index/4]);
    if (!m_bucket[index]) {
      m_bucket[index] = new_slot;
    } else {
      /* Insert into the head. */
      hash_map_slot *tmp = m_bucket[index];
      m_bucket[index] = new_slot;
      // new_slot->next = tmp;
      memcpy(new_slot->next, &tmp, 6);
    }
  }
};

// const double hash_map_t_size = sizeof(hash_map_t) / 1024.0 / 1024;

/* Abstract base engine */
class Engine {
 public:
  virtual ~Engine(){};

  virtual bool start(const std::string addr, const std::string port) = 0;
  virtual void stop() = 0;

  virtual bool alive() = 0;
};

/* Local-side engine */
class LocalEngine : public Engine {
 public:
 LocalEngine() {std::cout << "LocalEngine size:" << sizeof (LocalEngine) / 1024.0 / 1024.0 / 1024.0 << "GB" << std::endl; };
  ~LocalEngine(){};

  bool start(const std::string addr, const std::string port) override;
  void stop() override;
  bool alive() override;

  bool write(const std::string &key, const std::string &value);
  bool read(const std::string &key, std::string &value);

 private:
  kv::ConnectionManager *m_rdma_conn_;
  /* NOTE: should use some concurrent data structure, and also should take the
   * extra memory overhead into consideration */
  hash_map_slot m_hash_slot_array_[16 * 12000000];
  hash_map_t m_hash_map_[SHARDING_NUM];         /* Hash Map with sharding. */
  std::atomic<int> m_slot_cnt_{0}; /* Used to fetch the slot from hash_slot_array. */

  RDMAMemPool *m_mem_pool_[SHARDING_NUM];
  // std::mutex m_mutex_[SHARDING_NUM];
  LRUCache *m_cache_[SHARDING_NUM];
};

const double a = sizeof (LocalEngine) / 1024.0 / 1024.0 / 1024.0;

/* Remote-side engine */
class RemoteEngine : public Engine {
 public:
  struct WorkerInfo {
    CmdMsgBlock *cmd_msg;
    CmdMsgRespBlock *cmd_resp_msg;
    struct ibv_mr *msg_mr;
    struct ibv_mr *resp_mr;
    rdma_cm_id *cm_id;
    struct ibv_cq *cq;
  };

  ~RemoteEngine(){};

  bool start(const std::string addr, const std::string port) override;
  void stop() override;
  bool alive() override;

 private:
  void handle_connection();

  int create_connection(struct rdma_cm_id *cm_id);

  struct ibv_mr *rdma_register_memory(void *ptr, uint64_t size);

  int remote_write(WorkerInfo *work_info, uint64_t local_addr, uint32_t lkey, uint32_t length, uint64_t remote_addr,
                   uint32_t rkey);

  int allocate_and_register_memory(uint64_t &addr, uint32_t &rkey, uint64_t size);

  void worker(WorkerInfo *work_info, uint32_t num);

  struct rdma_event_channel *m_cm_channel_;
  struct rdma_cm_id *m_listen_id_;
  struct ibv_pd *m_pd_;
  struct ibv_context *m_context_;
  bool m_stop_;
  std::thread *m_conn_handler_;
  WorkerInfo **m_worker_info_;
  uint32_t m_worker_num_;
  std::thread **m_worker_threads_;
};

}  // namespace kv