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

#define SHARDING_NUM 97
#define BUCKET_NUM 1048573

#define THREAD_NUM 16

#define KV_NUMS (16 * 12000000) // 192000000
#define SLOT_BITMAP_NUMS 1920

#define USE_AES

#ifdef USE_AES
#include "ippcp.h"
#endif

namespace kv {

#ifdef USE_AES

/* Encryption algorithm competitor can choose. */
enum aes_algorithm {
  CTR=0, CBC, CBC_CS1, CBC_CS2, CBC_CS3, CFB, OFB
};

/* Algorithm relate message. */
typedef struct crypto_message_t {
  aes_algorithm algo;
  Ipp8u *key;
  Ipp32u key_len;
  Ipp8u *counter;
  Ipp32u counter_len;
  Ipp8u *piv;
  Ipp32u piv_len;
  Ipp32u blk_size;
  Ipp32u counter_bit;
} crypto_message_t;

#endif

/* One slot stores the key and the meta info of the value which
   describles the remote addr, size, remote-key on remote end. */
class hash_map_slot {
 public:
  char key[16];
  internal_value_t internal_value;
  // char finger; // key的finger，加速比较
  int next_slot_id;
  hash_map_slot() : next_slot_id(-1) {}
};

// const int hash_map_slot_size = sizeof(hash_map_slot);

#define READ_PTR(addr) ((*(uint64_t*)addr) & 0x0000FFFFFFFFFFFFUL)

// const int hash_map_slot_size = sizeof(hash_map_slot);

// 注意hash_map_t内部不会会更新位图，需要在外部管理位图
class hash_map_t {
 public:
  int m_bucket[BUCKET_NUM]; // 记录
  hash_map_slot *global_slot_array;
  // rw_spin_lock m_bucket_lock[BUCKET_NUM];
  rw_spin_lock m_bucket_lock[BUCKET_NUM/4 + 1]; // 每n行共用一个读写锁
  // char m_bucket_lock[56][BUCKET_NUM/4];
  /* Initialize all the pointers to nullptr. */
  hash_map_t() : global_slot_array(nullptr) {
    for (int i = 0; i < BUCKET_NUM; ++i) {
      m_bucket[i] = -1;
    }
  }

  void set_global_slot_array(hash_map_slot *s) {
    global_slot_array = s;
  }

  /* Find the corresponding key. */
  hash_map_slot *find(const std::string &key) {
    int index = std::hash<std::string>()(key) % BUCKET_NUM;
    // char key_finger = hashcode1B(key.c_str());
    // ReadLock rl(m_bucket_lock[index/4]);
    m_bucket_lock[index/4].lock_reader();
    int cur = m_bucket[index];
    
    hash_map_slot *cc = nullptr;
    while (-1 != cur) {
      cc = &(global_slot_array[cur]);
      if (memcmp(cc->key, (void*)key.c_str(), 16) == 0) {
        m_bucket_lock[index/4].unlock_reader();
        return cc;
      }
      cur = cc->next_slot_id;
    }
    m_bucket_lock[index/4].unlock_reader();
    return nullptr;
  }

  /* Insert into the head of the list. */
  void insert(const std::string &key, const internal_value_t &internal_value, int new_slot_id) {
    int index = std::hash<std::string>()(key) % BUCKET_NUM;
    hash_map_slot *new_slot = &(global_slot_array[new_slot_id]);
    memcpy(new_slot->key, key.c_str(), 16);
    // new_slot->finger = hashcode1B(new_slot->key);
    new_slot->internal_value = internal_value;
    // WriteLock wl(m_bucket_lock[index/4]);
    m_bucket_lock[index/4].lock_writer();
    int tmp_id = m_bucket[index];
    /* Insert into the head. */
    m_bucket[index] = new_slot_id;
    if (-1 != tmp_id) {
      new_slot->next_slot_id = tmp_id;
    }
    m_bucket_lock[index/4].unlock_writer();
  }

  // if not exist or delete fail, return -1, else return kv_slot_id
  int remove(const std::string &key) {
    int index = std::hash<std::string>()(key) % BUCKET_NUM;
    m_bucket_lock[index/4].lock_writer();
    int cur_id = m_bucket[index];

    hash_map_slot *cur = nullptr;
    int prev_id = -1;
    while (-1 != cur_id) {
      cur = &(global_slot_array[cur_id]);
      if (memcmp(cur->key, (void*)key.c_str(), 16) == 0) {
        if (-1 == prev_id) {
          m_bucket[index] = cur->next_slot_id;
        } else {
          hash_map_slot *prev = &(global_slot_array[prev_id]);
          prev->next_slot_id = cur->next_slot_id;
        }
        cur->next_slot_id = -1;
        m_bucket_lock[index/4].unlock_writer();
        return cur_id;
      }
      prev_id = cur_id;
      cur_id = cur->next_slot_id;
    }
    m_bucket_lock[index/4].unlock_writer();
    return -1;
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
 LocalEngine(){};

  ~LocalEngine(){};

  bool start(const std::string addr, const std::string port) override;
  void stop() override;
  bool alive() override;

  bool read(const std::string &key, std::string &value);

  // phase 2 add function

  bool write(const std::string &key, const std::string &value, bool use_aes = false);
  /** The delete interface */
  bool deleteK(const std::string &key);

#ifdef USE_AES
  /* Init aes context message. */
  bool set_aes();
  bool encrypted(const std::string value, std::string &encrypt_value);
  /* Evaluation problem will call this function. */
  crypto_message_t* get_aes() { return &m_aes_; }
#endif

  void Info() {
#ifdef STATIC_REMOTE_MEM_USE
    uint64_t total_remote_mem_use = 0;
    for (int i = 0; i < SHARDING_NUM; i++) {
      total_remote_mem_use += m_mem_pool_[i]->get_remote_mem_use();
    }
    std::cout << "Total Remote Mem Use: " << ((double)total_remote_mem_use)/1024.0/1024.0/1024.0 << " GB" << std::endl;
#endif
  }

 private:
  kv::ConnectionManager *m_rdma_conn_;
  /* NOTE: should use some concurrent data structure, and also should take the
   * extra memory overhead into consideration */
  // TODO: 将slot array分片？
  hash_map_slot m_hash_slot_array_[KV_NUMS];
  bitmap *slot_array_bitmap_[SLOT_BITMAP_NUMS]; //使用位图管理，是线程安全的嘛？
  // std::atomic<int> m_slot_cnt_{0}; /* Used to fetch the slot from hash_slot_array. */
  hash_map_t m_hash_map_[SHARDING_NUM];         /* Hash Map with sharding. */
  RDMAMemPool *m_mem_pool_[SHARDING_NUM];
  LRUCache *m_cache_[SHARDING_NUM];
#ifdef USE_AES
  crypto_message_t m_aes_;
#endif
};

// const double a = sizeof (LocalEngine) / 1024.0 / 1024.0 / 1024.0;

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