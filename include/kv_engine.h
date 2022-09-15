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
#define SHARDING_NUM 119
#define BUCKET_NUM 1048573
// static_assert(((SHARDING_NUM & (~SHARDING_NUM + 1)) == SHARDING_NUM),
//               "RingBuffer's size must be a positive power of 2");
#define likely(x) __builtin_expect(!!(x), 1)
#define unlikely(x) __builtin_expect(!!(x), 0)

#define THREAD_NUM 16

// #define USE_AES

#ifdef USE_AES
#include "ippcp.h"
#endif

namespace kv {

extern thread_local int my_thread_id;

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
// calculate fingerprint
  // static inline char hashcode1B(const char *x)
  // {
  //     uint64_t a = *(uint64_t*)x;
  //     uint64_t b = *(uint64_t*)(x + 8);
  //     a ^= b;
  //     a ^= a >> 32;
  //     a ^= a >> 16;
  //     a ^= a >> 8;
  //     return (char)(a & 0xffUL);
  // }

// 
typedef struct __attribute__((__aligned__(64))) statistic_kv {
  // 统计kv分布
  uint32_t kv_nums[64]; // 从80B到1024B每16B区间value数目统计
  statistic_kv() {
    memset(kv_nums, 0, sizeof(kv_nums));
  }
} statistic_kv;

typedef struct __attribute__((packed)) internal_value_t {
  // uint64_t remote_addr;
  uint16_t offset;
  uint16_t size;
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

class hash_map_t {
 public:
  hash_map_slot *m_bucket[BUCKET_NUM];
  // rw_spin_lock m_bucket_lock[BUCKET_NUM];
  rw_spin_lock m_bucket_lock[BUCKET_NUM/4 + 1]; // 每8行共用一个读写锁
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
    // ReadLock rl(m_bucket_lock[index/4]);
    m_bucket_lock[index/4].lock_reader();
    hash_map_slot *cur = m_bucket[index];
    if (!cur) {
      m_bucket_lock[index/4].unlock_reader();
      return nullptr;
    }
    
    while (cur) {
      if (
        // key_finger == cur->finger && 
        memcmp(cur->key, (void*)key.c_str(), 16) == 0) {
        m_bucket_lock[index/4].unlock_reader();
        return cur;
      }
      cur = (hash_map_slot *)READ_PTR(cur->next);
    }
    m_bucket_lock[index/4].unlock_reader();
    return nullptr;
  }

  /* Insert into the head of the list. */
  void insert(const std::string &key, internal_value_t internal_value, hash_map_slot *new_slot) {
    int index = std::hash<std::string>()(key) % BUCKET_NUM;
    memcpy(new_slot->key, key.c_str(), 16);
    // new_slot->finger = hashcode1B(new_slot->key);
    new_slot->internal_value = internal_value;
    // WriteLock wl(m_bucket_lock[index/4]);
    m_bucket_lock[index/4].lock_writer();
    if (!m_bucket[index]) {
      m_bucket[index] = new_slot;
    } else {
      /* Insert into the head. */
      hash_map_slot *tmp = m_bucket[index];
      m_bucket[index] = new_slot;
      // new_slot->next = tmp;
      memcpy(new_slot->next, &tmp, 6);
    }
    m_bucket_lock[index/4].unlock_writer();
  }

  // if not exist or delete fail, return false
  bool remove(const std::string &key) {
    int index = std::hash<std::string>()(key) % BUCKET_NUM;
    m_bucket_lock[index/4].lock_writer();
    hash_map_slot *cur = m_bucket[index];
    if (!cur) {
      m_bucket_lock[index/4].unlock_writer();
      return false;
    }

    hash_map_slot *prev = nullptr;
    while (cur) {
      if (
          // key_finger == cur->finger && 
          memcmp(cur->key, (void*)key.c_str(), 16) == 0) {
        if (prev == nullptr) {
          m_bucket[index] = (hash_map_slot *)READ_PTR(cur->next);
        } else {
          memcpy(prev->next, cur->next, 6);
        }
        delete cur;
        break;
      }
      prev = cur;
      cur = (hash_map_slot *)READ_PTR(cur->next);
    }

    m_bucket_lock[index/4].unlock_writer();
    return cur != nullptr;
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
 LocalEngine() : need_rebuild(false), thread_id_counter(0) {std::cout << "LocalEngine size:" << sizeof (LocalEngine) / 1024.0 / 1024.0 / 1024.0 << "GB" << std::endl; };
  ~LocalEngine(){};

  bool start(const std::string addr, const std::string port) override;
  void stop() override;
  bool alive() override;

  // bool write(const std::string &key, const std::string &value);
  bool read(const std::string &key, std::string &value);

  // phase 2 add function

#ifdef USE_AES
  /* Init aes context message. */
  bool set_aes();
  bool encrypted(const std::string value, std::string &encrypt_value);
  /* Evaluation problem will call this function. */
  crypto_message_t* get_aes() { return &m_aes_; }
#endif

  bool write(const std::string &key, const std::string &value, bool use_aes = false);
  /** The delete interface */
  bool deleteK(const std::string &key);

  /** Rebuild the hash index to recycle the unsed memory */
  void rebuild_index();

 private:
  kv::ConnectionManager *m_rdma_conn_;
  /* NOTE: should use some concurrent data structure, and also should take the
   * extra memory overhead into consideration */
  hash_map_slot m_hash_slot_array_[16 * 12000000];
  std::atomic<int> m_slot_cnt_{0}; /* Used to fetch the slot from hash_slot_array. */
  hash_map_t m_hash_map_[SHARDING_NUM];         /* Hash Map with sharding. */
  RDMAMemPool *m_mem_pool_[SHARDING_NUM];
  // std::mutex m_mutex_[SHARDING_NUM];
  LRUCache *m_cache_[SHARDING_NUM];
#ifdef USE_AES
  crypto_message_t m_aes_;
#endif
  bool need_rebuild; // delete后遇到第一个其他类型指令开始进行GC, GC完毕后置为false
  rw_spin_lock rebuild_lock;

  // for statistic
  statistic_kv s_kv_[THREAD_NUM];
  int thread_id_counter;
  rw_spin_lock thread_id_counter_lock;
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