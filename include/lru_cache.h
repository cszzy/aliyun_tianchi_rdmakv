#include <mutex>
#include <queue>
#include <unordered_map>
#include "rdma_conn.h"
#include "rdma_conn_manager.h"
#include "rdma_mem_pool.h"
#include "spinlock.h"

// #define STATISTIC

#ifdef STATISTIC
extern std::atomic<size_t> miss_times;
extern std::atomic<size_t> evict_times;
#endif

namespace kv {
// cache entry
struct CacheEntry {
  char *str; /* cache entry buffer */
};

// 把cache entry封装成一个node，用于实现double-linked list
struct ListNode {
  ListNode() : key_(0), prev_(nullptr), next_(nullptr), clean_(true), op_times(0){}
  // ListNode(uint64_t key, uint32_t rkey, const CacheEntry &value) : 
  //       key_(key), rkey_(rkey), value_(value), prev_(nullptr), next_(nullptr) {}

  /* 从remote读数据到当前 cache entry 的buffer */
  int remote_read(ConnectionManager *rdma) {
    int ret = rdma->remote_read((void *)value_.str, CACHELINE_SIZE, key_, rkey_);
    // TODO: 处理可能的错误
    return ret;
  }

  /* 把当前cache entry 的 buffer 写到 remote */
  int remote_write(ConnectionManager *rdma) {
    int ret = rdma->remote_write((void *)value_.str, CACHELINE_SIZE, key_, rkey_);
    return ret;
  }

  uint64_t key_; // ie. cacheline start_addr
  uint32_t rkey_;
  CacheEntry value_;
  ListNode *prev_;
  ListNode *next_;
  /* 标记数据是否被修改，evict可以用来判读是否需要写回到remote */
  bool clean_;
  int op_times;
};

// const int ListNode_size = sizeof(ListNode);

class LRUCache {
 private:
  ListNode *head;                                    /* 双向链表头节点 */
  ListNode *tail;                                    /* 双向链表尾节点 */
  std::unordered_map<uint64_t, ListNode *> hash_map; /* hash表 key是remote_addr，value是包含key和entry的节点 */
  ConnectionManager *rdma;                           /* 用于 rdma remote write/read */
  // typedef Spinlock LRUMutex;
  // LRUMutex mutex_;
  // MyLock mutex_;
  rw_spin_lock mutex_;
  RDMAMemPool *mem_pool; /* mem_pool 中保存了 remote addr
                            的rkey，可以调用mem_pool的接口来查询 */

  inline void PushToFront(ListNode *node) {
    // push the node to the front of the double-linked list
    if (node == head) return;

    if (node == tail) {
      tail = node->prev_;
      tail->next_ = nullptr;
    } else {
      node->prev_->next_ = node->next_;
      node->next_->prev_ = node->prev_;
    }

    // push to head
    node->prev_ = nullptr;
    node->next_ = head;
    head->prev_ = node;
    head = node;
  }

 public:
  LRUCache() {}
  LRUCache(uint64_t max_size, ConnectionManager *rdma_conn, RDMAMemPool *pool)
      : head(nullptr), tail(nullptr), rdma(rdma_conn), mem_pool(pool) {
    ListNode *prev_ = nullptr;
    for (size_t i = 0; i < max_size; i++) {
      // 预先分配内存 cache entry node
      ListNode *tmp = new ListNode();
      tmp->value_.str = new char[CACHELINE_SIZE];
      // free_nodes.push(tmp);
      if (prev_) {
        prev_->next_ = tmp;
        tmp->prev_ = prev_;
      } else {
        head = tmp;
      }
      prev_ = tmp;
    }
    tail = prev_;
  }

  // 返回淘汰的node
  ListNode *Evict() {
    auto node = tail;
    if (!node->clean_) {
      #ifdef STATISTIC
      evict_times++;
      #endif
      int ret = node->remote_write(rdma);
      if (ret) {
        printf("Evict write back error!\n");
      }
      node->clean_ = true;
    }
    if (0 != node->key_)
      hash_map.erase(node->key_);
    return node;
  }

  bool Insert(uint64_t addr, uint32_t rkey, uint32_t offset, uint32_t size, const char *str) {
    ListNode *node = nullptr;
    {
      // WriteLock wl(mutex_);
      mutex_.lock_writer();
      {
        auto iter = hash_map.find(addr);
        if (iter != hash_map.end()) {
          node = iter->second;
        }
      }

      if (node != nullptr) {
        node->clean_ = false;
        PushToFront(node);
      } else {
        #ifdef STATISTIC
        miss_times++;
        #endif
        node = Evict();
        if (node == nullptr) {
          printf("node is nullptr\n");
        }
        node->key_ = addr;
        node->rkey_ = rkey;
        hash_map[addr] = node;
        PushToFront(node);
        int ret = node->remote_read(rdma);
        if (ret) {
          printf("remote_read error\n");
          return false;
        }
        node->clean_ = false;
      }
      mutex_.unlock_writer();
    }

    memcpy(node->value_.str + offset, str, size);
    return true;
  }

  bool Find(uint64_t addr, uint32_t rkey, uint32_t offset, uint32_t size, char *str) {
    ListNode *node = nullptr;
    {
      // ReadLock rl(mutex_);
      mutex_.lock_reader();
      auto iter = hash_map.find(addr);
      if (iter != hash_map.end()) {
        node = iter->second;
        memcpy(str, node->value_.str + offset, size);
      }
      mutex_.unlock_reader();
    }

    if (node == nullptr) {
      #ifdef STATISTIC
      miss_times++;
      #endif
      {
        // WriteLock wl(mutex_);
        mutex_.lock_writer();
        node = Evict();
        node->key_ = addr;
        node->rkey_ = rkey;
        int ret = node->remote_read(rdma);
        if (ret) {
          printf("remote_read error\n");
          return false;
        }
        hash_map[addr] = node;
        PushToFront(node);
        mutex_.unlock_writer();
      }
      memcpy(str, node->value_.str + offset, size);
    }
    return true;
  }
};

}  // namespace kv
