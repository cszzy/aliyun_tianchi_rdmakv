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
  // uint32_t lkey; /* rdma lkey */
};

// 把cache entry封装成一个node，用于实现double-linked list
struct ListNode {
  ListNode() : prev(nullptr), next(nullptr), clean(true) {}
  ListNode(const uint64_t &k, const CacheEntry &v) : key(k), value(v), prev(nullptr), next(nullptr) {}

  /* 从remote读数据到当前 cache entry 的buffer */
  int remote_read(ConnectionManager *rdma, uint32_t rkey) {
    int ret = rdma->remote_read((void *)value.str, CACHE_ENTRY_MEM_SIZE, key, rkey);
    // TODO: 处理可能的错误
    return ret;
  }

  /* 把当前cache entry 的 buffer 写到 remote */
  int remote_write(ConnectionManager *rdma, uint32_t rkey) {
    int ret = rdma->remote_write((void *)value.str, CACHE_ENTRY_MEM_SIZE, key, rkey);
    return ret;
  }

  /* key 其实就是 remote addr, cache
   * entry的buffer对应的就是remote上地址为key的内容 */
  uint64_t key;
  CacheEntry value;
  ListNode *prev;
  ListNode *next;
  /* 标记数据是否被修改，evict可以用来判读是否需要写回到remote */
  bool clean;
};

const int ListNode_size = sizeof(ListNode);

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
      tail = node->prev;
      tail->next = nullptr;
    } else {
      node->prev->next = node->next;
      node->next->prev = node->prev;
    }

    // push to head
    node->prev = nullptr;
    node->next = head;
    head->prev = node;
    head = node;
  }

 public:
  LRUCache() {}
  LRUCache(uint64_t max_size, ConnectionManager *rdma_conn, RDMAMemPool *pool)
      : head(nullptr), tail(nullptr), rdma(rdma_conn), mem_pool(pool) {
    ListNode *prev = nullptr;
    for (int i = 0; i < max_size; i++) {
      // 预先分配内存 cache entry node
      ListNode *tmp = new ListNode();
      tmp->value.str = new char[CACHE_ENTRY_MEM_SIZE];
      // free_nodes.push(tmp);
      if (prev) {
        prev->next = tmp;
        tmp->prev = prev;
      } else {
        head = tmp;
      }
      prev = tmp;
    }
    tail = prev;
  }

  // 返回淘汰的node
  ListNode *Evict() {
    auto node = tail;
    if (!node->clean) {
      #ifdef STATISTIC
      evict_times++;
      #endif
      int ret = node->remote_write(rdma, mem_pool->get_rkey(node->key));
      if (ret) {
        printf("Evict write back error!\n");
      }
      node->clean = true;
    }
    hash_map.erase(node->key);
    return node;
  }

  bool Insert(uint64_t addr, uint32_t offset, uint32_t size, const char *str) {
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
        node->clean = false;
        PushToFront(node);
      } else {
        #ifdef STATISTIC
        miss_times++;
        #endif
        node = Evict();
        if (node == nullptr) {
          printf("node is nullptr\n");
        }
        node->key = addr;
        hash_map[addr] = node;
        auto rkey = mem_pool->get_rkey(node->key);
        PushToFront(node);
        int ret = node->remote_read(rdma, rkey);
        if (ret) {
          printf("remote_read error\n");
          return false;
        }
        node->clean = false;
      }
      mutex_.unlock_writer();
    }

    memcpy(node->value.str + offset, str, size);
    return true;
  }

  bool Find(uint64_t addr, uint32_t offset, uint32_t size, char *str) {
    ListNode *node = nullptr;
    {
      // ReadLock rl(mutex_);
      mutex_.lock_reader();
      auto iter = hash_map.find(addr);
      if (iter != hash_map.end()) {
        node = iter->second;
        memcpy(str, node->value.str + offset, size);
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
        node->key = addr;
        int ret = node->remote_read(rdma, mem_pool->get_rkey(node->key));
        if (ret) {
          printf("remote_read error\n");
          return false;
        }
        hash_map[addr] = node;
        PushToFront(node);
        mutex_.unlock_writer();
      }
      memcpy(str, node->value.str + offset, size);
    }
    return true;
  }
};

}  // namespace kv
