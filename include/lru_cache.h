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
  bool clean = true;
};

const int ListNode_size = sizeof(ListNode);

class LRUCache {
 private:
  ListNode *head;                                    /* 双向链表头节点 */
  ListNode *tail;                                    /* 双向链表尾节点 */
  std::unordered_map<uint64_t, ListNode *> hash_map; /* hash表 key是remote_addr，value是包含key和entry的节点 */
  ConnectionManager *rdma;                           /* 用于 rdma remote write/read */
  typedef Spinlock LRUMutex;
  LRUMutex mutex_;
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
    uint32_t size = max_size * CACHE_ENTRY_MEM_SIZE;
    // char *mem = (char *)malloc(size);
    // uint32_t lkey = 0;
    /* 提前注册内存给 local cache 用，
       可以把这个内存按照 CACHE_ENTRY_MEM_SIZE 大小划分给 cache entry node
       的buffer */
    // uint64_t addr = (uint64_t)mem;
    // rdma->register_local_memory(addr, size, lkey);
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
    // 把 tail 的node淘汰掉，
    // 1. 如果 cache entry 的数据被修改过，需要remote write写回到 remote
    // node->remote_write(rdma,  mem_pool->get_rkey(node->key));
    // 2. 从链表和hash map中删除节点，并放入到free hash list中
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
    // 1. 用addr 作为key，查hash map
    // 1.1 如果找到了，把str的数据写入到应的 cache entry
    // 的buffer中，注意是从offset的偏移量开始写
    // 1.2.
    // 需要把对应节点移动到头部，以维护LRU

    // 2.1. 如果没找到，则以 addr 为key新增加一个cache entry，
    /* 代码逻辑：
            ListNode  *node  =  allocate_node(addr);
            hash_map[addr]  =  node;
            PushToFront(node);
            // 需要先把远程的数据读回来
            node->remote_read(rdma,  mem_pool->get_rkey(node->key)); */
    // 2.2 把数据写入到本地cache：memcpy(node->value.str  +  offset,  str,
    // size);
    // 2.3.
    // 由于新增加了节点，需要判断cache已经满了，如果是，需要淘汰cache数据
    // std::lock_guard<LRUMutex> guard{mutex_};
    mutex_.lock();
    if (hash_map.find(addr) != hash_map.end()) {
      // printf("insert find\n");
      auto node = hash_map[addr];
      PushToFront(node);
      node->clean = false;
      mutex_.unlock();
      memcpy(node->value.str + offset, str, size);
    } else {
      #ifdef STATISTIC
      miss_times++;
      #endif
      ListNode *node = Evict();
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
      mutex_.unlock();
      memcpy(node->value.str + offset, str, size);
    }
    return true;
  }

  bool Find(uint64_t addr, uint32_t offset, uint32_t size, char *str) {
    // 1. 用addr作为key，查hash map
    // 1.1. 如果找到了，从对应的 cache entry
    // 的buffer中，从offset的地方读取大小为size的数据到 str，用以返回给上层。
    // 1.2. 需要把对应节点移动到头部，以维护LRU
    /*代码逻辑：
            auto it = hash_map.find(addr);
            ListNode  *node  =  it->second;
            char  *des  =  node->value.str;
            memcpy(des  +  offset,  str,  size); */

    // 2.1. 如果没找到，则以 addr 为key新增加一个cache entry，
    // 2.2. 并从remote读取对应的数据到cache
    // entry的buffer中，并从offset中拷贝数据到 str，用以返回给上层。
    // 2.3.
    // 由于新增加了节点，需要判断cache已经满了，如果是，需要淘汰cache数据
    /* 代码逻辑：
            ListNode  *node  =  allocate_node(addr);
            node->remote_read(rdma,  mem_pool->get_rkey(node->key));
            hash_map[addr]  =  node;
                PushToFront(node);
                memcpy(str,  node->value.str  +  offset,  size); */
    // std::lock_guard<LRUMutex> guard{mutex_};
    mutex_.lock();
    if (hash_map.find(addr) != hash_map.end()) {
      // printf("Find find\n");
      auto node = hash_map[addr];
      PushToFront(node);
      mutex_.unlock();
      memcpy(str, node->value.str + offset, size);
    } else {
      #ifdef STATISTIC
      miss_times++;
      #endif
      ListNode *node = Evict();
      node->key = addr;
      int ret = node->remote_read(rdma, mem_pool->get_rkey(node->key));
      if (ret) {
        printf("remote_read error\n");
        return false;
      }
      hash_map[addr] = node;
      PushToFront(node);
      mutex_.unlock();
      memcpy(str, node->value.str + offset, size);
    }
    return true;
  }
};

}  // namespace kv
