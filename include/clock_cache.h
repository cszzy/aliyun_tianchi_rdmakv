#include <mutex>
#include <queue>
#include <unordered_map>
#include "rdma_conn.h"
#include "rdma_conn_manager.h"
#include "rdma_mem_pool.h"
#include "rwlock.h"

namespace kv {

struct __attribute__((aligned(64))) Node {
    uint64_t key_; // cacheline start addr
    uint32_t rkey_;
    char *value_;
    bool dirty_; // whether is dirty
    int ring_slot_id_;
    rw_spin_lock lock_; // for read/write/evict concurrent control

    Node() : key_(0), rkey_(0), dirty_(false), ring_slot_id_(-1) {
        // value_ = (char*)safe_align(CACHELINE_SIZE, 64, false);
        value_ = new char[CACHELINE_SIZE];
    }

    /* 从remote读数据到当前 cache entry 的buffer */
    int remote_read(ConnectionManager *rdma) {
        int ret = rdma->remote_read((void *)value_, CACHELINE_SIZE, key_, rkey_);
        // TODO: 处理可能的错误
        return ret;
    }

    /* 把当前cache entry 的 buffer 写到 remote */
    int remote_write(ConnectionManager *rdma) {
        int ret = rdma->remote_write((void *)value_, CACHELINE_SIZE, key_, rkey_);
        return ret;
    }
};

class ClockCache {
    public:
        ClockCache(ConnectionManager *rdma_conn) : rdma_(rdma_conn), clock_ptr(0) {
            ring_ = new Node[CACHELINE_NUMS];
        }

        bool Insert(uint64_t addr, uint32_t rkey, uint32_t offset, uint32_t size, const char *str) {
            Node *node = nullptr;
            hash_map_lock_.lock_reader();
            auto iter = hash_map_.find(addr);
            if (iter != hash_map_.end()) {
                node = iter->second;
            }
            hash_map_lock_.unlock_reader();

            if (nullptr == node) {
                int free_slot = get_free_node();
                node = &(ring_[free_slot]);
                node->ring_slot_id_ = free_slot;
                
                hash_map_lock_.lock_writer();
                auto iter = hash_map_.find(addr);
                if (iter == hash_map_.end())
                    hash_map_[addr] = node;
                else {
                    node = iter->second;
                    hash_map_lock_.unlock_writer();
                    goto ffff;
                }
                    
                hash_map_lock_.unlock_writer();

                node->lock_.lock_writer();
                visited[free_slot] = true;
                if (node->dirty_) {
                    bool ret = node->remote_write(rdma_);
                    if (ret) {
                        printf("remote write error\n");
                        return false;
                    }
                }
                node->key_ = addr;
                node->rkey_ = rkey;
                bool ret = node->remote_read(rdma_);
                if (ret) {
                    printf("remote read error\n");
                    return false;
                }
                memcpy(node->value_ + offset, str, size);
                node->dirty_ = true;
                node->lock_.unlock_writer();
            } else {
ffff:
                visited[node->ring_slot_id_] = true;
                node->lock_.lock_writer();
                // check addr == key
                if (node->key_ == addr) {
                    memcpy(node->value_ + offset, str, size);
                } else {
                    // if dirty, flush
                    if (node->dirty_) {
                        bool ret = node->remote_write(rdma_);
                        if (ret) {
                            printf("remote write error\n");
                            return false;
                        }
                    }
                    node->key_ = addr;
                    node->rkey_ = rkey;
                    bool ret = node->remote_read(rdma_);
                    if (ret) {
                        printf("remote read error\n");
                        return false;
                    }
                    memcpy(node->value_ + offset, str, size);
                }
                node->dirty_ = true;
                node->lock_.unlock_writer();
            }
            return true;
        }

        bool Find(uint64_t addr, uint32_t rkey, uint32_t offset, uint32_t size, char *str) {
            // printf("should not be here\n");
            Node *node = nullptr;
            hash_map_lock_.lock_reader();
            auto iter = hash_map_.find(addr);
            if (iter != hash_map_.end()) {
                node = iter->second;
            }
            hash_map_lock_.unlock_reader();

            if (nullptr == node) {
                int free_slot = get_free_node();
                node = &(ring_[free_slot]);
                node->ring_slot_id_ = free_slot;

                hash_map_lock_.lock_writer();
                auto iter = hash_map_.find(addr);
                if (iter == hash_map_.end())
                    hash_map_[addr] = node;
                else {
                    node = iter->second;
                    hash_map_lock_.unlock_writer();
                    goto fffind;
                }
                    
                hash_map_lock_.unlock_writer();

                visited[free_slot] = true;
                node->lock_.lock_writer();
                if (node->dirty_) {
                    bool ret = node->remote_write(rdma_);
                    if (ret) {
                        printf("remote write error\n");
                        return false;
                    }
                }
                node->key_ = addr;
                node->rkey_ = rkey;
                bool ret = node->remote_read(rdma_);
                if (ret) {
                    printf("remote read error\n");
                    return false;
                }
                memcpy(str, node->value_ + offset, size);
                node->dirty_ = false;
                node->lock_.unlock_writer();
            } else {
fffind:
                visited[node->ring_slot_id_] = true;
                node->lock_.lock_reader();
                // check addr == key
                if (node->key_ == addr) {
                    memcpy(str, node->value_ + offset, size);
                    node->lock_.unlock_reader();
                } else {
                    node->lock_.unlock_reader();
                    node->lock_.lock_writer();
                    // double check
                    if (node->key_ == addr) {
                        memcpy(str, node->value_ + offset, size);
                        node->lock_.unlock_writer();
                        return true;
                    }
                    // if dirty, flush
                    if (node->dirty_) {
                        bool ret = node->remote_write(rdma_);
                        if (ret) {
                            printf("remote write error\n");
                            return false;
                        }
                    }
                    node->key_ = addr;
                    node->rkey_ = rkey;
                    bool ret = node->remote_read(rdma_);
                    if (ret) {
                        printf("remote read error\n");
                        return false;
                    }
                    node->dirty_ = false;
                    memcpy(str, node->value_ + offset, size);
                    node->lock_.unlock_writer();
                }
            }
            return true;
        }

    private:
        int get_free_node() {
            int old_pos = clock_ptr;
            clock_ptr++;
            // int cur_pos = old_pos + 1;
            while (clock_ptr % CACHELINE_NUMS != old_pos) {
                if (false == visited[clock_ptr % CACHELINE_NUMS]) {
                    // visited[cur_pos % CACHELINE_NUMS] == true;
                    return clock_ptr % CACHELINE_NUMS;
                } else {
                    visited[clock_ptr % CACHELINE_NUMS] = false;
                    clock_ptr++;
                }
            }
            // 绕了一圈
            // visited[old_pos] == true;
            clock_ptr = old_pos;
            return old_pos;
        }

    private:
        std::unordered_map<uint64_t, Node*> hash_map_; // 后期直接用大数组存，不需要加锁
        rw_spin_lock hash_map_lock_;
        ConnectionManager *rdma_;
        Node *ring_;
        bool visited[CACHELINE_NUMS]; // used for clock evict, when is visited, turn to true
        std::atomic<int> clock_ptr;  // clock指针先使用中心化的atomic_int试一下有无瓶颈
};


}