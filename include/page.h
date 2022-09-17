#pragma once

#include "bitmap.h"
#include "rwlock.h"
#include <assert.h>

// 增加分级page
#define CACHELINE_NUMS (800)
#define CACHELINE_SIZE (1 << 14) // 16K
#define RDMA_ALLOCATE_SIZE (1 << 21ul) // 每次分配的page size, 2M
#define BITMAP_NUMS (RDMA_ALLOCATE_SIZE/CACHELINE_SIZE)

namespace kv {

typedef uint16_t page_id_t;
typedef uint16_t cache_id_t;
typedef uint16_t slot_id_t;

class Page {
public:
    Page(page_id_t page_id, uint64_t start_addr, uint16_t slot_size, uint32_t rkey) :
            page_id_(page_id), start_addr_(start_addr), slot_size_(slot_size), kv_nums_(0), m_rkey_(rkey) {
        for (int i = 0; i < BITMAP_NUMS; i++) {
            bitmap_[i] = create_bitmap(CACHELINE_SIZE/slot_size);
        }
    }

    ~Page() { // TODO，归还内存,不涉及Page析构，暂时不需要实现
    
    }

    // 释放slot
    void free_slot(cache_id_t cacheline_id, slot_id_t slot_id) {
        put_back(bitmap_[cacheline_id], slot_id);
        kv_nums_--;
    }

    // 获取空闲slot,失败返回false
     bool get_free_slot(page_id_t &page_id, cache_id_t &cacheline_id, slot_id_t &slot_id) {
        for (int i = 0; i < BITMAP_NUMS; i++) {
            int s = get_free(bitmap_[i]);
            if (-1 != s) {
                page_id = page_id_;
                slot_id = s;
                cacheline_id = i;
                kv_nums_++;
                return true;
            }
        }
        return false;
    }

    void format_page(uint16_t slot_size) {
        // delete旧的位图，生成新的位图
        assert(0 == kv_nums_);
        slot_size_ = slot_size;
        for (int i = 0; i < BITMAP_NUMS; i++) {
            delete bitmap_[i];
            bitmap_[i] = create_bitmap(CACHELINE_SIZE/slot_size_);
        }
    }

    uint32_t get_rkey() const { return m_rkey_; }

    uint64_t get_start_addr() const { return start_addr_; }

    uint16_t get_slot_size() const { return slot_size_; }

    bool is_empty() const { return 0 == kv_nums_; }
private:
    page_id_t page_id_;
    uint64_t start_addr_; // page start addr
    /**
     * slot size:
     *  80B - 96B
     *  97B - 112B
     * 113b - 128B
     *    ...
     */
    uint16_t slot_size_;
    uint16_t kv_nums_; // record kv nums in this page 
    uint32_t m_rkey_; // page remote memory rkey
    bitmap *bitmap_[BITMAP_NUMS]; // use bitmap for alloc and gc, per CACHE_ENTRY need a bitmap
};

}