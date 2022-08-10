#pragma once

#include <mutex>
#include <shared_mutex>
#include <condition_variable>

#define pause_cpu() asm("pause")

namespace kv {

class rw_spin_lock {
	std::atomic_int8_t counter{0};
    // std::atomic_int8_t write_counter{0}; // 写优先
	
public:
	rw_spin_lock() = default;
	rw_spin_lock(const rw_spin_lock&) = delete;
	rw_spin_lock &operator=(const rw_spin_lock&) = delete;
	
	void lock_reader() {
        while (true) {
            // 1、等待写锁被释放
            int8_t c;
            while (counter == -1) { // 写锁被持有
                // pause_cpu();// 自旋，等待写锁释放 
            }

            // 2、设置读锁		
            while ((c = counter.load(std::memory_order_relaxed)) != -1) {// counter值为-1，说明写者抢先申请到了锁
                if (counter.compare_exchange_strong(c, c+1, std::memory_order_acquire))
                    return;
            }
        }
    }

	void unlock_reader() {
        counter.fetch_sub(1, std::memory_order_release);
    }

	void lock_writer() {
        while (true) {
            // 1、等待所有锁被释放
            while (counter.load(std::memory_order_relaxed) != 0) {  // 可能别的线程已经成功获取了读锁或者写锁，等待释放锁 
                // pause_cpu();
            }
                
            // 2、设置写锁
            int8_t c = 0; // 期望已经释放锁了
            if (counter.compare_exchange_strong(c, -1, std::memory_order_acquire)) { // 检查是否被别的线程抢先获得锁了
                break;
            }
        }  
    }

	void unlock_writer() {
        counter.exchange(0, std::memory_order_release);
    }
};

} /* namespace linduo */