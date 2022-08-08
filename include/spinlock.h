#pragma once

#include <iostream>
#include <atomic>

namespace kv {
class __attribute__((packed)) Spinlock {
  public:
  Spinlock() : flag(ATOMIC_FLAG_INIT) {};
  Spinlock(const Spinlock&) = delete;
  Spinlock& operator= (const Spinlock&) = delete;
  ~Spinlock() { }
  void lock() {
      // bool lock = false;
      while (flag.test_and_set(std::memory_order_acquire)) {
          // if (log_lock) {
          //     if (lock == false) {
          //         lock = true;
          //         lock_times++;
          //     }
          // }
      }
  }

  void unlock() { flag.clear(std::memory_order_release); }

  private:
  std::atomic_flag flag{ATOMIC_FLAG_INIT};
};
}