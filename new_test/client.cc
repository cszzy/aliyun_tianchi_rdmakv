#include <cstdint>
#include <cstdio>
#include <cstring>
#include <iostream>
#include <iterator>
#include <mutex>
#include <random>
#include <string>
#include <thread>
#include <vector>
#include "config.h"
#include "kv_engine.h"
#include "test.h"
#include "zipf.h"
#include "logging.h"

using namespace kv;
using namespace std;

// constexpr int gen_zipf_thread_num = 16;

constexpr int thread_num = 16;
constexpr int M = 1000000;

constexpr int insert_num = thread_num * 10 * M;  // 16 * 10M
constexpr int write_op_per_thread = insert_num / thread_num;

constexpr int update_num = thread_num * M;  // 16 * 1M
constexpr int update_op_per_thread = update_num / thread_num;

constexpr int delete_num = thread_num * 9 * M;  // 16 * 9M
constexpr int delete_op_per_thread = delete_num / thread_num;

constexpr int read_num = thread_num * 32 * M;  // 16 * 32M
constexpr int read_op_per_thread = read_num / thread_num;
// encryption & decrption

// part 1 validate
// write 16 * 10M
// read 16 * 10M
// write 16 * 10M
// read 16 * 10M
// update 16 * 1M
// read ? < 1M

// part 2 delete
// delete 16 * 9M
// read deleted 16 * 9M
// write 16 * 9M

// part 3 hot data
// read 16 * 32M

static inline void bindCore(uint16_t core) {
    // printf("bind to %d\n", core);
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(core, &cpuset);
    int rc = pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);
    if (rc != 0) {
        printf("can't bind core %d!", core);
        exit(-1);
    }
}

void part1(LocalEngine *local_engine, TestKey *keys, int *zipf_index, int *key_slab_class,
           std::vector<std::thread> &threads) {
  LOG_INFO(" ============= part1 validate ==============>");
  {
    LOG_INFO(" @@@@@@@@@@@@@ write 16 * 10M @@@@@@@@@@@@@@@");
    auto time_now = TIME_NOW;
    // write 16 * 10M
    threads.clear();
    auto ts = TIME_NOW;
    for (int i = 0; i < thread_num; i++) {
      threads.emplace_back(
          [=](TestKey *k, int *slab_class) {
            bindCore(i);
            if (i == 0)
              LOG_INFO("Start write threads");
            std::string value;
            for (int j = 0; j < write_op_per_thread; j++) {
              // if (j % M == 0) {
              //   LOG_INFO("[thread %d] finish write %d kv", i, j);
              // }
              int key_idx = j + i * write_op_per_thread;
              uint8_t sc = genValueSlabSize0();
              slab_class[key_idx] = sc;
              value.resize(sc * kSlabSize);
              memcpy((char *)value.c_str(), k[key_idx].key, 16);
              auto succ = local_engine->write(k[key_idx].to_string(), value);
              ASSERT(succ, "[thread %d] failed to write %d", i, key_idx);
            }
            if (i == 0)
              LOG_INFO("End write threads");
          },
          keys, key_slab_class);
    }
    for (auto &th : threads) {
      th.join();
    }
    auto te = TIME_NOW;
    auto time_delta = te - ts;
    auto count = std::chrono::duration_cast<std::chrono::microseconds>(time_delta).count();
    std::cout << "Total time:" << count * 1.0 / 1000 / 1000 << "s" << std::endl;

    // read 16 * 10M
    LOG_INFO(" @@@@@@@@@@@@@ read 16 * 10M @@@@@@@@@@@@@@@");
    threads.clear();
    ts = TIME_NOW;
    for (int i = 0; i < thread_num; i++) {
      threads.emplace_back(
          [=](TestKey *k, int *slab_class) {
            bindCore(i);
            if (i == 0)
              LOG_INFO("Start read threads");
            std::string value;
            for (int j = 0; j < write_op_per_thread; j++) {
              // if (j % M == 0) {
              //   LOG_INFO("[thread %d] finish read %d kv", i, j);
              // }
              int key_idx = j + i * write_op_per_thread;
              auto succ = local_engine->read(k[key_idx].to_string(), value);
              ASSERT(succ, "[thread %d] failed to read %d", i, key_idx);
              ASSERT(value.size() == (size_t)slab_class[key_idx] * kSlabSize, "got val length %lu, expected %d",
                     value.size(), slab_class[key_idx] * kSlabSize)
              auto cmp = memcmp(value.c_str(), k[key_idx].key, 16);
              ASSERT(cmp == 0, "expect %.16s, got %.16s", k[key_idx].key, value.c_str());
            }
            if (i == 0)
              LOG_INFO("End read threads");
          },
          keys, key_slab_class);
    }
    for (auto &th : threads) {
      th.join();
    }
    te = TIME_NOW;
    time_delta = te - ts;
    count = std::chrono::duration_cast<std::chrono::microseconds>(time_delta).count();
    std::cout << "Total time:" << count * 1.0 / 1000 / 1000 << "s" << std::endl;

    // write 16 * 10M
    // value的大小不更改，只更改内容
    LOG_INFO(" @@@@@@@@@@@@@ write 16 * 10M @@@@@@@@@@@@@@@");
    threads.clear();
    ts = TIME_NOW;
    for (int i = 0; i < thread_num; i++) {
      threads.emplace_back(
          [=](TestKey *k, int *slab_class) {
            bindCore(i);
            if (i == 0)
              LOG_INFO("Start write threads");
            std::string value;
            for (int j = 0; j < write_op_per_thread; j++) {
              // if (j % M == 0) {
              //   LOG_INFO("[thread %d] finish write %d kv", i, j);
              // }
              int key_idx = j + i * write_op_per_thread;
              uint8_t sc = slab_class[key_idx];
              value.resize(sc * kSlabSize);
              // 更改规则：第一个字节+1
              TestKey new_val;
              memcpy(new_val.key, k[key_idx].key, kKeyLength);
              new_val.key[0] += 1;
              memcpy((char *)value.c_str(), new_val.key, 16);
              auto succ = local_engine->write(k[key_idx].to_string(), value);
              ASSERT(succ, "[thread %d] failed to write %d", i, key_idx);
            }
            if (i == 0)
              LOG_INFO("End write threads");
          },
          keys, key_slab_class);
    }
    for (auto &th : threads) {
      th.join();
    }
    te = TIME_NOW;
    time_delta = te - ts;
    count = std::chrono::duration_cast<std::chrono::microseconds>(time_delta).count();
    std::cout << "Total time:" << count * 1.0 / 1000 / 1000 << "s" << std::endl;

    // read 16 * 10M
    LOG_INFO(" @@@@@@@@@@@@@ read 16 * 10M @@@@@@@@@@@@@@@");
    threads.clear();
    ts = TIME_NOW;
    for (int i = 0; i < thread_num; i++) {
      threads.emplace_back(
          [=](TestKey *k, int *slab_class) {
            bindCore(i);
            if (i == 0)
              LOG_INFO("Start read threads");
            std::string value;
            for (int j = 0; j < write_op_per_thread; j++) {
              // if (j % M == 0) {
              //   LOG_INFO("[thread %d] finish read %d kv", i, j);
              // }
              int key_idx = j + i * write_op_per_thread;
              auto succ = local_engine->read(k[key_idx].to_string(), value);
              ASSERT(succ, "[thread %d] failed to read %d", i, key_idx);
              ASSERT(value.size() == (size_t)slab_class[key_idx] * kSlabSize, "got val length %lu, expected %d",
                     value.size(), slab_class[key_idx] * kSlabSize)
              TestKey expect_val;
              memcpy(expect_val.key, k[key_idx].key, kKeyLength);
              expect_val.key[0] += 1;
              auto cmp = memcmp(value.c_str(), expect_val.key, 16);
              ASSERT(cmp == 0, "expect %.16s, got %.16s", expect_val.key, value.c_str());
            }
            if (i == 0)
              LOG_INFO("End read threads");
          },
          keys, key_slab_class);
    }
    for (auto &th : threads) {
      th.join();
    }
    te = TIME_NOW;
    time_delta = te - ts;
    count = std::chrono::duration_cast<std::chrono::microseconds>(time_delta).count();
    std::cout << "Total time:" << count * 1.0 / 1000 / 1000 << "s" << std::endl;

    // update 16 * 1M
    // 需要更改value的大小
    LOG_INFO(" @@@@@@@@@@@@@ update 16 * 1M @@@@@@@@@@@@@@@");
    threads.clear();
    ts = TIME_NOW;
    for (int i = 0; i < thread_num; i++) {
      threads.emplace_back(
          [=](TestKey *k, int *slab_class) {
            bindCore(i);
            if (i == 0)
              LOG_INFO("Start update thread");
            std::string value;
            for (int j = 0; j < update_op_per_thread; j++) {
              // if (j % M == 0) {
              //   LOG_INFO("[thread %d] finish update %d kv", i, j);
              // }
              int key_idx = j + i * update_op_per_thread;
              uint8_t sc = genValueSlabSize1();
              slab_class[key_idx] = sc;
              value.resize(sc * kSlabSize);
              TestKey new_val;
              memcpy(new_val.key, k[key_idx].key, kKeyLength);
              new_val.key[0] += 1;
              memcpy((char *)value.data(), new_val.key, kKeyLength);
              auto succ = local_engine->write(k[key_idx].to_string(), value);
              ASSERT(succ, "[thread %d] failed to update %d", i, key_idx);
            }
            if (i == 0)
              LOG_INFO("End update threads");
          },
          keys, key_slab_class);
    }
    for (auto &th : threads) {
      th.join();
    }
    te = TIME_NOW;
    time_delta = te - ts;
    count = std::chrono::duration_cast<std::chrono::microseconds>(time_delta).count();
    std::cout << "Total time:" << count * 1.0 / 1000 / 1000 << "s" << std::endl;

    // // read? < 1M
    // LOG_INFO(" @@@@@@@@@@@@@ read 1M @@@@@@@@@@@@@@@");
    // int read_num = 1 * 1000000;
    // int read_op_per_thread = read_num / thread_num;
    // threads.clear();
    // for (int i = 0; i < thread_num; i++) {
    //   threads.emplace_back(
    //       [=](TestKey *k, int *slab_class) {
                // bindCore(i);
    //         LOG_INFO("Start read thread %d", i);
    //         std::string value;
    //         for (int j = 0; j < read_op_per_thread; j++) {
    //           if (j % M == 0) {
    //             LOG_INFO("[thread %d] finish read %d kv", i, j);
    //           }
    //           int key_idx = j + i * read_op_per_thread;
    //           auto succ = local_engine->read(k[key_idx].to_string(), value);
    //           ASSERT(succ, "[thread %d] failed to read %d", i, key_idx);
    //           ASSERT(value.size() == (size_t)slab_class[key_idx] * kSlabSize, "got val length %lu, expected %d",
    //                  value.size(), slab_class[key_idx] * kSlabSize)
    //           TestKey expect_val; memcpy(expect_val.key, k[key_idx].key, kKeyLength); expect_val.key[0] += 1;
    //           auto cmp = memcmp(value.c_str(), expect_val.key, 16);
    //           ASSERT(cmp == 0, "expect %.16s, got %.16s", expect_val.key, value.c_str());
    //         }
    //         LOG_INFO("End read thread %d", i);
    //       },
    //       keys, key_slab_class);
    // }
    // for (auto &th : threads) {
    //   th.join();
    // }

    auto time_end = TIME_NOW;
    time_delta = time_end - time_now;
    count = std::chrono::duration_cast<std::chrono::microseconds>(time_delta).count();
    std::cout << "Total time:" << count * 1.0 / 1000 / 1000 << "s" << std::endl;
  }
}

// delete 16 * 9M
// read deleted 16 * 9M
// write 16 * 9M
void part2(LocalEngine *local_engine, TestKey *keys, int *zipf_index, int *key_slab_class,
           std::vector<std::thread> &threads) {
  LOG_INFO(" ============= part2 delete ===============>");
  {
    auto time_now = TIME_NOW;
    LOG_INFO(" @@@@@@@@@@@@@ delete 16 * 9M @@@@@@@@@@@@@@@");
    threads.clear();
    for (int i = 0; i < thread_num; i++) {
      threads.emplace_back(
          [=](TestKey *k, int *zipf_index, int *slab_class) {
            bindCore(i);
            for (int j = 0; j < delete_op_per_thread; j++) {
              int key_idx = j + i * delete_op_per_thread;
              // if (j % (4 * M) == 0) {
              //   LOG_INFO("[thread %d] finish delete %d M kv", i, j / M);
              // }
              auto succ = local_engine->deleteK(keys[key_idx].to_string());
              ASSERT(succ, "delete key %.16s failed.", keys[key_idx].key);
            }
          },
          keys, zipf_index, key_slab_class);
    }
    for (auto &th : threads) {
      th.join();
    }

    LOG_INFO(" @@@@@@@@@@@@@ read deleted 16 * 9M @@@@@@@@@@@@@@@");
    threads.clear();
    for (int i = 0; i < thread_num; i++) {
      threads.emplace_back(
          [=](TestKey *k, int *zipf_index, int *slab_class) {
            bindCore(i);
            for (int j = 0; j < delete_op_per_thread; j++) {
              int key_idx = j + i * delete_op_per_thread;
              // if (j % (4 * M) == 0) {
              //   LOG_INFO("[thread %d] finish read deleted %d M kv", i, j / M);
              // }
              std::string value;
              bool found = local_engine->read(keys[key_idx].to_string(), value);
              ASSERT(!found, "delete key %.16s failed.", keys[key_idx].key);
            }
          },
          keys, zipf_index, key_slab_class);
    }
    for (auto &th : threads) {
      th.join();
    }

    LOG_INFO(" @@@@@@@@@@@@@ write 16 * 9M @@@@@@@@@@@@@@@");
    threads.clear();
    for (int i = 0; i < thread_num; i++) {
      threads.emplace_back(
          [=](TestKey *k, int *slab_class) {
            bindCore(i);
            if (i == 0)
              LOG_INFO("Start write threads");
            std::string value;
            for (int j = 0; j < delete_op_per_thread; j++) {
              // if (j % M == 0) {
              //   LOG_INFO("[thread %d] finish write %d kv", i, j);
              // }
              int key_idx = j + i * delete_op_per_thread;
              uint8_t sc = genValueSlabSize1();
              slab_class[key_idx] = sc;
              value.resize(sc * kSlabSize);
              TestKey new_val;
              memcpy(new_val.key, k[key_idx].key, kKeyLength);
              new_val.key[0] += 1;
              memcpy((char *)value.c_str(), new_val.key, 16);
              auto succ = local_engine->write(k[key_idx].to_string(), value);
              ASSERT(succ, "[thread %d] failed to write %d", i, key_idx);
            }
            if (i == 0)
              LOG_INFO("End write threads");
          },
          keys, key_slab_class);
    }
    for (auto &th : threads) {
      th.join();
    }

    auto time_end = TIME_NOW;
    auto time_delta = time_end - time_now;
    auto count = std::chrono::duration_cast<std::chrono::microseconds>(time_delta).count();
    std::cout << "Total time:" << count * 1.0 / 1000 / 1000 << "s" << std::endl;
  }
}

// read 16 * 32M
void part3(LocalEngine *local_engine, TestKey *keys, int *zipf_index, int *key_slab_class,
           std::vector<std::thread> &threads) {
  LOG_INFO(" ============= part3 hot data ===============>");
  {
    LOG_INFO(" @@@@@@@@@@@@@ read 16 * 32M @@@@@@@@@@@@@@@");
    auto time_now = TIME_NOW;
    threads.clear();
    for (int i = 0; i < thread_num; i++) {
      threads.emplace_back(
          [=](TestKey *k, int *slab_class) {
            bindCore(i);
            if (i == 0)
              LOG_INFO("Start read threads");
            std::string value;
            for (int j = 0; j < read_op_per_thread; j++) {
              // if (j % M == 0) {
              //   LOG_INFO("[thread %d] finish read %d kv", i, j);
              // }
              int key_idx = (j + i * read_op_per_thread) % insert_num;
              auto succ = local_engine->read(k[key_idx].to_string(), value);
              ASSERT(succ, "[thread %d] failed to read %d", i, key_idx);
              ASSERT(value.size() == (size_t)slab_class[key_idx] * kSlabSize, "got val length %lu, expected %d",
                     value.size(), slab_class[key_idx] * kSlabSize)
              TestKey expect_val;
              memcpy(expect_val.key, k[key_idx].key, kKeyLength);
              expect_val.key[0] += 1;
              auto cmp = memcmp(value.c_str(), expect_val.key, 16);
              ASSERT(cmp == 0, "expect %.16s, got %.16s", expect_val.key, value.c_str());
            }
            if (i == 0)
              LOG_INFO("End read threads");
          },
          keys, key_slab_class);
    }
    for (auto &th : threads) {
      th.join();
    }

    auto time_end = TIME_NOW;
    auto time_delta = time_end - time_now;
    auto count = std::chrono::duration_cast<std::chrono::microseconds>(time_delta).count();
    std::cout << "Total time:" << count * 1.0 / 1000 / 1000 << "s" << std::endl;
  }
}

// 实时获取程序占用的内存，单位：kb
size_t physical_memory_used_by_process()
{
    FILE* file = fopen("/proc/self/status", "r");
    int result = -1;
    char line[128];

    while (fgets(line, 128, file) != nullptr) {
        if (strncmp(line, "VmRSS:", 6) == 0) {
            int len = strlen(line);

            const char* p = line;
            for (; std::isdigit(*p) == false; ++p) {}

            line[len - 3] = 0;
            result = atoi(p);

            break;
        }
    }

    fclose(file);
    return result;
}

int main() {
  LocalEngine *local_engine = new LocalEngine();
  // ip 必须写具体ip，不能直接写localhost和127.0.0.1
  local_engine->start("192.168.200.22", "23627");
  LOG_INFO("Engine Use DRAM Space: %lf GB", ((double)physical_memory_used_by_process())/1024.0/1024.0);
  std::vector<std::thread> threads;
  std::mutex zipf_mutex;

  LOG_INFO(" ============= gen key and zipf index ===============>");
  auto keys = genKey(insert_num);
  int *key_slab_class = new int[insert_num];
  int *zipf_index = new int[read_op_per_thread * thread_num];
  LOG_INFO(" start gen zipf key...");
  for (int i = 0; i < thread_num; i++) {
    threads.emplace_back(
        [i](int *zipf_index) {
          bindCore(i);
          if (i == 0)
            LOG_INFO("Start gen zipf key index");
          Zipf zipf(insert_num, 0x123ab324 * (i + 1), 2);
          for (int j = 0; j < read_op_per_thread; j++) {
            zipf_index[read_op_per_thread * i + j] = zipf.Next();
          }
          if (i == 0)
            LOG_INFO("End gen zipf key index");
        },
        zipf_index);
  }
  for (auto &th : threads) {
    th.join();
  }
  threads.clear();

  LOG_INFO(" end gen zipf key!");

  part1(local_engine, keys, zipf_index, key_slab_class, threads);
  local_engine->Info();
  part2(local_engine, keys, zipf_index, key_slab_class, threads);
  local_engine->Info();
  // part3(local_engine, keys, zipf_index, key_slab_class, threads);
  // local_engine->Info();

  delete[] keys;
  delete[] key_slab_class;
  delete[] zipf_index;

  LOG_INFO("Engine Use DRAM Space: %lf GB", ((double)physical_memory_used_by_process())/1024.0/1024.0);
  local_engine->stop();
  delete local_engine;
  LOG_INFO("Engine Use DRAM Space: %lf GB", ((double)physical_memory_used_by_process())/1024.0/1024.0);
  return 0;
}
