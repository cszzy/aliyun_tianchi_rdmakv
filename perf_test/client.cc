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
#include "kv_engine.h"
#include "test.h"
#include "zipf.h"

using namespace kv;
using namespace std;

constexpr int thread_num = 16;
constexpr int kKeyNum = 16 * 12000000;
constexpr int write_op_per_thread = kKeyNum / thread_num;
// constexpr int read_write_mix_op = 64 * 100;
constexpr int read_write_mix_op = 64 * 1000000;
constexpr int M = 1024 * 1024;

std::atomic<size_t> miss_times;
std::atomic<size_t> evict_times;

int main() {
  miss_times = 0;
  evict_times = 0;

  LocalEngine *local_engine = new LocalEngine();
  // ip 必须写具体ip，不能直接写localhost和127.0.0.1
  local_engine->start("192.168.200.26", "12344");
  std::vector<std::thread> threads;
  std::mutex zipf_mutex;

  printf(" ============= gen key and zipf index ===============>\n");
  auto keys = genPerfKey(kKeyNum);
  int *zipf_index = new int[read_write_mix_op * 16];
  for (int i = 0; i < read_write_mix_op * 16; i++) {
    zipf_index[i] = zipf(kKeyNum, 10) - 1;
  }

  printf(" ============= start write ==============>\n");
  {
    threads.clear();
    auto time_now = TIME_NOW;
    for (int i = 0; i < thread_num; i++) {
      threads.emplace_back(
          [=](TestKey *k) {
            printf("Start write thread %d\n", i);
            std::string value;
            value.resize(kValueLength);
            for (int j = 0; j < write_op_per_thread; j++) {
              if (j % M == 0 && i == 0) {
                printf("[thread %d] finish write %d K kv\n", i, j / 1024);
              }
              memcpy((char *)value.c_str(), k[i * write_op_per_thread + j].key, 16);
              auto succ = local_engine->write(k[i * write_op_per_thread + j].to_string(), value);
              EXPECT(succ, "[thread %d] failed to write %d", i, i * write_op_per_thread + j);

              if (j == write_op_per_thread / 2 || (j == write_op_per_thread - 1)) {
                // read
                for (int t = j - write_op_per_thread / 2; t <= j; t++) {
                  string read_value;
                  auto succ = local_engine->read(k[i * write_op_per_thread + t].to_string(), read_value);
                  EXPECT(succ, "MIX [thread %d] failed to read %d", i, k[i * write_op_per_thread + t]);
                  auto cmp = memcmp(read_value.c_str(), k[i * write_op_per_thread + t].key, 16);
                  EXPECT(cmp == 0, "expect %s, got %s", k[i * write_op_per_thread + t].key,
                        read_value.c_str());
                }
              }
            }
          },
          keys);
    }
    for (auto &th : threads) {
      th.join();
    }
    auto time_end = TIME_NOW;
    auto time_delta = time_end - time_now;
    auto count = std::chrono::duration_cast<std::chrono::microseconds>(time_delta).count();
    std::cout << "Total time:" << count * 1.0 / 1000 / 1000 << "s" << std::endl;
    std::cout << "miss times: " << miss_times << " evict times: " << evict_times << std::endl;
  }

  printf(" ============= start read & write ===============>\n");
  miss_times = 0;
  evict_times = 0;
  {
    auto time_now = TIME_NOW;
    threads.clear();
    for (int i = 0; i < thread_num; i++) {
      threads.emplace_back(
          [=](TestKey *k, int *zipf_index) {
            // mt19937 gen;
            // gen.seed(random_device()());
            // uniform_int_distribution<mt19937::result_type> dist;
            std::string write_value;
            write_value.resize(kValueLength);

            std::string read_value;
            for (int j = 0; j < read_write_mix_op; j++) {
              auto prob = j % 8;
              if (j % M == 0 && i == 0) {
                printf("[thread %d] finish read&wirte %dK kv\n", i, j/1024);
              }
              if (prob == 0) {
                // wirte;
                memcpy((char *)write_value.c_str(), k[zipf_index[i * read_write_mix_op + j]].key, 16);
                auto succ = local_engine->write(k[zipf_index[i * read_write_mix_op + j]].to_string(), write_value);
                EXPECT(succ, "MIX [thread %d] failed to write %d", i, zipf_index[i * read_write_mix_op + j]);
              } else {
                // read
                auto succ = local_engine->read(k[zipf_index[i * read_write_mix_op + j]].to_string(), read_value);
                EXPECT(succ, "MIX [thread %d] failed to read %d", i, zipf_index[i * read_write_mix_op + j]);
                auto cmp = memcmp(read_value.c_str(), k[zipf_index[i * read_write_mix_op + j]].key, 16);
                EXPECT(cmp == 0, "expect %s, got %s", k[zipf_index[i * read_write_mix_op + j]].key,
                       read_value.c_str());
              }
            }
          },
          keys, zipf_index);
    }
    for (auto &th : threads) {
      th.join();
    }
    auto time_end = TIME_NOW;
    auto time_delta = time_end - time_now;
    auto count = std::chrono::duration_cast<std::chrono::microseconds>(time_delta).count();
    std::cout << "Total time:" << count * 1.0 / 1000 / 1000 << "s" << std::endl;
    std::cout << "miss times: " << miss_times << " evict times: " << evict_times << std::endl;
  }
  local_engine->stop();
  delete local_engine;
  return 0;
}
