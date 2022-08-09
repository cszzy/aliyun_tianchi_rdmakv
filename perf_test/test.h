#pragma once

#include <cstdint>
#include <cstdio>
#include <cstring>
#include <iostream>
#include <random>
#include <string>
#include <vector>

using namespace kv;
using namespace std;

constexpr int kKeyLength = 16;
constexpr int kValueLength = 128;

#define OUTPUT(format, ...) printf(format, ##__VA_ARGS__)
#define ASSERT(condition, format, ...)                                                                      \
  if (!(condition)) {                                                                                       \
    OUTPUT("\033[;31mAssertion ' %s ' Failed!\n%s:%d: " format "\n\033[0m", #condition, __FILE__, __LINE__, \
           ##__VA_ARGS__);                                                                                  \
    exit(1);                                                                                                \
  }

#define EXPECT(condition, format, ...)                                                                            \
  if (!(condition)) {                                                                                             \
    OUTPUT("\033[;33mExpect ' %s ' \n%s:%d: " format "\n\033[0m", #condition, __FILE__, __LINE__, ##__VA_ARGS__); \
  }

struct TestKey {
  char key[kKeyLength]{};
  std::string to_string() { return std::string(key, kKeyLength); }
};

inline TestKey *genPerfKey(int num) {
  mt19937 gen;
  gen.seed(random_device()());
  uniform_int_distribution<mt19937::result_type> dist;
  TestKey *keys = new TestKey[num];

  for (int i = 0; i < num; i++) {
    if (i % 10000000 == 0) {
      printf("cur i %d\n", i);
    }
    auto &&tmp = to_string(dist(gen));
    tmp.resize(16);
    memcpy(keys[i].key, tmp.c_str(), 16);
  }
  return keys;
}

inline vector<string> genKey(int num) {
  mt19937 gen;
  gen.seed(random_device()());
  uniform_int_distribution<mt19937::result_type> dist;
  vector<string> keys;
  for (int i = 0; i < num; i++) {
    keys.emplace_back();
    keys.back().resize(kKeyLength);
    uint8_t *data = (uint8_t *)(keys.back().c_str());
    for (int i = 0; i < kKeyLength; i++) {
      data[i] = (dist(gen) % 26) + 'a';
    }
  }
  return keys;
}

inline vector<string> genValue(int num) {
  mt19937 gen;
  gen.seed(random_device()());
  uniform_int_distribution<mt19937::result_type> dist;
  vector<string> keys;
  for (int i = 0; i < num; i++) {
    keys.emplace_back();
    keys.back().resize(kValueLength);
    uint8_t *data = (uint8_t *)(keys.back().c_str());
    for (int i = 0; i < kValueLength; i++) {
      data[i] = (dist(gen) % 26) + 'a';
    }
  }
  return keys;
}