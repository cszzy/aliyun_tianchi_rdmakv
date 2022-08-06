#include <iostream>
#include <random>
#include <set>
#include <string>
#include <thread>
#include "../include/kv_engine.h"

using namespace kv;
using namespace std;

std::mutex mtx;

std::string random_string(std::size_t length) {
  std::lock_guard<std::mutex> guard{mtx};
  const std::string CHARACTERS =
      "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";

  std::random_device random_device;
  std::mt19937 generator(random_device());
  std::uniform_int_distribution<> distribution(0, CHARACTERS.size() - 1);

  std::string random_string;

  for (std::size_t i = 0; i < length; ++i) {
    random_string += CHARACTERS[distribution(generator)];
  }

  return random_string;
}

// 增加线程号，防止不同线程生成相同的key
std::string gen_key(int t) {
  string tt = to_string(t); 
  return tt + random_string(16 - tt.size()); 
}

std::string gen_val() { return random_string(128); }

const int sz = 100000;

const int num_tds = 4;
thread threads1[num_tds], threads2[num_tds];

unordered_map<string, string> keyvalues[num_tds];

void *put(LocalEngine *kv, int t) {
  for (int i = 0; i < sz; i++) {
    string k, v;
    k = gen_key(t), v = gen_val();
    // cout << "key: " << k << " value: " << v << endl;
    kv->write(k, v);
    keyvalues[t][k] = v;
    // {
    //   string vv;
    //   kv->read(k, vv);
    //   if (keyvalues[t][k] != vv) {
    //     printf("put thread#%d, error\n", t);
    //   }
    // }
    if (i % 1000 == 0) {
      printf("put: \t%d\r", i);
    }
  }
  return nullptr;
}

void *pget(LocalEngine *kv, int t) {
  size_t i = 0;
  for (const auto &s : keyvalues[t]) {
    string v;
    kv->read(s.first, v);
    if (v != s.second) {
      // printf("error! [%ld] expect: %s, but get: %s\n", i, s.second.c_str(), v.c_str());
      // for (int j = 0; j < num_tds; j++) {
      //   if (keyvalues[j].find(s.first) != keyvalues[j].end()) {
      //     printf("j = %d\n", j);
      //     printf("key: %s, value: %s\n", s.first.c_str(), keyvalues[j][s.first].c_str());
      //   }
      // }
      int index = std::hash<std::string>()(s.first) & (SHARDING_NUM - 1);
      printf("thraed#%d, index: %d\n",t, index);
      // std::this_thread::sleep_for(std::chrono::seconds(1));
    }
    ++i;
  }
}

int main(int argc, char *argv[]) {
  if (argc > 2) {
    std::cout << "usage: ./local <ip>, no need to specify <port>" << std::endl;
    return 0;
  }

  std::string rdma_addr("192.168.21.128");
  std::string rdma_port("22222");

  if (argc == 2) {
    rdma_addr = std::string(argv[1]);
  }

  LocalEngine *kv_imp = new LocalEngine();
  assert(kv_imp);
  auto ret = kv_imp->start(rdma_addr, rdma_port);
  assert(ret);  // 不能直接assert(kv_imp->start(rdma_addr,
                // rdma_port))!assert在运行期间无效！

  for (int i = 0; i < num_tds; i++) {  // 16 个线程插入
    cout << "start put thread " << i << endl;
    threads1[i] = thread(put, kv_imp, i);
  }

  for (auto &t : threads1) {
    t.join();
  }

  cout << "put done" << endl;

  sleep(5);

  for (int i = 0; i < num_tds; i++) {
    cout << "start get thread " << i << endl;
    threads2[i] = thread(pget, kv_imp, i);
  }

  for (auto &t : threads2) {
    t.join();
  }

  cout << "get done" << endl;

  // for (int i = 0; i < num_tds; i++) {
  //   cout << "thread " << i << " " << kv_imp->m_kvs_[i].size() << endl;
  // }

  // while (1)
  //   ;

  kv_imp->stop();
  delete kv_imp;

  return 0;
}


// #include <iostream>
// #include <random>
// #include <set>
// #include <string>
// #include <thread>
// #include "include/kv_engine.h"

// using namespace kv;
// using namespace std;

// std::mutex mtx;

// std::string random_string(std::size_t length) {
//   std::lock_guard<std::mutex> guard{mtx};
//   const std::string CHARACTERS =
//       "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";

//   std::random_device random_device;
//   std::mt19937 generator(random_device());
//   std::uniform_int_distribution<> distribution(0, CHARACTERS.size() - 1);

//   std::string random_string;

//   for (std::size_t i = 0; i < length; ++i) {
//     random_string += CHARACTERS[distribution(generator)];
//   }

//   return random_string;
// }

// std::string gen_key() { return random_string(16); }

// std::string gen_val() { return random_string(128); }

// const int sz = 100000;

// const int num_tds = 1;
// thread threads1[num_tds], threads2[num_tds];

// vector<string> keys[num_tds];
// vector<string> values[num_tds];

// void *put(LocalEngine *kv, int t) {
//   for (int i = 0; i < sz; i++) {
//     string k, v;
//     k = gen_key(), v = gen_val();
//     // cout << "key: " << k << " value: " << v << endl;
//     kv->write(k, v);
//     keys[t].emplace_back(k);
//     values[t].emplace_back(v);
//     if (i % 1000 == 0) {
//       printf("put: \t%d\r", i);
//     }
//   }
//   return nullptr;
// }

// void *pget(LocalEngine *kv, int t) {
//   for (int i = 0; i < keys[t].size(); i++) {
//   // for (const auto &s : keys[t]) {
//     string s = keys[t][i];
//     string v;
//     kv->read(s, v);
//     if (v != values[t][i]) {
//       printf("error! [%d] expect: %s, but get: %s\n", i, values[t][i].c_str(), v.c_str());
//       for (int j = 0; j < values[t].size(); j++) {
//         if (values[t][j] == v) {
//           printf("the same as [%d]\n", j);
//           // break;
//         }
//       }
//     }
//     // cout << "get " << s << " : " << v << endl;
//   }
// }

// int main(int argc, char *argv[]) {
//   if (argc > 2) {
//     std::cout << "usage: ./local <ip>, no need to specify <port>" << std::endl;
//     return 0;
//   }

//   std::string rdma_addr("127.0.0.1");
//   std::string rdma_port("33300");

//   if (argc == 2) {
//     rdma_addr = std::string(argv[1]);
//   }

//   LocalEngine *kv_imp = new LocalEngine();
//   assert(kv_imp);
//   auto ret = kv_imp->start(rdma_addr, rdma_port);
//   assert(ret);  // 不能直接assert(kv_imp->start(rdma_addr,
//                 // rdma_port))!assert在运行期间无效！

//   for (int i = 0; i < num_tds; i++) {  // 16 个线程插入
//     cout << "start put thread " << i << endl;
//     threads1[i] = thread(put, kv_imp, i);
//   }

//   for (auto &t : threads1) {
//     t.join();
//   }

//   cout << "put done" << endl;

//   for (int i = 0; i < num_tds; i++) {
//     cout << "start get thread " << i << endl;
//     threads2[i] = thread(pget, kv_imp, i);
//   }

//   for (auto &t : threads2) {
//     t.join();
//   }

//   cout << "get done" << endl;

//   // for (int i = 0; i < num_tds; i++) {
//   //   cout << "thread " << i << " " << kv_imp->m_kvs_[i].size() << endl;
//   // }

//   while (1)
//     ;

//   kv_imp->stop();
//   delete kv_imp;

//   return 0;
// }
