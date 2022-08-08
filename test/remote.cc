#include "../include/kv_engine.h"
#include <unistd.h>
#include <iostream>

using namespace kv;

int main(int argc, char *argv[]) {
  if (argc > 2) {
    std::cout << "usage: ./remote <ip>, no need to specify <port>" << std::endl;
    return 0;
  }
  
  std::string rdma_addr("192.168.200.26");
  std::string rdma_port("22222");

  if (argc == 2) {
    rdma_addr = std::string(argv[1]);
  }

  RemoteEngine *kv_imp = new RemoteEngine();
  assert(kv_imp);
  auto ret = kv_imp->start(rdma_addr, rdma_port);
  assert(ret);
  
  do {
    sleep(1);
  } while (kv_imp->alive());

  kv_imp->stop();
  delete kv_imp;

  return 0;
}
