#pragma once

#include <mutex>
#include <queue>
#include <thread>
#include <atomic>
#include "rdma_conn.h"
#include "conqueue.h"

namespace kv {

/* The RDMA connection queue */
class ConnQue {
 public:
  ConnQue() {}

  void enqueue(RDMAConnection *conn) {
    m_queue_.enqueue(conn);
  };

  RDMAConnection *dequeue() {
    RDMAConnection *conn;
    while (!m_queue_.try_dequeue(conn)) {
    }
    return conn;
  }

 private:
  moodycamel::ConcurrentQueue<RDMAConnection *> m_queue_;
};

/* The RDMA connection manager */
class ConnectionManager {
 public:
  ConnectionManager() {}

  ~ConnectionManager() {
    // TODO: release resources;
  }

  int init(const std::string ip, const std::string port, uint32_t rpc_conn_num,
           uint32_t one_sided_conn_num);
  int register_remote_memory(uint64_t &addr, uint32_t &rkey, uint64_t size);
  int remote_read(void *ptr, uint32_t size, uint64_t remote_addr,
                  uint32_t rkey);
  int remote_write(void *ptr, uint32_t size, uint64_t remote_addr,
                   uint32_t rkey);

 private:
  ConnQue *m_rpc_conn_queue_;
  ConnQue *m_one_sided_conn_queue_;
};

};  // namespace kv