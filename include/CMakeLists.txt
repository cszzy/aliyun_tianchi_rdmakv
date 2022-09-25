set(BASE_INCLUDE 
    bitmap.h conqueue.h lru_cache.h page.h rdma_conn_manager.h rwlock.h kv_engine.h msg.h rdma_conn.h rdma_mem_pool.h spinlock.h clock_cache.h)

install(FILES   ${BASE_INCLUDE}
    DESTINATION include)