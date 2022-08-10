#pragma once

#include <mutex>
#include <shared_mutex>
#include <condition_variable>

namespace kv {

class RWLock {
 public:
    RWLock() : m_readCount(0), m_writeCount(0), m_isWriting(false) {
    }
    virtual ~RWLock() = default;

    void lockWrite() {
        std::unique_lock<std::mutex> gurad(m_Lock);
        ++m_writeCount;
        m_writeCond.wait(gurad, [=] { return (0 == m_readCount) && !m_isWriting; });
        m_isWriting = true;
    }

    void unlockWrite() {
        std::unique_lock<std::mutex> gurad(m_Lock);
        m_isWriting = false;
        if (0 == (--m_writeCount)) {
            // All read can go on
            m_readCond.notify_all();
        } else {
            // One write can go on
            m_writeCond.notify_one();
        }
    }

    void lockRead() {
        std::unique_lock<std::mutex> gurad(m_Lock);
        m_readCond.wait(gurad, [=] { return 0 == m_writeCount; });
        ++m_readCount;
    }

    void unlockRead() {
        std::unique_lock<std::mutex> gurad(m_Lock);
        if (0 == (--m_readCount)
            && m_writeCount > 0) {
            // One write can go on
            m_writeCond.notify_one();
        }
    }

 private:
    volatile int m_readCount;
    volatile int m_writeCount;
    volatile bool m_isWriting;
    std::mutex m_Lock;
    std::condition_variable m_readCond;
    std::condition_variable m_writeCond;
};

// const int a = sizeof(RWLock);

class ReadGuard {
 public:
    ReadGuard(RWLock& lock) : m_lock(lock) {
        m_lock.lockRead();
    }

    ~ReadGuard() {
        m_lock.unlockRead();
    }

 private:
    ReadGuard(const ReadGuard&);
    ReadGuard& operator=(const ReadGuard&);

 private:
    RWLock &m_lock;
};

class WriteGuard {
 public:
    WriteGuard(RWLock &lock) : m_lock(lock) {
        m_lock.lockWrite();
    }

    ~WriteGuard() {
        m_lock.unlockWrite();
    }

 private:
    WriteGuard(const WriteGuard&);
    WriteGuard& operator=(const WriteGuard&);

 private:
  RWLock& m_lock;
};

typedef std::shared_mutex MyLock;
typedef std::unique_lock<MyLock> WriteLock;
typedef std::shared_lock<MyLock> ReadLock;

} /* namespace linduo */