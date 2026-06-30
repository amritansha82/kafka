#pragma once
#include <queue>
#include <mutex>
#include <condition_variable>
#include "connection_handler.hpp"
template<typename T>
class safe_queue {
private:
    std::queue<T> q;
    std::mutex m;
    std::condition_variable cv;

public:
    void push(const T& value) {
        std::lock_guard<std::mutex> lock(m);
        q.push(value);
        cv.notify_one();
    }
    void push(T&& value) {
        std::lock_guard<std::mutex> lock(m);
        q.push(std::move(value));
        cv.notify_one();
    }
    T pop() {
        std::unique_lock<std::mutex> lock(m);
        
        while (q.empty()) {
            cv.wait(lock);
        }
        
        T value = std::move(q.front());
        q.pop();
        return value;
    }
    bool empty() {
        std::lock_guard<std::mutex> lock(m);
        return q.empty();
    }
};

extern safe_queue<ConnectionState*> request_channel;