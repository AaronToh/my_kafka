#pragma once

#include <condition_variable>
#include <cstddef>
#include <mutex>
#include <optional>
#include <vector>

template <typename T>
class CondVarRingBuffer {
public:
    explicit CondVarRingBuffer(size_t capacity) : capacity_(capacity), buffer_(capacity) {}

    void push(T item) {
        std::unique_lock<std::mutex> lock(mutex_);
        notFull_.wait(lock, [this] { return count_ < capacity_; });

        buffer_[tail_] = std::move(item);
        tail_ = (tail_ + 1) % capacity_;
        count_++;
        lock.unlock();
        notEmpty_.notify_one();
    }

    T pop() {
        std::unique_lock<std::mutex> lock(mutex_);
        notEmpty_.wait(lock, [this] { return count_ > 0; });

        T item = std::move(*buffer_[head_]);
        buffer_[head_].reset();
        head_ = (head_ + 1) % capacity_;
        count_--;
        lock.unlock();
        notFull_.notify_one();

        return item;
    }

private:
    size_t capacity_;
    std::vector<std::optional<T>> buffer_;
    size_t head_ = 0;
    size_t tail_ = 0;
    size_t count_ = 0;
    std::mutex mutex_;
    std::condition_variable notEmpty_;
    std::condition_variable notFull_;
};
