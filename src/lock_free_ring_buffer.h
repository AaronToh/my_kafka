#pragma once

#include <atomic>
#include <cstddef>
#include <optional>
#include <vector>

template <typename T>
class LockFreeRingBuffer {
public:
    explicit LockFreeRingBuffer(size_t capacity) : capacity_(capacity), buffer_(capacity + 1) {}

    void push(T item) {
        size_t tail = tail_.load(std::memory_order_relaxed);
        while ((tail + 1) % (capacity_ + 1) == head_.load(std::memory_order_acquire)) continue;
        buffer_[tail] = std::move(item);
        size_t newTail = (tail + 1) % (capacity_ + 1);
        tail_.store(newTail, std::memory_order_release);
    }

    T pop() {
        size_t head = head_.load(std::memory_order_relaxed);
        while (head == tail_.load(std::memory_order_acquire)) continue;
        T item = std::move(*buffer_[head]);
        buffer_[head].reset();
        size_t newHead = (head + 1) % (capacity_ + 1);
        head_.store(newHead, std::memory_order_release);
        return item;
    }

private:
    size_t capacity_;
    std::vector<std::optional<T>> buffer_;
    std::atomic<size_t> head_{0};
    std::atomic<size_t> tail_{0};
};
