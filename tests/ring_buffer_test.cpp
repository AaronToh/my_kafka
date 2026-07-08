#include <cassert>
#include <chrono>
#include <iostream>
#include <thread>
#include <vector>

#include "ring_buffer.h"

void testSingleThreaded() {
    RingBuffer<int> rb(3);

    rb.push(1);
    rb.push(2);
    rb.push(3);
    assert(rb.pop() == 1);
    assert(rb.pop() == 2);
    assert(rb.pop() == 3);

    // push/pop past capacity repeatedly to exercise index wraparound
    for (int i = 0; i < 10; i++) {
        rb.push(i);
        assert(rb.pop() == i);
    }
}

void testThreaded() {
    const int capacity = 3;
    const int itemCount = 20;

    RingBuffer<int> rb(capacity);
    std::vector<int> consumed;
    consumed.reserve(itemCount);

    std::thread producer([&] {
        for (int i = 0; i < itemCount; i++) {
            rb.push(i);
        }
    });

    // Head start so the producer fills the buffer and actually blocks on
    // push (capacity < itemCount) before the consumer starts draining it.
    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    std::thread consumer([&] {
        for (int i = 0; i < itemCount; i++) {
            consumed.push_back(rb.pop());
        }
    });

    producer.join();
    consumer.join();

    for (int i = 0; i < itemCount; i++) {
        assert(consumed[i] == i);
    }
}

int main() {
    testSingleThreaded();
    testThreaded();

    std::cout << "all tests passed\n";
    return 0;
}
