cmake_minimum_required(VERSION 3.16)
project(my_kafka CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

find_package(Threads REQUIRED)

add_executable(my_kafka src/main.cpp src/protocol.cpp)
target_link_libraries(my_kafka PRIVATE Threads::Threads)

add_executable(ring_buffer_test tests/ring_buffer_test.cpp)
target_include_directories(ring_buffer_test PRIVATE src)
target_link_libraries(ring_buffer_test PRIVATE Threads::Threads)
