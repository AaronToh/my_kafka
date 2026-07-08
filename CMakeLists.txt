cmake_minimum_required(VERSION 3.16)
project(my_kafka CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

add_executable(my_kafka src/main.cpp src/protocol.cpp)
