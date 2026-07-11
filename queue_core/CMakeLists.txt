cmake_minimum_required(VERSION 3.10)
project(QueueServer VERSION 1.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED True)

# Find threads
find_package(Threads REQUIRED)

# Add executable
add_executable(queue_server 
    src/job_queue.cpp 
    src/server.cpp
)

# Link threads
target_link_libraries(queue_server PRIVATE Threads::Threads)
