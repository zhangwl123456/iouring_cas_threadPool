cmake_minimum_required(VERSION 3.20)
project(robotaxi_concurrency LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

add_library(robotaxi_core
  src/frame_decoder.cpp
  src/ring_queue.cpp
  src/thread_pool.cpp
)

target_include_directories(robotaxi_core
  PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/include
)

if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
  target_compile_options(robotaxi_core PRIVATE -Wall -Wextra -Wpedantic)
endif()

include(FetchContent)
FetchContent_Declare(
  googletest
  URL https://github.com/google/googletest/archive/refs/tags/v1.14.0.zip
)
set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(googletest)

enable_testing()

add_executable(ring_queue_enqueue_test
  tests/ring_queue_enqueue_test.cpp
)

add_executable(frame_decoder_test
  tests/frame_decoder_test.cpp
)

add_executable(thread_pool_test
  tests/thread_pool_test.cpp
)

target_link_libraries(ring_queue_enqueue_test
  PRIVATE
    robotaxi_core
    GTest::gtest_main
)

target_link_libraries(frame_decoder_test
  PRIVATE
    robotaxi_core
    GTest::gtest_main
)

target_link_libraries(thread_pool_test
  PRIVATE
    robotaxi_core
    GTest::gtest_main
)

include(GoogleTest)
gtest_discover_tests(ring_queue_enqueue_test)
gtest_discover_tests(frame_decoder_test)
gtest_discover_tests(thread_pool_test)
