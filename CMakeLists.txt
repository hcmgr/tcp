cmake_minimum_required(VERSION 3.31)
project(tcp)
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
find_package(GTest REQUIRED)
enable_testing()

#######################################################################################
## listen
#######################################################################################

file(GLOB SOURCES "${CMAKE_SOURCE_DIR}/src/*.cpp")
add_executable(listen ${SOURCES})

## include libevent 
target_include_directories(listen PRIVATE /usr/include)
target_link_libraries(listen PRIVATE /usr/lib/x86_64-linux-gnu/libevent.so)

#######################################################################################
## unit tests
#######################################################################################

## sources, excluding main.cpp
set(UNIT_TEST_SOURCES ${SOURCES})
list(REMOVE_ITEM UNIT_TEST_SOURCES "${CMAKE_SOURCE_DIR}/src/main.cpp")
file(GLOB UNIT_TEST_FILES "${CMAKE_SOURCE_DIR}/test/*_test.cpp")

add_executable(unit_tests ${UNIT_TEST_SOURCES} ${UNIT_TEST_FILES})

## include libevent and gtest
target_include_directories(unit_tests PRIVATE /usr/include)
target_link_libraries(unit_tests PRIVATE /usr/lib/x86_64-linux-gnu/libevent.so GTest::gtest GTest::gtest_main)
include(GoogleTest)
gtest_discover_tests(unit_tests)

#######################################################################################
## integration tests
#######################################################################################