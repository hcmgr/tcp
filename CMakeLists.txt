cmake_minimum_required(VERSION 3.31)
project(tcp)
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
find_package(GTest REQUIRED)

find_path(LIBEVENT_INCLUDE_DIR event2/event.h HINTS /opt/homebrew/include /usr/local/include /usr/include)
find_library(LIBEVENT_LIB event HINTS /opt/homebrew/lib /usr/local/lib /usr/lib/x86_64-linux-gnu)
find_library(LIBEVENT_PTHREADS_LIB event_pthreads HINTS /opt/homebrew/lib /usr/local/lib /usr/lib/x86_64-linux-gnu)
enable_testing()

file(GLOB SOURCES "${CMAKE_SOURCE_DIR}/src/*.cpp")

#######################################################################################
## listen
#######################################################################################

## add_executable(listen ${SOURCES})

## include libevent 
## target_include_directories(listen PRIVATE /usr/include)
## target_link_libraries(listen PRIVATE /usr/lib/x86_64-linux-gnu/libevent.so)

#######################################################################################
## unit tests
#######################################################################################

## sources, excluding main.cpp
set(UNIT_TEST_SOURCES ${SOURCES})
list(REMOVE_ITEM UNIT_TEST_SOURCES "${CMAKE_SOURCE_DIR}/src/main.cpp")
file(GLOB UNIT_TEST_FILES "${CMAKE_SOURCE_DIR}/test/*.cpp")

add_executable(unit_tests ${UNIT_TEST_SOURCES} ${UNIT_TEST_FILES})

## include libevent and gtest
target_include_directories(unit_tests PRIVATE ${LIBEVENT_INCLUDE_DIR})
target_link_libraries(unit_tests PRIVATE ${LIBEVENT_LIB} ${LIBEVENT_PTHREADS_LIB} GTest::gtest GTest::gtest_main)
include(GoogleTest)
gtest_discover_tests(unit_tests)

#######################################################################################
## integration tests
#######################################################################################

file(GLOB INTEGRATION_TEST_FILES CONFIGURE_DEPENDS "${CMAKE_SOURCE_DIR}/test/integration/*.cpp")

add_executable(integration_tests ${UNIT_TEST_SOURCES} ${INTEGRATION_TEST_FILES})

target_include_directories(integration_tests PRIVATE ${LIBEVENT_INCLUDE_DIR})
target_link_libraries(integration_tests PRIVATE ${LIBEVENT_LIB} ${LIBEVENT_PTHREADS_LIB} GTest::gtest GTest::gtest_main)
gtest_discover_tests(integration_tests)
