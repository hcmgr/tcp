cmake_minimum_required(VERSION 3.31)
project(tcp)
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
file(GLOB SOURCES "${CMAKE_SOURCE_DIR}/src/*.cpp")
add_executable(listen ${SOURCES})

## includes - libevent
target_include_directories(listen PRIVATE
    /opt/homebrew/opt/libevent/include
)

## linked libraries - libevent
target_link_libraries(listen PRIVATE
    /opt/homebrew/opt/libevent/lib/libevent.dylib
)