cmake_minimum_required(VERSION 3.3)
project(onecache)

include_directories("src" "deps/resp")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11 -ggdb")

set(SOURCE_FILES
    src/tinyxml/tinystr.cpp
    src/tinyxml/tinystr.h
    src/tinyxml/tinyxml.cpp
    src/tinyxml/tinyxml.h
    src/tinyxml/tinyxmlerror.cpp
    src/tinyxml/tinyxmlparser.cpp
    src/util/hash.cpp
    src/util/hash.h
    src/util/iobuffer.cpp
    src/util/iobuffer.h
    src/util/locker.cpp
    src/util/locker.h
    src/util/logger.cpp
    src/util/logger.h
    src/util/objectpool.h
    src/util/queue.h
    src/util/string.h
    src/util/tcpserver.cpp
    src/util/tcpserver.h
    src/util/tcpsocket.cpp
    src/util/tcpsocket.h
    src/util/thread.cpp
    src/util/thread.h
    src/util/vector.h
    src/cmdhandler.cpp
    src/cmdhandler.h
    src/command.cpp
    src/command.h
    src/eventloop.cpp
    src/eventloop.h
    src/main.cpp
    src/monitor.cpp
    src/monitor.h
    src/non-portable.cpp
    src/non-portable.h
    src/proxymanager.cpp
    src/proxymanager.h
    src/redis-proxy-config.cpp
    src/redis-proxy-config.h
    src/redis-servant-select.cpp
    src/redis-servant-select.h
    src/redisproto.cpp
    src/redisproto.h
    src/redisproxy.cpp
    src/redisproxy.h
    src/redisservant.cpp
    src/redisservant.h
    src/redisservantgroup.cpp
    src/redisservantgroup.h
    src/top-key.cpp
    src/top-key.h)

add_executable(onecache ${SOURCE_FILES})
target_link_libraries(onecache event event_pthreads jemalloc pthread)