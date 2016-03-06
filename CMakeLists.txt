cmake_minimum_required(VERSION 3.3)
project(onecache)
set(PROJECT_NAME_STR onecache)

set(EXT_PROJECTS_DIR ${PROJECT_SOURCE_DIR}/deps)

include_directories("src" "deps/resp" "deps/gtest/src/googletest/googletest/include")
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

add_subdirectory(${EXT_PROJECTS_DIR}/gtest)
enable_testing()
set(PROJECT_TEST_NAME ${PROJECT_NAME_STR}_test)
include_directories(${GTEST_INCLUDE_DIRS})
file(GLOB TEST_SRC_FILES ${PROJECT_SOURCE_DIR}/test/*.cpp
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

add_executable(${PROJECT_TEST_NAME} ${TEST_SRC_FILES})
add_dependencies(${PROJECT_TEST_NAME} googletest)
if(NOT WIN32)
    target_link_libraries(${PROJECT_TEST_NAME}
            ${GTEST_LIBS_DIR}/libgtest.a
            ${GTEST_LIBS_DIR}/libgtest_main.a
            )
else()
    target_link_libraries(${PROJECT_TEST_NAME}
            debug ${GTEST_LIBS_DIR}/DebugLibs/${CMAKE_FIND_LIBRARY_PREFIXES}gtest${CMAKE_FIND_LIBRARY_SUFFIXES}
            optimized ${GTEST_LIBS_DIR}/ReleaseLibs/${CMAKE_FIND_LIBRARY_PREFIXES}gtest${CMAKE_FIND_LIBRARY_SUFFIXES}
            )
    target_link_libraries(${PROJECT_TEST_NAME}
            debug ${GTEST_LIBS_DIR}/DebugLibs/${CMAKE_FIND_LIBRARY_PREFIXES}gtest_main${CMAKE_FIND_LIBRARY_SUFFIXES}
            optimized ${GTEST_LIBS_DIR}/ReleaseLibs/${CMAKE_FIND_LIBRARY_PREFIXES}gtest_main${CMAKE_FIND_LIBRARY_SUFFIXES}
            )
endif()

target_link_libraries(${PROJECT_TEST_NAME} event event_pthreads jemalloc pthread)

add_test(test1 ${PROJECT_TEST_NAME})
