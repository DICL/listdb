cmake_minimum_required(VERSION 3.10 FATAL_ERROR)

project(listdb)

set(CMAKE_CXX_COMPILER "g++")
#set(CMAKE_VERBOSE_MAKEFILE ON)

option(DEBUG "build debug mode." OFF)
option(STRING_KEY "string key mode." OFF)
option(WISCKEY "Store values in Wisckey manner." OFF)

if(DEBUG)
  message("[O] DEBUG MODE.")
  set(CMAKE_C_FLAGS "-Wall -Wsign-compare -g")
else()
  message("[X] RELEASE MODE: DEBUG disabled.")
  #set(CMAKE_C_FLAGS "-Wall -Wsign-compare -pg")
  set(CMAKE_C_FLAGS "-Wall -Wsign-compare -O3 -g -DNDEBUG")
endif(DEBUG)

if(STRING_KEY)
  message("[O] STRING KEY ENABLED.")
  set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -DLISTDB_STRING_KEY")
else()
  message("[X] STRING KEY disabled.")
endif(STRING_KEY)

if(WISCKEY)
  message("[O] WISCKEY ENABLED.")
  set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -DLISTDB_WISCKEY")
else()
  message("[X] WISCKEY disabled.")
endif(WISCKEY)

##
# GFLAGS
find_package(gflags)
if(gflags_FOUND)
  set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -DGFLAGS")
endif()

##
# Compiler options
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wfatal-errors ")
set(CMAKE_CXX_FLAGS "${CMAKE_C_FLAGS} -std=c++17 -march=native ")

##
# Link options
set(LINK_FLAGS "-lpthread")

##
# Include directories
include_directories(${PROJECT_SOURCE_DIR}/include)
include_directories(${PROJECT_SOURCE_DIR})
#include_directories(/home/wkim/Projects/libcds-2.3.3)

##
# liblistdb
#set(LISTDB_SRC_DIR ${PROJECT_SOURCE_DIR}/listdb)
##file(GLOB_RECURSE COMMON_FILE ${LISTDB_SRC_DIR}/*.cc)
##list(REMOVE_ITEM COMMON_FILE *_test.cc)
##message(${COMMON_FILE})
##add_library(listdb SHARED ${COMMON_FIlistdb
add_library(listdb STATIC
  #listdb/lsm/table.cc
  #listdb/listdb.cc
  listdb/lib/murmur3.cc
  listdb/lib/sha1.cc
  )
target_link_libraries(listdb
  pthread
  pmemobj
  pmempool
  numa
  jemalloc
  )

# test
#file(GLOB_RECURSE test_srcs ${LISTDB_SRC_DIR}/*_test.cc)
if(STRING_KEY)
set(test_srcs
  listdb/pmem/pmem_test.cc
  listdb/lib/numa_test.cc
  )
else()
set(test_srcs
  listdb/pmem/pmem_test.cc
  listdb/lib/numa_test.cc
  listdb/db_client_test.cc
  listdb/index/braided_pmem_skiplist_test.cc
  )
endif(STRING_KEY)
foreach (test_src ${test_srcs})
  get_filename_component(test_name ${test_src} NAME_WE)
  add_executable(${test_name} ${test_src}
    #listdb/listdb.cc
    )
  target_link_libraries(${test_name}
    listdb
    pthread
    pmemobj
    pmempool
    numa
    jemalloc
    stdc++fs
    )
  #add_executable(${test_name} ${test_src}
  #  listdb/user_key.cc
  #  listdb/lsm/table.cc)
  #target_link_libraries(${test_name} listdb)
  #target_link_libraries(${test_name}
  #  pthread
  #  numa
  #  jemalloc
  #  )
endforeach()

# ubench
#file(GLOB_RECURSE ubench_srcs ${LISTDB_SRC_DIR}/ubench/*.cc)
set(ubench_srcs
  listdb/ubench/eee.cc
  listdb/ubench/rw_ratio.cc
  listdb/ubench/tune_l1.cc
  listdb/ubench/string_kv_test.cc
  )
foreach (ubench_src ${ubench_srcs})
  get_filename_component(ubench_name ${ubench_src} NAME_WE)
  add_executable(${ubench_name} ${ubench_src}
    #listdb/listdb.cc
    #listdb/lsm/table.cc
    )
  #target_link_libraries(${ubench_name} listdb)
  target_link_libraries(${ubench_name}
    listdb
    pthread
    numa
    jemalloc
    pmemobj
    stdc++fs
    tbb
    #/home/wkim/Projects/libcds-2.3.3/build/bin/libcds.so
    )
  #target_include_directories(${ubench_name} PUBLIC
  #  /home/wkim/Projects/libcds-2.3.3
  #  )
endforeach()

# db_bench
set(db_bench_srcs
  listdb/tools/db_bench.cc
  )
foreach (db_bench_src ${db_bench_srcs})
  get_filename_component(db_bench_name ${db_bench_src} NAME_WE)
  add_executable(${db_bench_name} ${db_bench_src}
    )
  target_link_libraries(${db_bench_name}
    listdb
    pthread
    numa
    jemalloc
    pmemobj
    stdc++fs
    tbb
    )
  if(gflags_FOUND)
    target_link_libraries(${db_bench_name} ${gflags_LIBRARIES})
  endif()
endforeach()

###
## Benchmarks
#file(GLOB BENCHMAKR_SRC ${PROJECT_SOURCE_DIR}/bench/*.cc)
#foreach (BENCHMARK ${BENCHMAKR_SRC})
#  get_filename_component(BENCHMARK_NAME ${BENCHMARK} NAME_WE)
#  add_executable(${BENCHMARK_NAME} ${BENCHMARK})
#  target_link_libraries(${BENCHMARK_NAME} ${LINK_FLAGS})
#  target_link_libraries(${BENCHMARK_NAME} brdb)
#endforeach()

###
## YCSB 
#function(ycsb_benchmark ycsb_test_file)
#	# get_filename_component(ycsb_test_target_name "${ycsb_test_file}" NAME_WE)
#	get_filename_component(ycsb_test_target_name "ycsb" NAME_WE)
#	add_executable("${ycsb_test_target_name}" "")
#	target_sources("${ycsb_test_target_name}"
#		PRIVATE
#		"ycsb/src/brdb_client.cc"
#		"ycsb/core/core_workload.cc"
#		"${ycsb_test_file}"
#		)
#	target_link_libraries("${ycsb_test_target_name}" brdb)
#  target_link_libraries(${ycsb_test_target_name} stdc++fs)
#endfunction(ycsb_benchmark)
#ycsb_benchmark("${PROJECT_SOURCE_DIR}/ycsb/src/main.cc")


###
## YCSB - wookhee
#add_executable(ycsb-brdb benchmarks/ycsb.cpp)
#target_link_libraries(ycsb-brdb brdb
#tbb
#tcmalloc)
