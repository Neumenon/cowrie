cmake_minimum_required(VERSION 3.10)
project(cowrie VERSION 0.1.0 LANGUAGES C)

# Options
option(BUILD_SHARED_LIBS "Build shared libraries" OFF)
option(BUILD_TESTS "Build tests" ON)
option(COVERAGE "Enable coverage reporting" OFF)
if(COVERAGE)
  set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fprofile-arcs -ftest-coverage -O0")
  set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -lgcov")
endif()

# C standard
set(CMAKE_C_STANDARD 11)
set(CMAKE_C_STANDARD_REQUIRED ON)

# Include directories
include_directories(include)

# Gen1 library
add_library(cowrie_gen1
    src/gen1.c
)
target_include_directories(cowrie_gen1 PUBLIC include)

# Gen2 library
add_library(cowrie_gen2
    src/gen2.c
    src/json.c
)
target_include_directories(cowrie_gen2 PUBLIC include)

# zlib is required for framed compression (gzip)
find_package(ZLIB REQUIRED)
target_link_libraries(cowrie_gen2 PUBLIC ZLIB::ZLIB)

# Optional: zstd support for framed compression
find_package(PkgConfig QUIET)
if(PkgConfig_FOUND)
    pkg_check_modules(ZSTD QUIET libzstd)
endif()
if(ZSTD_FOUND)
    target_compile_definitions(cowrie_gen2 PUBLIC COWRIE_HAS_ZSTD)
    target_include_directories(cowrie_gen2 PUBLIC ${ZSTD_INCLUDE_DIRS})
    target_link_libraries(cowrie_gen2 PUBLIC ${ZSTD_LIBRARIES})
endif()

# Combined library (convenience target)
add_library(cowrie INTERFACE)
target_link_libraries(cowrie INTERFACE cowrie_gen1 cowrie_gen2)

# Install targets
install(TARGETS cowrie_gen1 cowrie_gen2
    LIBRARY DESTINATION lib
    ARCHIVE DESTINATION lib
)
install(FILES
    include/cowrie_gen1.h
    include/cowrie_gen2.h
    include/cowrie_json.h
    DESTINATION include
)

# Tests
if(BUILD_TESTS)
    enable_testing()

    # Gen1 tests
    add_executable(test_gen1 tests/test_gen1.c)
    target_link_libraries(test_gen1 cowrie_gen1)
    add_test(NAME test_gen1 COMMAND test_gen1)

    # Gen2 tests
    add_executable(test_gen2 tests/test_gen2.c)
    target_link_libraries(test_gen2 cowrie_gen2)
    add_test(NAME test_gen2 COMMAND test_gen2)

    # JSON bridge tests
    add_executable(test_json tests/test_json.c)
    target_link_libraries(test_json cowrie_gen2)
    add_test(NAME test_json COMMAND test_json)

    # Truth table tests
    add_executable(test_truth_table tests/test_truth_table.c)
    target_link_libraries(test_truth_table cowrie_gen2)
    target_link_libraries(test_truth_table m)
    add_test(NAME test_truth_table COMMAND test_truth_table)

    # Mutation tests
    add_executable(test_mutation tests/test_mutation.c)
    target_link_libraries(test_mutation cowrie_gen2)
    add_test(NAME test_mutation COMMAND test_mutation)
endif()
