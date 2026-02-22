cmake_minimum_required(VERSION 3.10)
project(sjson VERSION 0.1.0 LANGUAGES C)

# Options
option(BUILD_SHARED_LIBS "Build shared libraries" OFF)
option(BUILD_TESTS "Build tests" ON)

# C standard
set(CMAKE_C_STANDARD 11)
set(CMAKE_C_STANDARD_REQUIRED ON)

# Include directories
include_directories(include)

# Gen1 library
add_library(sjson_gen1
    src/gen1.c
)
target_include_directories(sjson_gen1 PUBLIC include)

# Gen2 library
add_library(sjson_gen2
    src/gen2.c
    src/json.c
)
target_include_directories(sjson_gen2 PUBLIC include)

# Combined library (convenience target)
add_library(sjson INTERFACE)
target_link_libraries(sjson INTERFACE sjson_gen1 sjson_gen2)

# Install targets
install(TARGETS sjson_gen1 sjson_gen2
    LIBRARY DESTINATION lib
    ARCHIVE DESTINATION lib
)
install(FILES
    include/sjson_gen1.h
    include/sjson_gen2.h
    include/sjson_json.h
    DESTINATION include
)

# Tests
if(BUILD_TESTS)
    enable_testing()

    # Gen1 tests
    add_executable(test_gen1 tests/test_gen1.c)
    target_link_libraries(test_gen1 sjson_gen1)
    add_test(NAME test_gen1 COMMAND test_gen1)

    # Gen2 tests
    add_executable(test_gen2 tests/test_gen2.c)
    target_link_libraries(test_gen2 sjson_gen2)
    add_test(NAME test_gen2 COMMAND test_gen2)

    # JSON bridge tests
    add_executable(test_json tests/test_json.c)
    target_link_libraries(test_json sjson_gen2)
    add_test(NAME test_json COMMAND test_json)
endif()
