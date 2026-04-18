# GLYPH codec — token-efficient serialization for LLM tool calls.
# Partial C port of go/glyph/. See README.md for details.

add_library(glyph STATIC
    src/glyph.c
    src/json.c
    src/decimal128.c
    src/schema_evolution.c
    src/stream_validator.c
)
target_include_directories(glyph PUBLIC include)
target_link_libraries(glyph PUBLIC m)

if(BUILD_TESTS)
    add_executable(test_glyph test/test_glyph.c)
    target_link_libraries(test_glyph glyph)
    add_test(NAME test_glyph COMMAND test_glyph)
endif()
