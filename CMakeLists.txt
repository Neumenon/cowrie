cmake_minimum_required(VERSION 3.18)
project(cowrie LANGUAGES C CXX)

set(COWRIE_CARGO_TARGET_DIR "/tmp/cowrie-cargo-target" CACHE PATH "Scratch Cargo target directory for libcowrie_core")

if(APPLE)
  set(COWRIE_LIB_NAME "libcowrie_core.dylib")
elseif(WIN32)
  set(COWRIE_LIB_NAME "cowrie_core.dll")
else()
  set(COWRIE_LIB_NAME "libcowrie_core.so")
endif()

set(COWRIE_LIB_PATH "${COWRIE_CARGO_TARGET_DIR}/release/${COWRIE_LIB_NAME}")

add_custom_command(
  OUTPUT "${COWRIE_LIB_PATH}"
  COMMAND ${CMAKE_COMMAND} -E env CARGO_TARGET_DIR=${COWRIE_CARGO_TARGET_DIR} cargo build --release -p cowrie-core
  WORKING_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}"
  COMMENT "Building Rust cowrie-core (${COWRIE_LIB_NAME})"
)

add_custom_target(cowrie_core_build ALL DEPENDS "${COWRIE_LIB_PATH}")

add_library(cowrie_core SHARED IMPORTED GLOBAL)
add_dependencies(cowrie_core cowrie_core_build)
set_target_properties(cowrie_core PROPERTIES
  IMPORTED_LOCATION "${COWRIE_LIB_PATH}"
  INTERFACE_INCLUDE_DIRECTORIES "${CMAKE_CURRENT_SOURCE_DIR}/crates/cowrie-core/include"
)

add_library(cowrie_llama_statepage INTERFACE)
target_include_directories(cowrie_llama_statepage INTERFACE
  "${CMAKE_CURRENT_SOURCE_DIR}/adapters/llama-statepage"
  "${CMAKE_CURRENT_SOURCE_DIR}/crates/cowrie-core/include"
)
target_link_libraries(cowrie_llama_statepage INTERFACE cowrie_core)

install(FILES "${CMAKE_CURRENT_SOURCE_DIR}/crates/cowrie-core/include/cowrie.h" DESTINATION include)
install(FILES "${CMAKE_CURRENT_SOURCE_DIR}/adapters/llama-statepage/cowrie_llama_statepage.hpp" DESTINATION include)
install(FILES "${COWRIE_LIB_PATH}" DESTINATION lib OPTIONAL)
