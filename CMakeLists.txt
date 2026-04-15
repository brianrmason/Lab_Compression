cmake_minimum_required(VERSION 3.15)
project(labcompress VERSION 1.0.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

if(MSVC)
    add_compile_options(/W4)
else()
    add_compile_options(-Wall -Wextra -Wpedantic)
endif()

if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release)
endif()

set(SOURCES
    src/schemaparser.cpp
    src/compressor.cpp
    src/queryengine.cpp
)

set(HEADERS
    include/schemaparser.hpp
    include/queryengine.hpp
    include/compressor.hpp
)

add_executable(labcompress
    src/main.cpp
    ${SOURCES}
)

target_include_directories(labcompress PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/include
)

add_library(labcompress_lib STATIC
    ${SOURCES}
)

target_include_directories(labcompress_lib PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/include
)

install(TARGETS labcompress DESTINATION bin)
install(TARGETS labcompress_lib DESTINATION lib)
install(FILES ${HEADERS} DESTINATION include/labcompress)