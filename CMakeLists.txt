cmake_minimum_required(VERSION 3.21)

project(terse-eventbus LANGUAGES C)

if (PROJECT_IS_TOP_LEVEL)
    set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
endif ()

add_library(teb STATIC src/event_bus.c)
add_library(teb::teb ALIAS teb)

target_compile_features(teb PUBLIC c_std_23)

set_target_properties(teb PROPERTIES
        C_STANDARD 23
        C_STANDARD_REQUIRED ON
        C_EXTENSIONS OFF
)

target_include_directories(teb PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
)

# GCC/Clang only
target_compile_options(teb PRIVATE -Wall -Wextra -Wpedantic)

if (PROJECT_IS_TOP_LEVEL)
    add_subdirectory(examples)

    enable_testing()
    add_subdirectory(test)
endif ()
