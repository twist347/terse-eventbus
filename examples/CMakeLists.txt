foreach (name IN ITEMS immediate deferred)
    add_executable(example_${name} ${name}.c)
    target_link_libraries(example_${name} PRIVATE teb::teb)
    target_compile_options(example_${name} PRIVATE -Wall -Wextra -Wpedantic)

    set_target_properties(example_${name} PROPERTIES
            C_STANDARD 23
            C_STANDARD_REQUIRED ON
            C_EXTENSIONS OFF
    )
endforeach ()
