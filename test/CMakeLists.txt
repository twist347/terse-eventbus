add_executable(test_event_bus test_event_bus.c)
target_link_libraries(test_event_bus PRIVATE teb::teb)
target_compile_options(test_event_bus PRIVATE -Wall -Wextra -Wpedantic)

set_target_properties(test_event_bus PROPERTIES
        C_STANDARD 23
        C_STANDARD_REQUIRED ON
        C_EXTENSIONS OFF
)

add_test(NAME event_bus COMMAND test_event_bus)
