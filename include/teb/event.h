#pragma once

// One event as a handler sees it: the type it was published with and a
// borrowed payload.

#include <stddef.h>
#include <stdint.h>
#include <assert.h>
#include <string.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef int32_t teb_EventType;
typedef struct teb_Event teb_Event;

/// The type this event was published with.
[[nodiscard]]
teb_EventType teb_event_type(const teb_Event *self);

/// The payload handed to teb_event_bus_publish_data: borrowed, valid only for
/// the duration of the handler call. Copy it out with TEB_EVENT_LOAD to keep it.
[[nodiscard]]
const void *teb_event_data(const teb_Event *self);

/// Payload size in bytes; 0 when the event carries none.
[[nodiscard]]
size_t teb_event_data_size(const teb_Event *self);

/// Asserts the payload really is a T. Compiled out under NDEBUG.
#define TEB_EVENT_EXPECT(T, ev)                                                     \
do {                                                                                \
    [[maybe_unused]] const teb_Event *teb_exp_ = (ev);                              \
    assert(teb_exp_);                                                               \
    assert(teb_event_data(teb_exp_));                                               \
    assert(((uintptr_t) teb_event_data(teb_exp_) % (uintptr_t) alignof(T)) == 0);   \
    assert(teb_event_data_size(teb_exp_) == sizeof(T));                             \
} while (0)

/// Read the payload in place, as a const T *. Run TEB_EVENT_EXPECT first.
#define TEB_EVENT_DATA_AS(T, ev)    ((const T *)(teb_event_data((ev))))

/// Copy the payload into dst -- the only form that outlives the dispatch.
#define TEB_EVENT_LOAD(T, ev, dst)                             \
do {                                                           \
    const teb_Event *teb_event_ = (ev);                        \
    TEB_EVENT_EXPECT(T, teb_event_);                           \
    memcpy(&(dst), teb_event_data(teb_event_), sizeof(T));     \
} while (0)

#ifdef __cplusplus
}
#endif
