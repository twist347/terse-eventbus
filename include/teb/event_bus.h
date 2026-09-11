#pragma once

// Synchronous event bus: publish runs every handler before it returns.
// Posting queues an event instead, to be dispatched by a later drain.
// Single-threaded.

#include "teb/event.h"

#include <stddef.h>
#include <stdint.h>
#include <assert.h>

#ifdef __cplusplus
#include <type_traits>
#endif

#ifdef __cplusplus
extern "C" {
#endif

/* ========== event bus ========== */

typedef struct teb_EventBus teb_EventBus;

/// Post queue sizes teb_event_bus_new picks; teb_event_bus_new_cap takes its own.
constexpr size_t TEB_DEFAULT_POST_SLOT_SIZE = 64;
constexpr size_t TEB_DEFAULT_POST_QUEUE_CAP = 256;

/// Valid event types are [0, type_count). nullptr on allocation failure.
[[nodiscard]]
teb_EventBus *teb_event_bus_new(size_t type_count);

/// Same, with the post queue sized for this bus instead of taking the defaults:
/// `post_slot_size` is the largest payload teb_event_bus_post_data will accept
/// -- 0 allows payload-less posts only and costs no payload storage -- and
/// `post_queue_cap` is how many events may wait for a drain. Neither is
/// allocated until the first post. nullptr on allocation failure, or on sizes
/// whose product does not fit in size_t.
[[nodiscard]]
teb_EventBus *teb_event_bus_new_cap(size_t type_count, size_t post_slot_size, size_t post_queue_cap);

/// Safe on nullptr.
void teb_event_bus_drop(teb_EventBus *self);

/// Drops every subscription and every event still queued, keeping both
/// allocations. Not callable during a dispatch.
void teb_event_bus_clear(teb_EventBus *self);

/// Pre-allocates room for `cap` subscribers on `type`, so
/// teb_event_bus_subscribe cannot fail on allocation until that many are
/// registered.
[[nodiscard]]
bool teb_event_bus_reserve(teb_EventBus *self, teb_EventType type, size_t cap);

/// Releases per-type capacity no live subscription is using; a type with none
/// frees its storage outright. Not callable during a dispatch.
void teb_event_bus_shrink_to_fit(teb_EventBus *self);

/* ========== event bus subs ========== */

typedef void (*teb_EventHandler)(const teb_Event *ev, teb_EventBus *bus, void *ctx);

/// Subscribing during dispatch does not affect the event in flight. There is no
/// cap on subscribers: the per-type list grows on demand. Fails on an invalid
/// type, an already registered (handler, ctx) pair, or allocation failure.
[[nodiscard]]
bool teb_event_bus_subscribe(teb_EventBus *self, teb_EventType type, teb_EventHandler handler, void *ctx);

/// Every removal below takes effect immediately: a handler removed during
/// dispatch is not called for the event in flight unless the dispatch already
/// reached it, which makes it safe to free its ctx from inside another handler.
[[nodiscard]]
bool teb_event_bus_unsubscribe(teb_EventBus *self, teb_EventType type, teb_EventHandler handler, void *ctx);

/// Removes every handler on `type`. Does nothing for an invalid one.
void teb_event_bus_unsubscribe_type(teb_EventBus *self, teb_EventType type);

/// Removes every subscription with this ctx. Returns how many were removed.
[[nodiscard]]
size_t teb_event_bus_unsubscribe_ctx(teb_EventBus *self, const void *ctx);

/// Same, matched by handler instead of ctx.
[[nodiscard]]
size_t teb_event_bus_unsubscribe_handler(teb_EventBus *self, teb_EventHandler handler);

/// Live handlers on `type`, 0 for an invalid one. Handlers removed earlier in an
/// ongoing dispatch are already excluded.
[[nodiscard]]
size_t teb_event_bus_count_subscribers(const teb_EventBus *self, teb_EventType type);

/// Asserts ctx is non-null and aligned for T. Compiled out under NDEBUG.
#define TEB_CTX_EXPECT(T, ctx)                                        \
do {                                                                  \
    [[maybe_unused]] const void *teb_ctx_ = (ctx);                    \
    assert(teb_ctx_);                                                 \
    assert(((uintptr_t) teb_ctx_ % (uintptr_t) alignof(T)) == 0);     \
} while (0)

/// Read ctx as it was passed to teb_event_bus_subscribe: as a const T *, or as
/// a T * to write through.
#define TEB_CTX_AS(T, ctx)        ((const T *)(ctx))
#define TEB_CTX_MUT_AS(T, ctx)    ((T *)(ctx))

/* ========== event bus publish ========== */

/// Nesting limit for publishes made from inside a handler. Past it the event is
/// dropped and teb_event_bus_publish_data returns false.
constexpr size_t TEB_MAX_DISPATCH_DEPTH = 32;

/// `data` is borrowed, never copied -- it must outlive the call. Returns false
/// on an invalid type or on exceeding the nesting limit; no subscribers is true.
[[nodiscard]]
bool teb_event_bus_publish_data(teb_EventBus *self, teb_EventType type, const void *data, size_t data_size);

/// Publishes with no payload.
[[nodiscard]]
bool teb_event_bus_publish(teb_EventBus *self, teb_EventType type);

#ifdef __cplusplus
#define TEB_UNQUAL_(...) std::remove_cvref_t<decltype(__VA_ARGS__)>
#else
#define TEB_UNQUAL_(...) typeof_unqual(__VA_ARGS__)
#endif

/// Publishes a copy of the expression as the payload, discarding the result.
/// Variadic so that a braced initializer with commas needs no extra parens.
#define TEB_EVENT_BUS_PUBLISH_DATA(bus, type, ...)                                    \
do {                                                                                  \
    TEB_UNQUAL_(__VA_ARGS__) teb_tmp_ = (__VA_ARGS__);                                \
    (void) teb_event_bus_publish_data((bus), (type), &teb_tmp_, sizeof(teb_tmp_));    \
} while (0)

/* ========== event bus deferred publish ========== */

/// Queues the event instead of dispatching it: no handler runs until
/// teb_event_bus_drain, and the subscriber list is read then, not now.
/// Unlike teb_event_bus_publish_data the payload is COPIED, so it need not
/// outlive the call. Returns false on an invalid type, a full queue, or
/// allocation failure.
/// A payload above this bus's post slot size returns false too, and asserts:
/// its size is fixed by the call site, so going over is a bug, not a run-time
/// condition.
/// A queue slot is aligned for max_align_t and no further, so a type needing
/// stricter alignment cannot be posted -- teb_event_bus_publish_data still
/// takes it, since it borrows the caller's own object instead of copying into a
/// slot.
[[nodiscard]]
bool teb_event_bus_post_data(teb_EventBus *self, teb_EventType type, const void *data, size_t data_size);

/// Posts with no payload.
[[nodiscard]]
bool teb_event_bus_post(teb_EventBus *self, teb_EventType type);

/// Dispatches the events queued as of entry and returns how many it dispatched.
/// A post made from a handler waits for the next drain, so this cannot spin.
/// While it runs one slot is held by the event in flight. Not callable from a
/// handler: asserts, and returns 0.
size_t teb_event_bus_drain(teb_EventBus *self);

/// Discards every queued event, dispatching none. Not callable during a drain:
/// asserts, and leaves the queue alone.
void teb_event_bus_clear_posted(teb_EventBus *self);

/// Events waiting for a drain, plus the one in flight when called during one.
[[nodiscard]]
size_t teb_event_bus_count_posted(const teb_EventBus *self);

/// Posts a copy of the expression as the payload, discarding the result. A full
/// queue is an ordinary run-time state, unlike anything that fails a publish,
/// so this drops events silently -- call teb_event_bus_post_data directly where
/// that matters.
#define TEB_EVENT_BUS_POST_DATA(bus, type, ...)                                             \
do {                                                                                        \
    static_assert(alignof(TEB_UNQUAL_(__VA_ARGS__)) <= alignof(max_align_t),                \
                  "TEB_EVENT_BUS_POST_DATA: payload needs more alignment than a slot has"); \
    TEB_UNQUAL_(__VA_ARGS__) teb_tmp_ = (__VA_ARGS__);                                      \
    (void) teb_event_bus_post_data((bus), (type), &teb_tmp_, sizeof(teb_tmp_));             \
} while (0)

#ifdef __cplusplus
}
#endif
