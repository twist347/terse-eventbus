# teb — terse event bus

[![ci](https://github.com/twist347/terse-eventbus/actions/workflows/ci.yml/badge.svg)](https://github.com/twist347/terse-eventbus/actions/workflows/ci.yml)

A tiny synchronous event bus for **C23** (callable from **C++20**).  
FIFO delivery, safe subscribe/unsubscribe even from inside handlers, and an
optional deferred queue for work that must wait until the frame settles.

## Features
- **Synchronous** `publish`: handlers run before the call returns.
- **Deferred** `post` + `drain`: queue an event now, dispatch it at a point you
  choose. Flattens nested publishes and defers destructive work out of an
  iteration.
- **FIFO order**: handlers are invoked in registration order.
- **Mutation-safe dispatch**: unsubscribing inside a handler takes effect immediately; subscribing never feeds the event in flight.
- **Zero-copy payloads**: pass a pointer + size; no allocations in the hot path.
- **No subscriber cap**: the per-type list grows on demand, with `teb_event_bus_reserve`
  to pre-allocate and `teb_event_bus_shrink_to_fit` to hand memory back.
- **Queue sized per bus**: `teb_event_bus_new_cap` takes the post slot size and queue
  depth, so a consumer picks them without editing the header.
- **C++ friendly**: functions are `extern "C"`.
- **No deps** beyond the standard library.

## Caveats
- **Unsubscribe is immediate, so the result depends on registration order.**
  A handler removed during a publish is skipped, unless the dispatch had already
  reached it. Freeing that handler's `ctx` on the spot is safe.
- **Subscribe allocates**, so it returns `false` if the subscriber list cannot
  grow. Growing from inside a handler is safe: the dispatch in flight follows
  the moved array.
- **Single-threaded.** No internal locking — call from one thread only.
- **Bounded recursion.** Nested publishes are capped at
  `TEB_MAX_DISPATCH_DEPTH` levels (default 32). Hitting the cap drops
  the event and asserts in debug builds.
- **`publish` borrows the payload, `post` copies it.** A posted payload outlives
  the caller's frame, so it has to be copied, and it is capped at the bus's post
  slot size (`TEB_DEFAULT_POST_SLOT_SIZE`, 64 bytes, unless `teb_event_bus_new_cap` says
  otherwise) — going over drops the event and asserts in debug builds. This is
  the one asymmetry between the two paths.
- **A queue slot is aligned for `max_align_t`, no further.** A type needing
  stricter alignment (SIMD vectors, cache-line-padded structs) cannot go through
  `post`; `TEB_EVENT_BUS_POST_DATA` refuses to compile, and `teb_event_bus_post_data` has no way to notice.
  `publish` carries such a payload fine — it borrows your object rather than
  copying into a slot.
- **`drain` handles the events queued as of entry**, never more. A post made
  from a handler waits for the next drain, so two handlers posting to each other
  cannot keep a drain alive. `drain` is not callable from any handler, and
  `clear_posted` not from inside a drain; both do nothing and assert in debug
  builds.

## Complexity & memory
- `subscribe` / `unsubscribe` / `publish`: **O(n)** in handlers per type.
- `subscribe` is amortized O(1) past the duplicate check: capacity doubles from 4.
- A bus costs `type_count` empty vectors up front; slots are allocated per
  type on first subscribe. Vectors never shrink — compaction and `teb_event_bus_clear`
  reclaim slots but keep the capacity.
- Dead slots are swept only once the outermost dispatch returns, so heavy
  subscribe/unsubscribe churn *inside* a single dispatch grows a vector by the
  churn rather than by the live count. `teb_event_bus_shrink_to_fit` is the way back.
- The post queue is a ring of fixed-size slots — `TEB_DEFAULT_POST_QUEUE_CAP` of
  them at `TEB_DEFAULT_POST_SLOT_SIZE` bytes each, about 20 KB, unless
  `teb_event_bus_new_cap` picks other sizes. It is allocated on the first `post`, so
  a bus that never posts never pays, and `post` and `drain` never allocate. A
  full queue makes `post` return `false`; during a drain one slot is held by the
  event in flight.
- Slot payloads are strided up to `max_align_t`, so a 40-byte slot really costs
  48. `teb_event_bus_new_cap(n, 0, cap)` allocates no payload storage at all, which
  is what you want for a bus that only carries signals.
- `teb_event_bus_reserve(bus, type, n)` up front makes the following `n` subscribes
  allocation-free, which is what you want if the bus must not allocate after
  init. It is not a speed knob — doubling already costs ~6 reallocs to reach 100
  subscribers, all of them at startup.

## Usage

### Immediate

```c
#include "teb/event_bus.h"
#include <stdio.h>

typedef enum : int32_t {
    EV_PLAYER_DAMAGED,
    EV_ENEMY_KILLED,
    EV_COUNT
} GameEvent;

typedef struct { int hp; } DamageEvt;

void on_player_damaged(const teb_Event *ev, teb_EventBus *bus, void *ctx) {
    (void) bus;
    (void) ctx;

    TEB_EVENT_EXPECT(DamageEvt, ev);
    const DamageEvt *d = TEB_EVENT_DATA_AS(DamageEvt, ev);
    printf("player lost %d hp\n", d->hp);
}

int main(void) {
    teb_EventBus *bus = teb_event_bus_new(EV_COUNT);

    [[maybe_unused]] bool ok = teb_event_bus_subscribe(bus, EV_PLAYER_DAMAGED, on_player_damaged, nullptr);
    assert(ok);

    DamageEvt d = {.hp = 5};
    TEB_EVENT_BUS_PUBLISH_DATA(bus, EV_PLAYER_DAMAGED, d);

    teb_event_bus_drop(bus);
}
```

### Deferred

Same enum, same handler, same subscription — only the delivery changes:

```c
int main(void) {
    teb_EventBus *bus = teb_event_bus_new(EV_COUNT);

    [[maybe_unused]] bool ok = teb_event_bus_subscribe(bus, EV_PLAYER_DAMAGED, on_player_damaged, nullptr);
    assert(ok);

    while (running) {
        DamageEvt d = {.hp = 5};
        TEB_EVENT_BUS_POST_DATA(bus, EV_PLAYER_DAMAGED, d); // queued; d may die on the next line

        update_the_world();

        (void) teb_event_bus_drain(bus); // the handler runs here, once the frame settled
    }

    teb_event_bus_drop(bus);
}
```

Runnable versions of both live in [`examples/`](examples).

Sizing the queue for the bus instead of taking the defaults:

```c
    // 4 events deep, 32-byte payloads; refuses anything larger
    teb_EventBus *bus = teb_event_bus_new_cap(EV_COUNT, 32, 4);
```

> Note: public API is marked `[[nodiscard]]` — don't silently drop return values.
> Never wrap the calls themselves in `assert()`: under `-DNDEBUG` the argument
> isn't evaluated and nothing runs. Assign, then assert (as above).

## API

`teb_EventType` is an `int32_t`, `teb_Event` and `teb_EventBus` are opaque, and a
handler is a `teb_EventHandler`, i.e.
`void (*)(const teb_Event *ev, teb_EventBus *bus, void *ctx)`.

**Event**

| | |
|---|---|
| `teb_event_type(ev)` | the type it was published with |
| `teb_event_data(ev)` / `teb_event_data_size(ev)` | payload, borrowed for the call |
| `TEB_EVENT_EXPECT(T, ev)` | assert it really is a `T` |
| `TEB_EVENT_DATA_AS(T, ev)` | read it in place, as a `const T *` |
| `TEB_EVENT_LOAD(T, ev, dst)` | copy it out — the only form that outlives dispatch |

**Bus**

| | |
|---|---|
| `teb_event_bus_new(n)` | types `[0, n)`, default queue |
| `teb_event_bus_new_cap(n, slot, cap)` | same, queue sized here |
| `teb_event_bus_drop(bus)` | frees it; safe on `nullptr` |
| `teb_event_bus_clear(bus)` | drops subscriptions and queue, keeps the memory |
| `teb_event_bus_reserve(bus, type, cap)` | room for `cap` subscribers up front |
| `teb_event_bus_shrink_to_fit(bus)` | hand unused subscriber capacity back |

**Subscriptions**

| | |
|---|---|
| `teb_event_bus_subscribe(bus, type, h, ctx)` | register; duplicate pairs rejected |
| `teb_event_bus_unsubscribe(bus, type, h, ctx)` | remove one |
| `teb_event_bus_unsubscribe_type(bus, type)` | remove all on a type |
| `teb_event_bus_unsubscribe_ctx(bus, ctx)` | remove by ctx, returns how many |
| `teb_event_bus_unsubscribe_handler(bus, h)` | remove by handler, returns how many |
| `teb_event_bus_count_subscribers(bus, type)` | live handlers on a type |
| `TEB_CTX_EXPECT(T, ctx)` / `TEB_CTX_AS(T, ctx)` / `TEB_CTX_MUT_AS(T, ctx)` | the same, for `ctx` |

**Publish — now**

| | |
|---|---|
| `teb_event_bus_publish_data(bus, type, data, size)` | run every handler; payload borrowed |
| `teb_event_bus_publish(bus, type)` | same, no payload |
| `TEB_EVENT_BUS_PUBLISH_DATA(bus, type, expr)` | publish a copy of `expr` |
| `TEB_MAX_DISPATCH_DEPTH` | nesting cap, 32 |

**Publish — later**

| | |
|---|---|
| `teb_event_bus_post_data(bus, type, data, size)` | queue it; payload **copied** |
| `teb_event_bus_post(bus, type)` | same, no payload |
| `TEB_EVENT_BUS_POST_DATA(bus, type, expr)` | post a copy of `expr` |
| `teb_event_bus_drain(bus)` | dispatch what is queued, returns how many |
| `teb_event_bus_clear_posted(bus)` | discard the queue |
| `teb_event_bus_count_posted(bus)` | events waiting |
| `TEB_DEFAULT_POST_SLOT_SIZE` / `TEB_DEFAULT_POST_QUEUE_CAP` | 64 B, 256 slots |

## Requirements

GCC or Clang with **C23** support (tested on Linux; macOS and the BSDs should
work as-is). MSVC is not supported.

## Build

```
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build
```

## Testing

```
cmake -S . -B build -DCMAKE_BUILD_TYPE=Debug
cmake --build build
ctest --test-dir build --output-on-failure
```

The tests and the examples are built only when this project is the top-level
one, so consuming it via `FetchContent` or `add_subdirectory` does not pull them
in.

## Examples

Two runnable programs, one per delivery model:

- `examples/immediate.c` — `teb_event_bus_publish`. An enemy dies inside the damage
  dispatch and frees itself on the spot; the handler registered after it never
  sees the fatal hit.
- `examples/deferred.c` — `teb_event_bus_post` + `teb_event_bus_drain` in a frame loop. Deaths are
  queued so nothing is freed while the tick dispatch is still walking the
  subscribers, and the loot a reaper posts lands one drain later, which is the
  snapshot rule in plain sight.

## Integration

Linking against the target propagates the include path and `-std=c23`, so
nothing has to be repeated on your side.

### FetchContent

```cmake
include(FetchContent)
FetchContent_Declare(terse-eventbus
    GIT_REPOSITORY https://github.com/twist347/terse-eventbus.git
    GIT_TAG main)
FetchContent_MakeAvailable(terse-eventbus)

target_link_libraries(my_app PRIVATE teb::teb)
```

### Submodule

```cmake
add_subdirectory(third_party/terse-eventbus)
target_link_libraries(my_app PRIVATE teb::teb)
```

### Vendoring

It is one `.c` file — dropping `src/event_bus.c` and `include/teb/` straight
into your own tree works too. Compile with `-std=c23`.
