#include "teb/event_bus.h"

#include <stdint.h>
#include <stdio.h>
#include <string.h>

#define UNUSED(x) (void) (x)

#define CHECK(cond)                                                  \
do {                                                                 \
    ++checks_run;                                                    \
    if (!(cond)) {                                                   \
        ++checks_failed;                                             \
        printf("  FAIL %s:%d: %s\n", __FILE__, __LINE__, #cond);     \
    }                                                                \
} while (0)

static int checks_run = 0;
static int checks_failed = 0;

enum : int32_t {
    EV_X = 0,
    EV_Y,
    EV_COUNT
};

/* ========== handlers ========== */

// every handler appends one letter, so a dispatch leaves a readable trace
static char trace[64];
static size_t trace_len;

static void trace_reset() {
    trace_len = 0;
    trace[0] = '\0';
}

static void trace_put(char c) {
    trace[trace_len++] = c;
    trace[trace_len] = '\0';
}

static void h_a(const teb_Event *ev, teb_EventBus *bus, void *ctx) {
    UNUSED(ev);
    UNUSED(bus);
    UNUSED(ctx);
    trace_put('a');
}

static void h_b(const teb_Event *ev, teb_EventBus *bus, void *ctx) {
    UNUSED(ev);
    UNUSED(bus);
    UNUSED(ctx);
    trace_put('b');
}

static void h_c(const teb_Event *ev, teb_EventBus *bus, void *ctx) {
    UNUSED(ev);
    UNUSED(bus);
    UNUSED(ctx);
    trace_put('c');
}

static int payload_seen;

static void h_payload(const teb_Event *ev, teb_EventBus *bus, void *ctx) {
    UNUSED(bus);
    UNUSED(ctx);
    TEB_EVENT_EXPECT(int, ev);
    payload_seen = *TEB_EVENT_DATA_AS(int, ev);
}

// removes h_c, which is subscribed after it and has not been reached yet
static void h_kill_c(const teb_Event *ev, teb_EventBus *bus, void *ctx) {
    UNUSED(ev);
    UNUSED(ctx);
    trace_put('k');
    CHECK(teb_event_bus_unsubscribe(bus, EV_X, h_c, nullptr));
}

static void h_relay(const teb_Event *ev, teb_EventBus *bus, void *ctx) {
    UNUSED(ev);
    UNUSED(ctx);
    trace_put('r');
    CHECK(teb_event_bus_publish(bus, EV_Y));
}

static int calls;

static void h_count(const teb_Event *ev, teb_EventBus *bus, void *ctx) {
    UNUSED(ev);
    UNUSED(bus);
    UNUSED(ctx);
    ++calls;
}

// subscribes from inside a dispatch; called with the vector exactly full, so the
// append reallocs and moves the array the loop above is still walking
static void h_grow(const teb_Event *ev, teb_EventBus *bus, void *ctx) {
    UNUSED(ev);
    UNUSED(ctx);
    trace_put('g');
    CHECK(teb_event_bus_subscribe(bus, EV_X, h_count, nullptr));
}

typedef struct {
    int a;
    int b;
} Pair;

static Pair pair_seen;

static void h_pair(const teb_Event *ev, teb_EventBus *bus, void *ctx) {
    UNUSED(bus);
    UNUSED(ctx);
    TEB_EVENT_EXPECT(Pair, ev); // also checks the queue slot is aligned for Pair
    TEB_EVENT_LOAD(Pair, ev, pair_seen);
}

// the two below post to each other: without a snapshot a drain would never end
static void h_ping(const teb_Event *ev, teb_EventBus *bus, void *ctx) {
    UNUSED(ev);
    UNUSED(ctx);
    trace_put('p');
    CHECK(teb_event_bus_post(bus, EV_Y));
}

static void h_pong(const teb_Event *ev, teb_EventBus *bus, void *ctx) {
    UNUSED(ev);
    UNUSED(ctx);
    trace_put('q');
    CHECK(teb_event_bus_post(bus, EV_X));
}

// posts from inside a drain of a full queue, then rereads its own payload: the
// post must be refused, because the slot it would take is the one in flight
static bool probe_ran;
static bool probe_post_ok;
static bool probe_payload_kept;

static void h_post_probe(const teb_Event *ev, teb_EventBus *bus, void *ctx) {
    UNUSED(ctx);
    if (probe_ran) {
        return;
    }
    probe_ran = true;

    // a payload of its own, so a slot reused here would visibly rewrite ours
    const int before = *TEB_EVENT_DATA_AS(int, ev);
    const int mine = -12345;
    probe_post_ok = teb_event_bus_post_data(bus, EV_Y, &mine, sizeof(mine));
    probe_payload_kept = *TEB_EVENT_DATA_AS(int, ev) == before;
}

// checks the drained payloads arrive in post order, values intact
static int seq_next;
static bool seq_ok;

static void h_seq(const teb_Event *ev, teb_EventBus *bus, void *ctx) {
    UNUSED(bus);
    UNUSED(ctx);
    TEB_EVENT_EXPECT(int, ev);
    if (*TEB_EVENT_DATA_AS(int, ev) != seq_next) {
        seq_ok = false;
    }
    ++seq_next;
}

static bool no_payload_ok;

static void h_no_payload(const teb_Event *ev, teb_EventBus *bus, void *ctx) {
    UNUSED(bus);
    UNUSED(ctx);
    no_payload_ok = teb_event_data(ev) == nullptr && teb_event_data_size(ev) == 0;
}

// a strictly-aligned payload, to catch a stride that is not rounded up
static bool dbl_ok;

static void h_dbl(const teb_Event *ev, teb_EventBus *bus, void *ctx) {
    UNUSED(bus);
    UNUSED(ctx);
    TEB_EVENT_EXPECT(double, ev); // asserts the slot is aligned for a double
    if (*TEB_EVENT_DATA_AS(double, ev) != 1.5) {
        dbl_ok = false;
    }
}

// publishes synchronously from inside a drain
static void h_relay_sync(const teb_Event *ev, teb_EventBus *bus, void *ctx) {
    UNUSED(ev);
    UNUSED(ctx);
    trace_put('r');
    CHECK(teb_event_bus_publish(bus, EV_Y));
}

/* ========== tests ========== */

static void test_empty_bus() {
    teb_EventBus *bus = teb_event_bus_new(EV_COUNT);
    CHECK(bus);

    CHECK(teb_event_bus_count_subscribers(bus, EV_X) == 0);
    CHECK(teb_event_bus_publish(bus, EV_X)); // no subscribers is not a failure
    CHECK(!teb_event_bus_publish(bus, EV_COUNT)); // out of range
    CHECK(!teb_event_bus_publish(bus, -1));

    teb_event_bus_drop(bus);
}

static void test_subscribe_rules() {
    teb_EventBus *bus = teb_event_bus_new(EV_COUNT);
    int c1 = 0;
    int c2 = 0;

    CHECK(teb_event_bus_subscribe(bus, EV_X, h_a, &c1));
    CHECK(!teb_event_bus_subscribe(bus, EV_X, h_a, &c1)); // exact duplicate
    CHECK(teb_event_bus_subscribe(bus, EV_X, h_a, &c2)); // same handler, other ctx
    CHECK(!teb_event_bus_subscribe(bus, EV_COUNT, h_a, nullptr));
    CHECK(teb_event_bus_count_subscribers(bus, EV_X) == 2);

    teb_event_bus_drop(bus);
}

static void test_fifo_order() {
    teb_EventBus *bus = teb_event_bus_new(EV_COUNT);
    trace_reset();

    CHECK(teb_event_bus_subscribe(bus, EV_X, h_a, nullptr));
    CHECK(teb_event_bus_subscribe(bus, EV_X, h_b, nullptr));
    CHECK(teb_event_bus_subscribe(bus, EV_X, h_c, nullptr));
    CHECK(teb_event_bus_publish(bus, EV_X));
    CHECK(strcmp(trace, "abc") == 0);

    teb_event_bus_drop(bus);
}

static void test_payload() {
    teb_EventBus *bus = teb_event_bus_new(EV_COUNT);
    payload_seen = 0;

    CHECK(teb_event_bus_subscribe(bus, EV_X, h_payload, nullptr));
    const int x = 42;
    TEB_EVENT_BUS_PUBLISH_DATA(bus, EV_X, x);
    CHECK(payload_seen == 42);

    teb_event_bus_drop(bus);
}

static void test_unsubscribe() {
    teb_EventBus *bus = teb_event_bus_new(EV_COUNT);
    trace_reset();

    CHECK(teb_event_bus_subscribe(bus, EV_X, h_a, nullptr));
    CHECK(teb_event_bus_subscribe(bus, EV_X, h_b, nullptr));

    CHECK(teb_event_bus_unsubscribe(bus, EV_X, h_a, nullptr));
    CHECK(!teb_event_bus_unsubscribe(bus, EV_X, h_a, nullptr)); // already gone
    CHECK(teb_event_bus_count_subscribers(bus, EV_X) == 1);

    CHECK(teb_event_bus_publish(bus, EV_X));
    CHECK(strcmp(trace, "b") == 0);

    teb_event_bus_drop(bus);
}

static void test_unsubscribe_during_dispatch() {
    teb_EventBus *bus = teb_event_bus_new(EV_COUNT);
    trace_reset();

    CHECK(teb_event_bus_subscribe(bus, EV_X, h_kill_c, nullptr));
    CHECK(teb_event_bus_subscribe(bus, EV_X, h_c, nullptr));

    CHECK(teb_event_bus_publish(bus, EV_X));
    CHECK(strcmp(trace, "k") == 0); // h_c was skipped, not called
    CHECK(teb_event_bus_count_subscribers(bus, EV_X) == 1); // and swept once dispatch ended

    teb_event_bus_drop(bus);
}

static void test_unsubscribe_ctx() {
    teb_EventBus *bus = teb_event_bus_new(EV_COUNT);
    int obj = 0;

    CHECK(teb_event_bus_subscribe(bus, EV_X, h_a, &obj));
    CHECK(teb_event_bus_subscribe(bus, EV_Y, h_a, &obj));
    CHECK(teb_event_bus_subscribe(bus, EV_X, h_b, nullptr));

    CHECK(teb_event_bus_unsubscribe_ctx(bus, &obj) == 2);
    CHECK(teb_event_bus_count_subscribers(bus, EV_X) == 1);
    CHECK(teb_event_bus_count_subscribers(bus, EV_Y) == 0);

    teb_event_bus_drop(bus);
}

static void test_nested_publish() {
    teb_EventBus *bus = teb_event_bus_new(EV_COUNT);
    trace_reset();

    CHECK(teb_event_bus_subscribe(bus, EV_X, h_relay, nullptr));
    CHECK(teb_event_bus_subscribe(bus, EV_Y, h_b, nullptr));

    CHECK(teb_event_bus_publish(bus, EV_X));
    CHECK(strcmp(trace, "rb") == 0);

    teb_event_bus_drop(bus);
}

// well past every doubling step, so the vector reallocs several times
static void test_growth() {
    constexpr size_t n = 200;

    teb_EventBus *bus = teb_event_bus_new(EV_COUNT);
    int slots[n];

    bool all_ok = true;
    for (size_t i = 0; i < n; ++i) {
        slots[i] = 0;
        if (!teb_event_bus_subscribe(bus, EV_X, h_count, &slots[i])) {
            all_ok = false;
        }
    }
    CHECK(all_ok);
    CHECK(teb_event_bus_count_subscribers(bus, EV_X) == n);
    CHECK(!teb_event_bus_subscribe(bus, EV_X, h_count, &slots[0])); // duplicate, after growth

    calls = 0;
    CHECK(teb_event_bus_publish(bus, EV_X));
    CHECK(calls == n);

    // compaction reclaims every slot but keeps the capacity, so the vector stays
    // usable and the next subscribe needs no realloc
    teb_event_bus_unsubscribe_type(bus, EV_X);
    CHECK(teb_event_bus_count_subscribers(bus, EV_X) == 0);
    CHECK(teb_event_bus_subscribe(bus, EV_X, h_count, &slots[0]));

    calls = 0;
    CHECK(teb_event_bus_publish(bus, EV_X));
    CHECK(calls == 1);

    teb_event_bus_drop(bus);
}

// the realloc hazard: growing from inside a handler must not disturb the
// dispatch already walking the array
static void test_grow_during_dispatch() {
    teb_EventBus *bus = teb_event_bus_new(EV_COUNT);
    int slots[3] = {0};
    trace_reset();

    // fill the first capacity step (4) exactly: h_grow then three more
    CHECK(teb_event_bus_subscribe(bus, EV_X, h_grow, nullptr));
    CHECK(teb_event_bus_subscribe(bus, EV_X, h_a, &slots[0]));
    CHECK(teb_event_bus_subscribe(bus, EV_X, h_a, &slots[1]));
    CHECK(teb_event_bus_subscribe(bus, EV_X, h_a, &slots[2]));

    calls = 0;
    CHECK(teb_event_bus_publish(bus, EV_X));
    CHECK(strcmp(trace, "gaaa") == 0); // the rest of the loop survived the move
    CHECK(calls == 0); // the new handler stayed out of this event
    CHECK(teb_event_bus_count_subscribers(bus, EV_X) == 5);

    // it takes part in the next one
    CHECK(teb_event_bus_unsubscribe(bus, EV_X, h_grow, nullptr));
    trace_reset();
    calls = 0;
    CHECK(teb_event_bus_publish(bus, EV_X));
    CHECK(strcmp(trace, "aaa") == 0);
    CHECK(calls == 1);

    teb_event_bus_drop(bus);
}

static void test_reserve() {
    constexpr size_t n = 50;

    teb_EventBus *bus = teb_event_bus_new(EV_COUNT);
    int slots[n];

    CHECK(!teb_event_bus_reserve(bus, EV_COUNT, n)); // invalid type
    CHECK(!teb_event_bus_reserve(bus, -1, n));
    CHECK(!teb_event_bus_reserve(bus, EV_X, SIZE_MAX)); // byte count would overflow

    CHECK(teb_event_bus_reserve(bus, EV_X, n));
    CHECK(teb_event_bus_reserve(bus, EV_X, 1)); // below capacity: a no-op, not a failure
    CHECK(teb_event_bus_count_subscribers(bus, EV_X) == 0); // reserving registers nobody

    bool all_ok = true;
    for (size_t i = 0; i < n; ++i) {
        slots[i] = 0;
        if (!teb_event_bus_subscribe(bus, EV_X, h_count, &slots[i])) {
            all_ok = false;
        }
    }
    CHECK(all_ok);
    CHECK(teb_event_bus_count_subscribers(bus, EV_X) == n);

    calls = 0;
    CHECK(teb_event_bus_publish(bus, EV_X));
    CHECK(calls == n);

    teb_event_bus_drop(bus);
}

static void test_shrink() {
    constexpr size_t n = 100;

    teb_EventBus *bus = teb_event_bus_new(EV_COUNT);
    int slots[n];

    bool all_ok = true;
    for (size_t i = 0; i < n; ++i) {
        slots[i] = 0;
        if (!teb_event_bus_subscribe(bus, EV_X, h_count, &slots[i])) {
            all_ok = false;
        }
    }
    CHECK(all_ok);

    bool all_gone = true;
    for (size_t i = 3; i < n; ++i) {
        if (!teb_event_bus_unsubscribe(bus, EV_X, h_count, &slots[i])) {
            all_gone = false;
        }
    }
    CHECK(all_gone);
    CHECK(teb_event_bus_count_subscribers(bus, EV_X) == 3);

    teb_event_bus_shrink_to_fit(bus);
    CHECK(teb_event_bus_count_subscribers(bus, EV_X) == 3); // subscriptions are untouched

    calls = 0;
    CHECK(teb_event_bus_publish(bus, EV_X));
    CHECK(calls == 3);

    CHECK(teb_event_bus_subscribe(bus, EV_X, h_count, &slots[10])); // still growable
    CHECK(teb_event_bus_count_subscribers(bus, EV_X) == 4);

    // a type with nothing live frees its storage outright, and shrinking an
    // already tight bus twice changes nothing
    teb_event_bus_unsubscribe_type(bus, EV_X);
    teb_event_bus_shrink_to_fit(bus);
    teb_event_bus_shrink_to_fit(bus);
    CHECK(teb_event_bus_count_subscribers(bus, EV_X) == 0);
    CHECK(teb_event_bus_publish(bus, EV_X)); // publishing on emptied storage is fine

    CHECK(teb_event_bus_subscribe(bus, EV_X, h_count, &slots[0])); // and it regrows
    CHECK(teb_event_bus_count_subscribers(bus, EV_X) == 1);
    calls = 0;
    CHECK(teb_event_bus_publish(bus, EV_X));
    CHECK(calls == 1);

    teb_event_bus_drop(bus);
}

static void test_clear() {
    teb_EventBus *bus = teb_event_bus_new(EV_COUNT);
    trace_reset();

    CHECK(teb_event_bus_subscribe(bus, EV_X, h_a, nullptr));
    CHECK(teb_event_bus_subscribe(bus, EV_Y, h_b, nullptr));

    teb_event_bus_clear(bus);
    CHECK(teb_event_bus_count_subscribers(bus, EV_X) == 0);
    CHECK(teb_event_bus_count_subscribers(bus, EV_Y) == 0);

    CHECK(teb_event_bus_publish(bus, EV_X));
    CHECK(strcmp(trace, "") == 0);

    teb_event_bus_drop(bus);
}

static void test_post_defers() {
    teb_EventBus *bus = teb_event_bus_new(EV_COUNT);
    trace_reset();

    CHECK(teb_event_bus_subscribe(bus, EV_X, h_a, nullptr));

    CHECK(teb_event_bus_post(bus, EV_X));
    CHECK(strcmp(trace, "") == 0); // nothing runs at post time
    CHECK(teb_event_bus_count_posted(bus) == 1);

    CHECK(teb_event_bus_drain(bus) == 1);
    CHECK(strcmp(trace, "a") == 0);
    CHECK(teb_event_bus_count_posted(bus) == 0);
    CHECK(teb_event_bus_drain(bus) == 0); // an empty drain is not a failure

    // an event nobody listens to still counts as dispatched
    CHECK(teb_event_bus_post(bus, EV_Y));
    CHECK(teb_event_bus_drain(bus) == 1);

    CHECK(!teb_event_bus_post(bus, EV_COUNT)); // out of range
    CHECK(!teb_event_bus_post(bus, -1));
    CHECK(teb_event_bus_count_posted(bus) == 0);

    teb_event_bus_drop(bus);
}

// the asymmetry against teb_event_bus_publish_data: a post copies, so the
// source may die
static void test_post_copies_payload() {
    teb_EventBus *bus = teb_event_bus_new(EV_COUNT);
    pair_seen = (Pair){};

    CHECK(teb_event_bus_subscribe(bus, EV_X, h_pair, nullptr));

    Pair p = {.a = 1, .b = 2};
    TEB_EVENT_BUS_POST_DATA(bus, EV_X, p);
    p = (Pair){.a = 99, .b = 99}; // the posted copy must not follow

    CHECK(teb_event_bus_drain(bus) == 1);
    CHECK(pair_seen.a == 1 && pair_seen.b == 2);

    teb_event_bus_drop(bus);
}

static void test_post_fifo_order() {
    teb_EventBus *bus = teb_event_bus_new(EV_COUNT);
    trace_reset();

    CHECK(teb_event_bus_subscribe(bus, EV_X, h_a, nullptr));
    CHECK(teb_event_bus_subscribe(bus, EV_Y, h_b, nullptr));

    CHECK(teb_event_bus_post(bus, EV_X));
    CHECK(teb_event_bus_post(bus, EV_Y));
    CHECK(teb_event_bus_post(bus, EV_X));

    CHECK(teb_event_bus_drain(bus) == 3);
    CHECK(strcmp(trace, "aba") == 0); // queue order, not type order

    teb_event_bus_drop(bus);
}

// a drain handles the events queued as of entry and no more
static void test_drain_snapshot() {
    teb_EventBus *bus = teb_event_bus_new(EV_COUNT);
    trace_reset();

    CHECK(teb_event_bus_subscribe(bus, EV_X, h_ping, nullptr));
    CHECK(teb_event_bus_subscribe(bus, EV_Y, h_pong, nullptr));

    CHECK(teb_event_bus_post(bus, EV_X));

    // each round would be an infinite loop if the drain ran to exhaustion
    CHECK(teb_event_bus_drain(bus) == 1);
    CHECK(strcmp(trace, "p") == 0);
    CHECK(teb_event_bus_count_posted(bus) == 1); // h_ping's post waits for the next drain

    CHECK(teb_event_bus_drain(bus) == 1);
    CHECK(strcmp(trace, "pq") == 0);
    CHECK(teb_event_bus_count_posted(bus) == 1);

    CHECK(teb_event_bus_drain(bus) == 1);
    CHECK(strcmp(trace, "pqp") == 0);

    teb_event_bus_clear_posted(bus);
    teb_event_bus_drop(bus);
}

static void test_post_queue_full() {
    teb_EventBus *bus = teb_event_bus_new(EV_COUNT);

    bool all_ok = true;
    for (size_t i = 0; i < TEB_DEFAULT_POST_QUEUE_CAP; ++i) {
        if (!teb_event_bus_post(bus, EV_X)) {
            all_ok = false;
        }
    }
    CHECK(all_ok);
    CHECK(teb_event_bus_count_posted(bus) == TEB_DEFAULT_POST_QUEUE_CAP);

    CHECK(!teb_event_bus_post(bus, EV_X)); // the cap is hard
    CHECK(teb_event_bus_count_posted(bus) == TEB_DEFAULT_POST_QUEUE_CAP);

    CHECK(teb_event_bus_drain(bus) == TEB_DEFAULT_POST_QUEUE_CAP);
    CHECK(teb_event_bus_count_posted(bus) == 0);
    CHECK(teb_event_bus_post(bus, EV_X)); // and the room comes back

    teb_event_bus_clear_posted(bus);
    teb_event_bus_drop(bus);
}

// enough rounds to carry head past the end of the ring several times, with a
// distinct payload per event so a slot mixed up on the wrap would show
static void test_post_ring_wraps() {
    constexpr size_t chunk = 200;
    constexpr size_t rounds = 5;

    teb_EventBus *bus = teb_event_bus_new(EV_COUNT);
    seq_next = 0;
    seq_ok = true;
    CHECK(teb_event_bus_subscribe(bus, EV_X, h_seq, nullptr));

    bool all_ok = true;
    int posted = 0;
    for (size_t round = 0; round < rounds; ++round) {
        for (size_t i = 0; i < chunk; ++i) {
            if (!teb_event_bus_post_data(bus, EV_X, &posted, sizeof(posted))) {
                all_ok = false;
            }
            ++posted;
        }
        if (teb_event_bus_drain(bus) != chunk) {
            all_ok = false;
        }
    }
    CHECK(all_ok);
    CHECK(seq_ok); // every payload came back, in post order
    CHECK(seq_next == (int) (rounds * chunk));
    CHECK(teb_event_bus_count_posted(bus) == 0);

    teb_event_bus_drop(bus);
}

// a post carries the type and the payload, not the subscriber list: that is
// read at drain time, so the roster may change in between
static void test_subscribers_resolved_at_drain() {
    teb_EventBus *bus = teb_event_bus_new(EV_COUNT);
    trace_reset();

    CHECK(teb_event_bus_subscribe(bus, EV_X, h_a, nullptr));
    CHECK(teb_event_bus_post(bus, EV_X));

    CHECK(teb_event_bus_subscribe(bus, EV_X, h_b, nullptr)); // joined after the post
    CHECK(teb_event_bus_unsubscribe(bus, EV_X, h_a, nullptr)); // left after the post

    CHECK(teb_event_bus_drain(bus) == 1);
    CHECK(strcmp(trace, "b") == 0);

    teb_event_bus_drop(bus);
}

static void test_post_without_payload() {
    teb_EventBus *bus = teb_event_bus_new(EV_COUNT);
    no_payload_ok = false;

    CHECK(teb_event_bus_subscribe(bus, EV_X, h_no_payload, nullptr));
    CHECK(teb_event_bus_post(bus, EV_X));
    CHECK(teb_event_bus_drain(bus) == 1);
    CHECK(no_payload_ok); // data == nullptr and size == 0, as for teb_event_bus_publish

    teb_event_bus_drop(bus);
}

// the slot being dispatched stays owned until the handler returns, so a post
// from inside the drain cannot land on it and rewrite the payload in flight
static void test_slot_held_during_dispatch() {
    teb_EventBus *bus = teb_event_bus_new(EV_COUNT);
    probe_ran = false;
    probe_post_ok = true;
    probe_payload_kept = false;

    CHECK(teb_event_bus_subscribe(bus, EV_X, h_post_probe, nullptr));

    bool all_ok = true;
    for (size_t i = 0; i < TEB_DEFAULT_POST_QUEUE_CAP; ++i) {
        const int payload = (int) i;
        if (!teb_event_bus_post_data(bus, EV_X, &payload, sizeof(payload))) {
            all_ok = false;
        }
    }
    CHECK(all_ok);

    CHECK(teb_event_bus_drain(bus) == TEB_DEFAULT_POST_QUEUE_CAP);
    CHECK(probe_ran);
    CHECK(!probe_post_ok); // refused: the only free slot is the one in flight
    CHECK(probe_payload_kept);

    teb_event_bus_drop(bus);
}

// a drained event dispatches like any other, nesting included
static void test_publish_from_drain() {
    teb_EventBus *bus = teb_event_bus_new(EV_COUNT);
    trace_reset();

    CHECK(teb_event_bus_subscribe(bus, EV_X, h_relay_sync, nullptr));
    CHECK(teb_event_bus_subscribe(bus, EV_Y, h_b, nullptr));

    CHECK(teb_event_bus_post(bus, EV_X));
    CHECK(teb_event_bus_drain(bus) == 1);
    CHECK(strcmp(trace, "rb") == 0); // the sync publish ran inside the drain

    teb_event_bus_drop(bus);
}

static void test_clear_posted() {
    teb_EventBus *bus = teb_event_bus_new(EV_COUNT);
    trace_reset();

    CHECK(teb_event_bus_subscribe(bus, EV_X, h_a, nullptr));
    CHECK(teb_event_bus_post(bus, EV_X));
    CHECK(teb_event_bus_post(bus, EV_X));

    teb_event_bus_clear_posted(bus);
    CHECK(teb_event_bus_count_posted(bus) == 0);
    CHECK(teb_event_bus_drain(bus) == 0);
    CHECK(strcmp(trace, "") == 0);

    teb_event_bus_clear_posted(bus); // dropping an empty queue changes nothing
    CHECK(teb_event_bus_count_posted(bus) == 0);

    teb_event_bus_drop(bus);
}

static void test_clear_clears_queue() {
    teb_EventBus *bus = teb_event_bus_new(EV_COUNT);
    trace_reset();

    CHECK(teb_event_bus_subscribe(bus, EV_X, h_a, nullptr));
    CHECK(teb_event_bus_post(bus, EV_X));

    teb_event_bus_clear(bus);
    CHECK(teb_event_bus_count_posted(bus) == 0);
    CHECK(teb_event_bus_drain(bus) == 0);
    CHECK(strcmp(trace, "") == 0);

    teb_event_bus_drop(bus);
}

// a bus torn down with events still queued owns no payload pointers to leak
static void test_drop_with_queued() {
    teb_EventBus *bus = teb_event_bus_new(EV_COUNT);

    const Pair p = {.a = 7, .b = 8};
    CHECK(teb_event_bus_post_data(bus, EV_X, &p, sizeof(p)));
    CHECK(teb_event_bus_count_posted(bus) == 1);

    teb_event_bus_drop(bus);
}

// a bus sized for itself: small ring, small slot, everything still holds
static void test_new_cap_sizes() {
    constexpr size_t cap = 4;

    teb_EventBus *bus = teb_event_bus_new_cap(EV_COUNT, sizeof(Pair), cap);
    CHECK(bus);
    pair_seen = (Pair){};
    CHECK(teb_event_bus_subscribe(bus, EV_X, h_pair, nullptr));

    // a payload of exactly the slot size fits
    const Pair p = {.a = 3, .b = 4};
    CHECK(teb_event_bus_post_data(bus, EV_X, &p, sizeof(p)));
    CHECK(teb_event_bus_drain(bus) == 1);
    CHECK(pair_seen.a == 3 && pair_seen.b == 4);

    // the custom cap is the one enforced, not the default
    bool all_ok = true;
    for (size_t i = 0; i < cap; ++i) {
        if (!teb_event_bus_post_data(bus, EV_X, &p, sizeof(p))) {
            all_ok = false;
        }
    }
    CHECK(all_ok);
    CHECK(teb_event_bus_count_posted(bus) == cap);
    CHECK(!teb_event_bus_post_data(bus, EV_X, &p, sizeof(p)));
    CHECK(teb_event_bus_drain(bus) == cap);

    // and the ring wraps on the small cap just the same
    all_ok = true;
    for (size_t round = 0; round < 3; ++round) {
        for (size_t i = 0; i < cap; ++i) {
            if (!teb_event_bus_post_data(bus, EV_X, &p, sizeof(p))) {
                all_ok = false;
            }
        }
        if (teb_event_bus_drain(bus) != cap) {
            all_ok = false;
        }
    }
    CHECK(all_ok);

    teb_event_bus_drop(bus);
}

// slot_size 0: signals only, and no payload storage is ever allocated
static void test_new_cap_no_payload() {
    teb_EventBus *bus = teb_event_bus_new_cap(EV_COUNT, 0, 2);
    CHECK(bus);
    no_payload_ok = false;

    CHECK(teb_event_bus_subscribe(bus, EV_X, h_no_payload, nullptr));
    CHECK(teb_event_bus_post(bus, EV_X));
    CHECK(teb_event_bus_drain(bus) == 1);
    CHECK(no_payload_ok);

    teb_event_bus_drop(bus);
}

// a slot size that is not a multiple of max_align_t: the stride has to be
// rounded up, or every other slot lands misaligned for what it holds
static void test_new_cap_odd_slot_size() {
    constexpr size_t cap = 4;

    teb_EventBus *bus = teb_event_bus_new_cap(EV_COUNT, sizeof(double) + 1, cap);
    CHECK(bus);
    dbl_ok = true;
    CHECK(teb_event_bus_subscribe(bus, EV_X, h_dbl, nullptr));

    // two rounds, so the payload is read from every slot in the ring
    bool all_ok = true;
    for (size_t round = 0; round < 2; ++round) {
        for (size_t i = 0; i < cap; ++i) {
            const double d = 1.5;
            if (!teb_event_bus_post_data(bus, EV_X, &d, sizeof(d))) {
                all_ok = false;
            }
        }
        if (teb_event_bus_drain(bus) != cap) {
            all_ok = false;
        }
    }
    CHECK(all_ok);
    CHECK(dbl_ok);

    teb_event_bus_drop(bus);
}

// sizes whose product cannot be indexed are refused at creation, not at post
static void test_new_cap_rejects_overflow() {
    CHECK(!teb_event_bus_new_cap(EV_COUNT, SIZE_MAX, 2));
    CHECK(!teb_event_bus_new_cap(EV_COUNT, 64, SIZE_MAX));
    CHECK(!teb_event_bus_new_cap(EV_COUNT, SIZE_MAX / 2, 4));

    teb_EventBus *bus = teb_event_bus_new_cap(EV_COUNT, 0, SIZE_MAX); // headers alone still overflow
    CHECK(!bus);
}

int main() {
    test_empty_bus();
    test_subscribe_rules();
    test_fifo_order();
    test_payload();
    test_unsubscribe();
    test_unsubscribe_during_dispatch();
    test_unsubscribe_ctx();
    test_nested_publish();
    test_growth();
    test_grow_during_dispatch();
    test_reserve();
    test_shrink();
    test_clear();
    test_post_defers();
    test_post_copies_payload();
    test_post_fifo_order();
    test_drain_snapshot();
    test_post_queue_full();
    test_post_ring_wraps();
    test_subscribers_resolved_at_drain();
    test_post_without_payload();
    test_new_cap_sizes();
    test_new_cap_no_payload();
    test_new_cap_odd_slot_size();
    test_new_cap_rejects_overflow();
    test_slot_held_during_dispatch();
    test_publish_from_drain();
    test_clear_posted();
    test_clear_clears_queue();
    test_drop_with_queued();

    printf("%d checks, %d failed\n", checks_run, checks_failed);
    return checks_failed != 0;
}
