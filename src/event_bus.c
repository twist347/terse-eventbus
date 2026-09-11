#include "teb/event_bus.h"

#include <assert.h>
#include <stdlib.h>
#include <string.h>

/* ========== internal ========== */

#define TYPE_IS_VALID(self, t) \
    ((t) >= 0 && (size_t) (t) < (self)->type_count)

// Removal model: tombstones.
//
// A removed subscription is marked dead (handler == nullptr) instead of being
// erased, so indices stay stable while a dispatch loop walks the array. Dead
// slots are swept once the outermost dispatch returns.
//
// Invariants:
//   * len is the used-slot border, NOT the number of live handlers;
//   * dispatch_depth == 0  =>  total_dead == 0, i.e. the array is dense
//     outside a dispatch, so teb_event_bus_subscribe may simply append;
//   * a dispatch loop fixes its upper bound before calling anything, so a
//     subscription appended during dispatch cannot see the event in flight;
//   * slots in [len, cap) are zeroed and never read.
//
// Growth: teb_event_bus_subscribe may realloc from inside a handler, so any
// Subscription * taken before a call into user code is dangling afterwards.
// Index the vector instead of holding a pointer across such a call.

static bool grow(teb_EventBus *self, teb_EventType type);

static void mark_dead(teb_EventBus *self, teb_EventType type, size_t idx);

static void compact_type(teb_EventBus *self, teb_EventType type);

static void compact_all(teb_EventBus *self);

static bool queue_alloc(teb_EventBus *self);

/* ========== event ========== */

struct teb_Event {
    teb_EventType type;
    const void *data;
    size_t data_size;
};

teb_EventType teb_event_type(const teb_Event *self) {
    assert(self);

    return self->type;
}

const void *teb_event_data(const teb_Event *self) {
    assert(self);

    return self->data;
}

size_t teb_event_data_size(const teb_Event *self) {
    assert(self);

    return self->data_size;
}

/* ========== event bus ========== */

typedef struct {
    teb_EventHandler handler;
    void *ctx;
} Subscription;

// Growable, never shrinks: compaction reclaims slots but keeps the capacity.
typedef struct {
    Subscription *data;
    size_t len;
    size_t cap;
    size_t dead;
} SubscriptionVec;

typedef struct {
    teb_EventType type;
    size_t data_size;
} PostHeader;

// Ring of fixed-size slots: a post copies its payload in, a drain dispatches
// straight out of it. Headers and payloads sit in separate arrays because the
// payload size is chosen at run time, so a slot cannot be one struct. The
// payload base comes from malloc and the stride is a multiple of max_align_t,
// which is what lands every slot aligned for whatever type was posted -- the
// alignment TEB_EVENT_EXPECT asserts on.
//
// Neither array is resized, so unlike SubscriptionVec they never move and a
// slot stays put across a call into user code.
typedef struct {
    PostHeader *headers;
    unsigned char *payloads; // nullptr while slot_size is 0
    size_t head;
    size_t count;
    size_t cap;
    size_t slot_size; // largest payload a caller may post
    size_t stride;    // slot_size rounded up to max_align_t
} PostQueue;

struct teb_EventBus {
    size_t type_count;
    SubscriptionVec *subs;
    size_t total_dead;
    size_t dispatch_depth;
    PostQueue queue;
    bool draining;
};

teb_EventBus *teb_event_bus_new(size_t type_count) {
    return teb_event_bus_new_cap(type_count, TEB_DEFAULT_POST_SLOT_SIZE, TEB_DEFAULT_POST_QUEUE_CAP);
}

teb_EventBus *teb_event_bus_new_cap(size_t type_count, size_t post_slot_size, size_t post_queue_cap) {
    assert(type_count > 0);
    assert(post_queue_cap > 0); // a feature is not switched off by sizing it to zero

    constexpr size_t align = alignof(max_align_t);

    // the queue arithmetic is settled once, here, so that every later post can
    // index the ring without rechecking anything
    if (post_slot_size > SIZE_MAX - (align - 1)) {
        return nullptr;
    }
    const size_t stride = (post_slot_size + align - 1) / align * align;

    if (post_queue_cap > SIZE_MAX / sizeof(PostHeader)) {
        return nullptr;
    }
    if (stride > 0 && post_queue_cap > SIZE_MAX / stride) {
        return nullptr;
    }

    teb_EventBus *obj = malloc(sizeof(teb_EventBus));
    if (!obj) {
        return nullptr;
    }

    // every vector starts empty; storage is allocated on first subscribe
    obj->type_count = type_count;
    obj->subs = calloc(type_count, sizeof(*obj->subs));
    obj->total_dead = 0;
    obj->dispatch_depth = 0;

    // the queue buffers are allocated on the first post, not here
    obj->queue = (PostQueue){.cap = post_queue_cap, .slot_size = post_slot_size, .stride = stride};
    obj->draining = false;

    if (!obj->subs) {
        free(obj);
        return nullptr;
    }
    return obj;
}

void teb_event_bus_drop(teb_EventBus *self) {
    if (!self) {
        return;
    }
    for (size_t t = 0; t < self->type_count; ++t) {
        free(self->subs[t].data);
    }
    free(self->subs);
    free(self->queue.headers);
    free(self->queue.payloads);
    free(self);
}

void teb_event_bus_clear(teb_EventBus *self) {
    assert(self);
    assert(self->dispatch_depth == 0); // implies no drain is in flight either

    for (size_t t = 0; t < self->type_count; ++t) {
        SubscriptionVec *v = &self->subs[t];
        if (v->len > 0) {
            memset(v->data, 0, v->len * sizeof(*v->data));
        }
        v->len = 0;
        v->dead = 0;
    }
    self->total_dead = 0;

    // queued events go too; the queue keeps its allocation
    self->queue.head = 0;
    self->queue.count = 0;
}

bool teb_event_bus_reserve(teb_EventBus *self, teb_EventType type, size_t cap) {
    assert(self);

    if (!TYPE_IS_VALID(self, type)) {
        return false;
    }

    SubscriptionVec *v = &self->subs[type];
    if (cap <= v->cap) {
        return true;
    }

    if (cap > SIZE_MAX / sizeof(*v->data)) {
        return false;
    }

    Subscription *data = realloc(v->data, cap * sizeof(*data));
    if (!data) {
        return false;
    }

    memset(&data[v->cap], 0, (cap - v->cap) * sizeof(*data));
    v->data = data;
    v->cap = cap;

    return true;
}

void teb_event_bus_shrink_to_fit(teb_EventBus *self) {
    assert(self);
    assert(self->dispatch_depth == 0);
    assert(self->total_dead == 0); // dense outside a dispatch, so len == live

    for (size_t t = 0; t < self->type_count; ++t) {
        SubscriptionVec *v = &self->subs[t];
        if (v->len == v->cap) {
            continue;
        }

        if (v->len == 0) {
            free(v->data);
            v->data = nullptr;
            v->cap = 0;
            continue;
        }

        Subscription *data = realloc(v->data, v->len * sizeof(*data));
        if (data) {
            v->data = data;
            v->cap = v->len;
        }
    }
}

/* ========== event bus subs ========== */

bool teb_event_bus_subscribe(teb_EventBus *self, teb_EventType type, teb_EventHandler handler, void *ctx) {
    assert(self);
    assert(handler);

    if (!TYPE_IS_VALID(self, type)) {
        return false;
    }

    SubscriptionVec *v = &self->subs[type];

    // forbid duplicates; dead slots hold handler == nullptr and never match
    const size_t n = v->len;
    for (size_t i = 0; i < n; ++i) {
        if (v->data[i].handler == handler && v->data[i].ctx == ctx) {
            return false;
        }
    }

    // append only: reusing a dead slot would expose the new handler to a
    // dispatch already in flight
    if (n == v->cap && !grow(self, type)) {
        return false;
    }

    // FIFO
    v->data[n].handler = handler;
    v->data[n].ctx = ctx;
    v->len = n + 1;

    return true;
}

bool teb_event_bus_unsubscribe(teb_EventBus *self, teb_EventType type, teb_EventHandler handler, void *ctx) {
    assert(self);
    assert(handler);

    if (!TYPE_IS_VALID(self, type)) {
        return false;
    }

    SubscriptionVec *v = &self->subs[type];
    const size_t n = v->len;
    for (size_t i = 0; i < n; ++i) {
        if (v->data[i].handler == handler && v->data[i].ctx == ctx) {
            mark_dead(self, type, i);
            if (self->dispatch_depth == 0) {
                compact_type(self, type);
            }
            return true;
        }
    }

    return false;
}

void teb_event_bus_unsubscribe_type(teb_EventBus *self, teb_EventType type) {
    assert(self);

    if (!TYPE_IS_VALID(self, type)) {
        return;
    }

    SubscriptionVec *v = &self->subs[type];
    const size_t n = v->len;
    for (size_t i = 0; i < n; ++i) {
        if (v->data[i].handler) {
            mark_dead(self, type, i);
        }
    }

    if (self->dispatch_depth == 0) {
        compact_type(self, type);
    }
}

size_t teb_event_bus_unsubscribe_ctx(teb_EventBus *self, const void *ctx) {
    assert(self);

    size_t removed = 0;
    for (size_t t = 0; t < self->type_count; ++t) {
        SubscriptionVec *v = &self->subs[t];
        const size_t n = v->len;
        for (size_t i = 0; i < n; ++i) {
            if (v->data[i].handler && v->data[i].ctx == ctx) {
                mark_dead(self, (teb_EventType) t, i);
                ++removed;
            }
        }
    }

    if (self->dispatch_depth == 0) {
        compact_all(self);
    }
    return removed;
}

size_t teb_event_bus_unsubscribe_handler(teb_EventBus *self, teb_EventHandler handler) {
    assert(self);
    assert(handler);

    size_t removed = 0;
    for (size_t t = 0; t < self->type_count; ++t) {
        SubscriptionVec *v = &self->subs[t];
        const size_t n = v->len;
        for (size_t i = 0; i < n; ++i) {
            if (v->data[i].handler == handler) {
                mark_dead(self, (teb_EventType) t, i);
                ++removed;
            }
        }
    }

    if (self->dispatch_depth == 0) {
        compact_all(self);
    }
    return removed;
}

size_t teb_event_bus_count_subscribers(const teb_EventBus *self, teb_EventType type) {
    assert(self);

    if (!TYPE_IS_VALID(self, type)) {
        return 0;
    }

    // dead slots sit inside the border, so the difference is exact and O(1)
    return self->subs[type].len - self->subs[type].dead;
}

/* ========== event bus publish ========== */

bool teb_event_bus_publish_data(teb_EventBus *self, teb_EventType type, const void *data, size_t data_size) {
    assert(self);
    assert((data == nullptr) == (data_size == 0));

    if (!TYPE_IS_VALID(self, type)) {
        return false;
    }

    if (self->dispatch_depth >= TEB_MAX_DISPATCH_DEPTH) {
        assert(0 && "teb_event_bus_publish_data: dispatch depth limit exceeded");
        return false;
    }

    const teb_Event ev = {.type = type, .data = data, .data_size = data_size};

    SubscriptionVec *v = &self->subs[ev.type];

    // fixed before any handler runs: subscriptions appended during dispatch
    // stay out of this event
    const size_t n = v->len;
    if (n == 0) {
        return true;
    }

    ++self->dispatch_depth;
    for (size_t i = 0; i < n; ++i) {
        // re-read the slot every iteration and copy it out: a handler may kill
        // itself or a peer, and a subscribe from inside one may have moved the
        // base pointer. The vector header itself never moves.
        const Subscription s = v->data[i];
        if (!s.handler) {
            continue;
        }
        s.handler(&ev, self, s.ctx);
    }
    --self->dispatch_depth;

    if (self->dispatch_depth == 0 && self->total_dead > 0) {
        compact_all(self);
    }

    return true;
}

bool teb_event_bus_publish(teb_EventBus *self, teb_EventType type) {
    assert(self);

    return teb_event_bus_publish_data(self, type, nullptr, 0);
}

/* ========== event bus deferred publish ========== */

bool teb_event_bus_post_data(teb_EventBus *self, teb_EventType type, const void *data, size_t data_size) {
    assert(self);
    assert((data == nullptr) == (data_size == 0));

    if (!TYPE_IS_VALID(self, type)) {
        return false;
    }

    PostQueue *q = &self->queue;

    if (data_size > q->slot_size) {
        assert(0 && "teb_event_bus_post_data: payload above this bus's post slot size");
        return false;
    }

    if (!q->headers && !queue_alloc(self)) {
        return false;
    }

    if (q->count == q->cap) {
        return false;
    }

    // the payload is copied, not borrowed: by the time it is dispatched the
    // frame it came from is long gone
    const size_t idx = (q->head + q->count) % q->cap;
    q->headers[idx] = (PostHeader){.type = type, .data_size = data_size};
    if (data_size > 0) {
        memcpy(q->payloads + idx * q->stride, data, data_size);
    }
    ++q->count;

    return true;
}

bool teb_event_bus_post(teb_EventBus *self, teb_EventType type) {
    assert(self);

    return teb_event_bus_post_data(self, type, nullptr, 0);
}

size_t teb_event_bus_drain(teb_EventBus *self) {
    assert(self);

    // a handler always runs at a non-zero depth, whichever path reached it, so
    // this rejects a drain nested in a publish and one nested in a drain alike
    if (self->dispatch_depth != 0) {
        assert(0 && "teb_event_bus_drain: called from inside a handler");
        return 0;
    }

    PostQueue *q = &self->queue;

    // fixed before any handler runs, mirroring teb_event_bus_publish_data: a post made
    // during the drain waits for the next one, so two handlers posting to each
    // other cannot keep this loop alive
    const size_t n = q->count;
    if (n == 0) {
        return 0;
    }

    self->draining = true;
    for (size_t i = 0; i < n; ++i) {
        const PostHeader h = q->headers[q->head];
        const void *data = h.data_size > 0 ? q->payloads + q->head * q->stride : nullptr;

        // ev.data points into the slot, so the slot is released only after the
        // dispatch: freeing it first would let a post from one of these
        // handlers reuse it and overwrite the payload being read
        (void) teb_event_bus_publish_data(self, h.type, data, h.data_size);

        q->head = (q->head + 1) % q->cap;
        --q->count;
    }
    self->draining = false;

    return n;
}

void teb_event_bus_clear_posted(teb_EventBus *self) {
    assert(self);

    // dropping mid-drain would pull the slots out from under the loop. Depth is
    // not the test here: dropping from inside a plain publish harms nothing.
    if (self->draining) {
        assert(0 && "teb_event_bus_clear_posted: called from inside a drain");
        return;
    }

    self->queue.head = 0;
    self->queue.count = 0;
}

size_t teb_event_bus_count_posted(const teb_EventBus *self) {
    assert(self);

    return self->queue.count;
}

/* ========== internal ========== */

static bool grow(teb_EventBus *self, teb_EventType type) {
    SubscriptionVec *v = &self->subs[type];
    assert(v->len == v->cap);

    const size_t old_cap = v->cap;
    const size_t cap = old_cap ? old_cap * 2 : 4;

    Subscription *data = realloc(v->data, cap * sizeof(*data));
    if (!data) {
        return false;
    }

    memset(&data[old_cap], 0, (cap - old_cap) * sizeof(*data));
    v->data = data;
    v->cap = cap;

    return true;
}

static void mark_dead(teb_EventBus *self, teb_EventType type, size_t idx) {
    SubscriptionVec *v = &self->subs[type];
    assert(v->data[idx].handler);

    v->data[idx].handler = nullptr;
    v->data[idx].ctx = nullptr;
    ++v->dead;
    ++self->total_dead;
}

static void compact_type(teb_EventBus *self, teb_EventType type) {
    assert(self->dispatch_depth == 0);

    SubscriptionVec *v = &self->subs[type];

    const size_t dead = v->dead;
    if (dead == 0) {
        return;
    }

    Subscription *arr = v->data;
    const size_t n = v->len;

    size_t w = 0;
    for (size_t r = 0; r < n; ++r) {
        if (arr[r].handler) {
            arr[w++] = arr[r];
        }
    }
    assert(w + dead == n);

    // capacity is kept: the vector never shrinks
    memset(&arr[w], 0, dead * sizeof(arr[0]));
    v->len = w;
    v->dead = 0;
    self->total_dead -= dead;
}

static void compact_all(teb_EventBus *self) {
    for (size_t t = 0; t < self->type_count && self->total_dead > 0; ++t) {
        compact_type(self, (teb_EventType) t);
    }
}

static bool queue_alloc(teb_EventBus *self) {
    PostQueue *q = &self->queue;
    assert(!q->headers);
    assert(q->stride % alignof(max_align_t) == 0); // what keeps every slot aligned

    // neither product can overflow: both were checked in teb_event_bus_new_cap. Not
    // zeroed either -- a slot is written whole before it is ever read, and
    // zeroing would touch every page of a buffer most buses never fill
    q->headers = malloc(q->cap * sizeof(*q->headers));
    if (!q->headers) {
        return false;
    }

    if (q->stride > 0) {
        q->payloads = malloc(q->cap * q->stride);
        if (!q->payloads) {
            free(q->headers);
            q->headers = nullptr;
            return false;
        }
    }

    return true;
}
