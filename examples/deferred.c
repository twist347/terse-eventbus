// Deferred delivery: teb_event_bus_post queues an event, teb_event_bus_drain
// dispatches it later.
//
// A frame loop is the natural home for this. The tick dispatch walks every
// enemy, so an enemy that dies must not free itself right there -- it posts
// EV_DIED instead, and the reaper runs at the end of the frame, once the walk
// is over. The reaper in turn posts EV_LOOT, which by the snapshot rule lands
// in the NEXT drain: a drain handles what was queued when it started, never
// what its own handlers add. That is what keeps two handlers posting to each
// other from spinning a drain forever.

#include "teb/event_bus.h"

#include <assert.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>

#define UNUSED(x) (void) (x)

typedef enum : int32_t {
    EV_TICK = 0,
    EV_DIED,
    EV_LOOT,
    EV_COUNT
} GameEvent;

typedef struct {
    const char *name;
    int hp;
} Enemy;

typedef struct {
    int damage;
} Tick;

typedef struct {
    Enemy *who;
} Died;

typedef struct {
    const char *from;
    int gold;
} Loot;

static void on_tick(const teb_Event *ev, teb_EventBus *bus, void *ctx) {
    TEB_EVENT_EXPECT(Tick, ev);
    TEB_CTX_EXPECT(Enemy, ctx);

    Enemy *self = TEB_CTX_MUT_AS(Enemy, ctx);
    const Tick tick = *TEB_EVENT_DATA_AS(Tick, ev);

    self->hp -= tick.damage;
    printf("   %s: %d hp\n", self->name, self->hp);

    if (self->hp <= 0) {
        // NOT teb_event_bus_publish: unsubscribing and freeing here would happen
        // while the tick dispatch is still walking the subscriber list
        const Died died = {.who = self};
        [[maybe_unused]] const bool ok = teb_event_bus_post_data(bus, EV_DIED, &died, sizeof(died));
        assert(ok);
    }
}

static void on_died(const teb_Event *ev, teb_EventBus *bus, void *ctx) {
    UNUSED(ctx);
    TEB_EVENT_EXPECT(Died, ev);

    Enemy *who = TEB_EVENT_DATA_AS(Died, ev)->who;
    printf("   reaped %s\n", who->name);

    [[maybe_unused]] const size_t dropped = teb_event_bus_unsubscribe_ctx(bus, who);
    assert(dropped == 1);
    free(who);

    // posted from inside a drain, so it waits for the next one
    const Loot loot = {.from = "a corpse", .gold = 10};
    [[maybe_unused]] const bool ok = teb_event_bus_post_data(bus, EV_LOOT, &loot, sizeof(loot));
    assert(ok);
}

static void on_loot(const teb_Event *ev, teb_EventBus *bus, void *ctx) {
    UNUSED(bus);
    UNUSED(ctx);
    TEB_EVENT_EXPECT(Loot, ev);

    const Loot loot = *TEB_EVENT_DATA_AS(Loot, ev);
    printf("   looted %d gold from %s\n", loot.gold, loot.from);
}

// ownership goes to the bus: the reaper frees the enemy when it dies
static void spawn(teb_EventBus *bus, const char *name, int hp) {
    Enemy *e = malloc(sizeof(Enemy));
    assert(e);
    *e = (Enemy){.name = name, .hp = hp};

    [[maybe_unused]] const bool ok = teb_event_bus_subscribe(bus, EV_TICK, on_tick, e);
    assert(ok);
}

int main() {
    teb_EventBus *bus = teb_event_bus_new(EV_COUNT);
    assert(bus);

    [[maybe_unused]] bool ok = teb_event_bus_subscribe(bus, EV_DIED, on_died, nullptr);
    assert(ok);
    ok = teb_event_bus_subscribe(bus, EV_LOOT, on_loot, nullptr);
    assert(ok);

    spawn(bus, "goblin", 5);
    spawn(bus, "orc", 4);
    spawn(bus, "rat", 2);

    const Tick tick = {.damage = 2};

    for (int frame = 1; frame <= 3; ++frame) {
        printf("frame %d\n", frame);

        printf("  update\n");
        TEB_EVENT_BUS_PUBLISH_DATA(bus, EV_TICK, tick);

        printf("  drain (%zu queued)\n", teb_event_bus_count_posted(bus));
        (void) teb_event_bus_drain(bus);
    }

    // the last reaper's loot is still waiting: one drain behind, by design
    printf("teardown\n");
    printf("  drain (%zu queued)\n", teb_event_bus_count_posted(bus));
    (void) teb_event_bus_drain(bus);

    // nobody outlived the fight, so the reaper freed every enemy. Survivors
    // would have to be freed here instead -- this asserts we have none.
    assert(teb_event_bus_count_subscribers(bus, EV_TICK) == 0);
    assert(teb_event_bus_count_posted(bus) == 0);

    teb_event_bus_drop(bus);
}
