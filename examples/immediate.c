// Immediate delivery: teb_event_bus_publish runs every handler before it returns.
//
// The goblin dies inside the damage dispatch and frees itself on the spot, so
// on_damage_report -- registered after the handler that killed it -- never sees
// the fatal hit. That missing "goblin is at 0 hp" line is the tombstone model
// working: a subscription removed mid-dispatch is skipped, not left dangling.

#include "teb/event_bus.h"

#include <assert.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>

#define UNUSED(x) (void) (x)

typedef enum : int32_t {
    EV_DAMAGE = 0,
    EV_DIED,
    EV_COUNT
} GameEvent;

typedef struct {
    const char *name;
    int hp;
} Enemy;

typedef struct {
    int amount;
} Damage;

static void on_damage_apply(const teb_Event *ev, teb_EventBus *bus, void *ctx) {
    TEB_EVENT_EXPECT(Damage, ev);
    TEB_CTX_EXPECT(Enemy, ctx);

    Enemy *self = TEB_CTX_MUT_AS(Enemy, ctx);
    const Damage dmg = *TEB_EVENT_DATA_AS(Damage, ev);

    self->hp -= dmg.amount;
    printf("%s takes %d damage\n", self->name, dmg.amount);

    if (self->hp <= 0) {
        [[maybe_unused]] const bool ok = teb_event_bus_publish(bus, EV_DIED);
        assert(ok);
    }
}

static void on_damage_report(const teb_Event *ev, teb_EventBus *bus, void *ctx) {
    UNUSED(ev);
    UNUSED(bus);
    TEB_CTX_EXPECT(Enemy, ctx);

    const Enemy *self = TEB_CTX_AS(Enemy, ctx);
    printf("  %s is at %d hp\n", self->name, self->hp);
}

static void on_died(const teb_Event *ev, teb_EventBus *bus, void *ctx) {
    UNUSED(ev);
    TEB_CTX_EXPECT(Enemy, ctx);

    Enemy *self = TEB_CTX_MUT_AS(Enemy, ctx);
    printf("  %s died\n", self->name);

    const size_t dropped = teb_event_bus_unsubscribe_ctx(bus, self);
    printf("  dropped %zu coins\n", dropped);

    free(self);
}

int main() {
    teb_EventBus *bus = teb_event_bus_new(EV_COUNT);
    assert(bus);

    Enemy *goblin = malloc(sizeof(Enemy));
    assert(goblin);
    *goblin = (Enemy){.name = "goblin", .hp = 5};

    [[maybe_unused]] bool ok = teb_event_bus_subscribe(bus, EV_DAMAGE, on_damage_apply, goblin);
    assert(ok);

    ok = teb_event_bus_subscribe(bus, EV_DAMAGE, on_damage_report, goblin);
    assert(ok);

    ok = teb_event_bus_subscribe(bus, EV_DIED, on_died, goblin);
    assert(ok);

    Damage light = {.amount = 2};
    TEB_EVENT_BUS_PUBLISH_DATA(bus, EV_DAMAGE, light);

    Damage fatal = {.amount = 3};
    TEB_EVENT_BUS_PUBLISH_DATA(bus, EV_DAMAGE, fatal); // on_damage_report never runs

    TEB_EVENT_BUS_PUBLISH_DATA(bus, EV_DAMAGE, light); // nobody is listening any more

    teb_event_bus_drop(bus);
}
