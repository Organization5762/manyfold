"""Private Rx-compatible streaming facade for Manyfold internals.

Manyfold still builds on RxPy for its in-process Python stream surface. This
module keeps the re-export surface available to internal code without making it
part of the public ``manyfold`` namespace.
"""

from reactivex import *  # noqa: F403
from reactivex import (
    Observable as Observable,
    amb as amb,
    case as case,
    catch as catch,
    combine_latest as combine_latest,
    concat as concat,
    create as create,
    defer as defer,
    empty as empty,
    fork_join as fork_join,
    from_callable as from_callable,
    from_callback as from_callback,
    from_future as from_future,
    from_iterable as from_iterable,
    from_marbles as from_marbles,
    generate as generate,
    generate_with_relative_time as generate_with_relative_time,
    if_then as if_then,
    interval as interval,
    just as just,
    merge as merge,
    never as never,
    of as of,
    operators as operators,
    pipe as pipe,
    range as range,
    repeat_value as repeat_value,
    return_value as return_value,
    start as start,
    start_async as start_async,
    throw as throw,
    timer as timer,
    to_async as to_async,
    using as using,
    with_latest_from as with_latest_from,
)
from reactivex.subject import Subject as Subject

from . import abc as abc


def _install_fluent_observable_methods() -> None:
    def _map(self: Observable, mapper):  # type: ignore[no-untyped-def]
        return self.pipe(operators.map(mapper))

    def _filter(self: Observable, predicate):  # type: ignore[no-untyped-def]
        return self.pipe(operators.filter(predicate))

    def _scan(self: Observable, accumulator, seed=None):  # type: ignore[no-untyped-def]
        return self.pipe(operators.scan(accumulator, seed=seed))

    def _start_with(self: Observable, *values):  # type: ignore[no-untyped-def]
        return self.pipe(operators.start_with(*values))

    def _distinct_until_changed(  # type: ignore[no-untyped-def]
        self: Observable,
        key_mapper=None,
        comparer=None,
    ):
        return self.pipe(
            operators.distinct_until_changed(
                key_mapper=key_mapper,
                comparer=comparer,
            )
        )

    def _do_action(  # type: ignore[no-untyped-def]
        self: Observable,
        on_next=None,
        on_error=None,
        on_completed=None,
    ):
        return self.pipe(
            operators.do_action(
                on_next=on_next,
                on_error=on_error,
                on_completed=on_completed,
            )
        )

    def _pairwise(self: Observable):  # type: ignore[no-untyped-def]
        return self.pipe(operators.pairwise())

    def _take(self: Observable, count):  # type: ignore[no-untyped-def]
        return self.pipe(operators.take(count))

    def _with_latest_from(self: Observable, *sources):  # type: ignore[no-untyped-def]
        return self.pipe(operators.with_latest_from(*sources))

    def _flat_map(self: Observable, mapper):  # type: ignore[no-untyped-def]
        return self.pipe(operators.flat_map(mapper))

    def _switch_latest(self: Observable):  # type: ignore[no-untyped-def]
        return self.pipe(operators.switch_latest())

    methods = {
        "map": _map,
        "filter": _filter,
        "scan": _scan,
        "start_with": _start_with,
        "distinct_until_changed": _distinct_until_changed,
        "do_action": _do_action,
        "pairwise": _pairwise,
        "take": _take,
        "with_latest_from": _with_latest_from,
        "flat_map": _flat_map,
        "switch_latest": _switch_latest,
    }
    for name, method in methods.items():
        if not hasattr(Observable, name):
            setattr(Observable, name, method)


_install_fluent_observable_methods()
