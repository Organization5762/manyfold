"""Rx-compatible streaming facade for Manyfold consumers.

Manyfold still builds on RxPy for its in-process Python stream surface. This
module gives downstream projects a Manyfold-owned import path while the
underlying implementation evolves.
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
