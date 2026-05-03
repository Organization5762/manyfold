"""Rx-compatible streaming facade for Manyfold consumers.

Manyfold still builds on RxPy for its in-process Python stream surface. This
module gives downstream projects a Manyfold-owned import path while the
underlying implementation evolves.
"""

from reactivex import *  # noqa: F403
from reactivex import Observable as Observable
from . import abc as abc
from reactivex import amb as amb
from reactivex import case as case
from reactivex import catch as catch
from reactivex import combine_latest as combine_latest
from reactivex import concat as concat
from reactivex import create as create
from reactivex import defer as defer
from reactivex import empty as empty
from reactivex import fork_join as fork_join
from reactivex import from_callable as from_callable
from reactivex import from_callback as from_callback
from reactivex import from_future as from_future
from reactivex import from_iterable as from_iterable
from reactivex import from_marbles as from_marbles
from reactivex import generate as generate
from reactivex import generate_with_relative_time as generate_with_relative_time
from reactivex import if_then as if_then
from reactivex import interval as interval
from reactivex import just as just
from reactivex import merge as merge
from reactivex import never as never
from reactivex import of as of
from reactivex import operators as operators
from reactivex import pipe as pipe
from reactivex import range as range
from reactivex import repeat_value as repeat_value
from reactivex import return_value as return_value
from reactivex import start as start
from reactivex import start_async as start_async
from reactivex import throw as throw
from reactivex import timer as timer
from reactivex import to_async as to_async
from reactivex import using as using
from reactivex import with_latest_from as with_latest_from
from reactivex.subject import Subject as Subject
