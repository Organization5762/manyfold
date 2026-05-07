"""Disposable implementations re-exported through Manyfold."""

from reactivex.disposable import (
    BooleanDisposable as BooleanDisposable,
    CompositeDisposable as CompositeDisposable,
    Disposable as Disposable,
    MultipleAssignmentDisposable as MultipleAssignmentDisposable,
    RefCountDisposable as RefCountDisposable,
    ScheduledDisposable as ScheduledDisposable,
    SerialDisposable as SerialDisposable,
    SingleAssignmentDisposable as SingleAssignmentDisposable,
)

__all__ = (
    "BooleanDisposable",
    "CompositeDisposable",
    "Disposable",
    "MultipleAssignmentDisposable",
    "RefCountDisposable",
    "ScheduledDisposable",
    "SerialDisposable",
    "SingleAssignmentDisposable",
)
