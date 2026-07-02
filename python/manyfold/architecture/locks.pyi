from __future__ import annotations

class ManyFoldLock:
    name: str
    path: str
    def __init__(self, name: str) -> None: ...
    @classmethod
    def for_resource(cls, name: str) -> ManyFoldLock: ...
    def take(
        self,
        *,
        owner: str | None = None,
        blocking: bool = True,
    ) -> ManyFoldLockLease: ...

class ManyFoldLockLease:
    lock_name: str
    owner: str
    acquired_time_ns: int
    is_released: bool
    def release(self) -> bool: ...
    def __enter__(self) -> ManyFoldLockLease: ...
    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None: ...

Lock = ManyFoldLock
LockLease = ManyFoldLockLease

__all__: list[str]
