"""Private facade for Rust-backed native Manyfold architecture elements."""

from __future__ import annotations

from typing import Literal

from manyfold._manyfold_rust import (
    ArchitectureCapacitor as Capacitor,
    ArchitectureGround as Ground,
    ArchitecturePad as Pad,
    ArchitectureProbe as Probe,
    ArchitectureRegulator as Regulator,
    ArchitectureRelay as Relay,
    ArchitectureResistor as Resistor,
    ArchitectureVia as Via,
    CalibratedClock as CalibratedClock,
    Clock as Clock,
    ClockCalibrationSample as ClockCalibrationSample,
    MonotonicLogicalClock as MonotonicLogicalClock,
    NtpTimeProvider as NtpTimeProvider,
    SystemTimeProvider as SystemTimeProvider,
)

PadDirection = Literal["input", "output", "internal"]

__all__ = [
    "CalibratedClock",
    "Capacitor",
    "Clock",
    "ClockCalibrationSample",
    "Ground",
    "MonotonicLogicalClock",
    "NtpTimeProvider",
    "Pad",
    "PadDirection",
    "Probe",
    "Regulator",
    "Relay",
    "Resistor",
    "SystemTimeProvider",
    "Via",
]
