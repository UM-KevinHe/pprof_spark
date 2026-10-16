"""Null distributions for z in tests on standardized measures (pprof_py's `TheoreticalNull` and `FixedNull`).

Standardization specification §2a (D-49). Empirical nulls are not implemented yet (OI-78).
"""
import math
from dataclasses import dataclass


@dataclass(frozen=True)
class TheoreticalNull:
    """N(0, 1), the default null for z."""


@dataclass(frozen=True)
class FixedNull:
    """N(mean, sd²): z is calibrated to (z - mean) / sd before p-values, flags and intervals."""
    mean: float = 0.0
    sd: float = 1.0

    def __post_init__(self):
        if not (math.isfinite(self.mean) and math.isfinite(self.sd) and self.sd > 0):
            raise ValueError("FixedNull needs a finite mean and a positive finite sd.")
