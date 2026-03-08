"""Tongdaxin-compatible BACKSET implementation.

MyTT does not provide BACKSET, so this module adds it for formula migration.
"""

from __future__ import annotations

import numpy as np
import pandas as pd


def _as_1d_array(x) -> np.ndarray:
    """Convert input to a 1-D numpy array."""
    if isinstance(x, pd.Series):
        arr = x.values
    else:
        arr = np.asarray(x)

    if arr.ndim == 0:
        arr = np.asarray([arr])
    return arr.reshape(-1)


def _expand_n(n, length: int) -> np.ndarray:
    """Expand scalar/series N into per-bar integer window sizes."""
    n_arr = _as_1d_array(n)
    if n_arr.size == 1:
        n_arr = np.full(length, n_arr.item())
    elif n_arr.size != length:
        raise ValueError("BACKSET: N must be a scalar or have the same length as X")

    # Tongdaxin period arguments are integer-like; floor non-integers and clamp to >= 1.
    n_arr = np.floor(n_arr.astype(float))
    n_arr[np.isnan(n_arr)] = 1.0
    n_arr[n_arr < 1.0] = 1.0
    return n_arr.astype(np.int64)


def BACKSET(x, n):
    """BACKSET(X, N): if X != 0, set current and previous N-1 bars to 1.

    This matches Tongdaxin's commonly used semantics for formula migration:
    when X at bar i is non-zero, output[j] is set to 1 for j in [i-N+1, i].

    Args:
        x: Condition/input sequence. Non-zero values are treated as True.
        n: Backset window. Scalar or sequence with same length as x.

    Returns:
        np.ndarray of int (0/1), same length as x.
    """
    x_arr = _as_1d_array(x)
    n_arr = _expand_n(n, len(x_arr))

    out = np.zeros(len(x_arr), dtype=np.int8)
    cond = np.nan_to_num(x_arr.astype(float), nan=0.0) != 0.0

    for i, flag in enumerate(cond):
        if not flag:
            continue
        start = max(0, i - n_arr[i] + 1)
        out[start : i + 1] = 1

    return out


__all__ = ["BACKSET"]
