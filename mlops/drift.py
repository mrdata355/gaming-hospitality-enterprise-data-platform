from __future__ import annotations

import numpy as np
import pandas as pd


def population_stability_index(reference: pd.Series, current: pd.Series, bins: int = 10) -> float:
    reference = reference.dropna().astype(float)
    current = current.dropna().astype(float)
    if reference.empty or current.empty:
        return float("inf")
    edges = np.unique(np.quantile(reference, np.linspace(0, 1, bins + 1)))
    if len(edges) < 3:
        return 0.0
    ref_hist, _ = np.histogram(reference, bins=edges)
    cur_hist, _ = np.histogram(current, bins=edges)
    ref_pct = np.clip(ref_hist / max(ref_hist.sum(), 1), 1e-6, None)
    cur_pct = np.clip(cur_hist / max(cur_hist.sum(), 1), 1e-6, None)
    return float(np.sum((cur_pct - ref_pct) * np.log(cur_pct / ref_pct)))


def classify_drift(psi: float) -> str:
    if psi < 0.10:
        return "PASS"
    if psi < 0.25:
        return "REVIEW"
    return "BLOCK"
