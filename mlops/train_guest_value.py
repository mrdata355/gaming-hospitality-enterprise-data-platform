from __future__ import annotations

from dataclasses import dataclass

import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import HistGradientBoostingRegressor
from sklearn.metrics import mean_absolute_error, r2_score
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

from mlops.features import FEATURES, build_guest_value_features


@dataclass(frozen=True)
class TrainingResult:
    model: Pipeline
    mae: float
    r2: float


def train(frame: pd.DataFrame, target: str = "future_90d_measured_value") -> TrainingResult:
    prepared = build_guest_value_features(frame)
    train_mask = prepared["snapshot_date"] < prepared["snapshot_date"].quantile(0.8)
    train_df = prepared.loc[train_mask]
    test_df = prepared.loc[~train_mask]
    if train_df.empty or test_df.empty:
        raise ValueError("Time-based split produced an empty train or test set")

    preprocessor = ColumnTransformer([("numeric", StandardScaler(), FEATURES)], remainder="drop")
    model = Pipeline([
        ("preprocess", preprocessor),
        ("model", HistGradientBoostingRegressor(max_iter=250, learning_rate=0.05, max_leaf_nodes=31, random_state=42)),
    ])
    model.fit(train_df[FEATURES], train_df[target])
    prediction = model.predict(test_df[FEATURES])
    return TrainingResult(
        model=model,
        mae=float(mean_absolute_error(test_df[target], prediction)),
        r2=float(r2_score(test_df[target], prediction)),
    )
