from __future__ import annotations

import warnings
from dataclasses import dataclass
from typing import Any

import numpy as np
import pandas as pd
from sklearn.exceptions import ConvergenceWarning
from sklearn.gaussian_process import GaussianProcessRegressor
from sklearn.gaussian_process.kernels import ConstantKernel, Matern, RBF, RationalQuadratic, WhiteKernel
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.model_selection import KFold
from sklearn.preprocessing import StandardScaler


def _build_kernel(kernel_name: str, n_features: int):
    feature_scale = np.ones(n_features, dtype=float)
    noise = WhiteKernel(noise_level=1e-3, noise_level_bounds=(1e-8, 1e1))

    if kernel_name == "matern32":
        base = ConstantKernel(1.0, (1e-3, 1e3)) * Matern(
            length_scale=feature_scale,
            length_scale_bounds=(1e-2, 1e2),
            nu=1.5,
        )
        return base + noise

    if kernel_name == "matern52":
        base = ConstantKernel(1.0, (1e-3, 1e3)) * Matern(
            length_scale=feature_scale,
            length_scale_bounds=(1e-2, 1e2),
            nu=2.5,
        )
        return base + noise

    if kernel_name == "rbf":
        base = ConstantKernel(1.0, (1e-3, 1e3)) * RBF(
            length_scale=feature_scale,
            length_scale_bounds=(1e-2, 1e2),
        )
        return base + noise

    if kernel_name == "rq":
        base = ConstantKernel(1.0, (1e-3, 1e3)) * RationalQuadratic(
            length_scale=1.0,
            alpha=1.0,
            length_scale_bounds=(1e-2, 1e2),
            alpha_bounds=(1e-3, 1e3),
        )
        return base + noise

    raise ValueError(f"Unsupported kernel name: {kernel_name}")


@dataclass(slots=True)
class GPRSurrogate:
    kernel_name: str
    alpha: float
    normalize_y: bool
    n_restarts_optimizer: int
    random_state: int
    scaler: StandardScaler | None = None
    regressor: GaussianProcessRegressor | None = None

    def fit(self, X: np.ndarray, y: np.ndarray) -> "GPRSurrogate":
        self.scaler = StandardScaler()
        scaled_X = self.scaler.fit_transform(X)
        kernel = _build_kernel(self.kernel_name, X.shape[1])
        self.regressor = GaussianProcessRegressor(
            kernel=kernel,
            alpha=self.alpha,
            normalize_y=self.normalize_y,
            n_restarts_optimizer=self.n_restarts_optimizer,
            random_state=self.random_state,
        )
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", category=ConvergenceWarning)
            self.regressor.fit(scaled_X, y)
        return self

    def predict(self, X: np.ndarray, return_std: bool = False):
        if self.scaler is None or self.regressor is None:
            raise RuntimeError("The GPR surrogate must be fitted before prediction.")
        scaled_X = self.scaler.transform(X)
        return self.regressor.predict(scaled_X, return_std=return_std)

    def to_dict(self) -> dict[str, Any]:
        if self.regressor is None:
            raise RuntimeError("Cannot serialize an unfitted GPR surrogate.")
        return {
            "kernel_name": self.kernel_name,
            "optimized_kernel": str(self.regressor.kernel_),
            "log_marginal_likelihood": float(self.regressor.log_marginal_likelihood_value_),
        }


def _make_splitter(n_samples: int, n_splits: int, random_state: int) -> KFold:
    actual_splits = min(n_splits, n_samples)
    if actual_splits < 2:
        raise ValueError("At least two samples are required for cross-validation.")
    return KFold(n_splits=actual_splits, shuffle=True, random_state=random_state)


def _new_surrogate(kernel_name: str, gpr_config: dict[str, Any]) -> GPRSurrogate:
    return GPRSurrogate(
        kernel_name=kernel_name,
        alpha=float(gpr_config["alpha"]),
        normalize_y=bool(gpr_config["normalize_y"]),
        n_restarts_optimizer=int(gpr_config["n_restarts_optimizer"]),
        random_state=int(gpr_config["random_state"]),
    )


def evaluate_candidate_kernels(
    X: np.ndarray,
    y: np.ndarray,
    gpr_config: dict[str, Any],
) -> pd.DataFrame:
    splitter = _make_splitter(
        n_samples=len(X),
        n_splits=int(gpr_config["cv_folds"]),
        random_state=int(gpr_config["random_state"]),
    )
    candidate_kernels = list(gpr_config["candidate_kernels"])
    rows: list[dict[str, float | str]] = []

    for kernel_name in candidate_kernels:
        fold_metrics: list[dict[str, float]] = []
        for train_idx, test_idx in splitter.split(X, y):
            model = _new_surrogate(kernel_name, gpr_config).fit(X[train_idx], y[train_idx])
            predicted = model.predict(X[test_idx], return_std=False)

            rmse = float(np.sqrt(mean_squared_error(y[test_idx], predicted)))
            mae = float(mean_absolute_error(y[test_idx], predicted))
            r2 = float(r2_score(y[test_idx], predicted)) if len(test_idx) >= 2 else float("nan")
            fold_metrics.append({"rmse": rmse, "mae": mae, "r2": r2})

        metric_frame = pd.DataFrame(fold_metrics)
        rows.append(
            {
                "kernel_name": kernel_name,
                "rmse_mean": float(metric_frame["rmse"].mean()),
                "rmse_std": float(metric_frame["rmse"].std(ddof=0)),
                "mae_mean": float(metric_frame["mae"].mean()),
                "mae_std": float(metric_frame["mae"].std(ddof=0)),
                "r2_mean": float(metric_frame["r2"].mean(skipna=True)),
                "r2_std": float(metric_frame["r2"].std(ddof=0, skipna=True)),
            }
        )

    result = pd.DataFrame(rows).sort_values(
        by=["rmse_mean", "mae_mean", "r2_mean"],
        ascending=[True, True, False],
    )
    return result.reset_index(drop=True)


def fit_best_model(
    X: np.ndarray,
    y: np.ndarray,
    gpr_config: dict[str, Any],
) -> tuple[GPRSurrogate, pd.DataFrame, pd.DataFrame]:
    cv_summary = evaluate_candidate_kernels(X, y, gpr_config)
    best_kernel = str(cv_summary.iloc[0]["kernel_name"])

    splitter = _make_splitter(
        n_samples=len(X),
        n_splits=int(gpr_config["cv_folds"]),
        random_state=int(gpr_config["random_state"]),
    )
    oof_mean = np.zeros(len(X), dtype=float)
    oof_std = np.zeros(len(X), dtype=float)
    for train_idx, test_idx in splitter.split(X, y):
        fold_model = _new_surrogate(best_kernel, gpr_config).fit(X[train_idx], y[train_idx])
        pred_mean, pred_std = fold_model.predict(X[test_idx], return_std=True)
        oof_mean[test_idx] = pred_mean
        oof_std[test_idx] = pred_std

    oof_table = pd.DataFrame(
        {
            "oof_prediction": oof_mean,
            "oof_std": oof_std,
            "oof_residual": y - oof_mean,
        }
    )

    model = _new_surrogate(best_kernel, gpr_config).fit(X, y)
    return model, cv_summary, oof_table
