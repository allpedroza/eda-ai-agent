"""Testes para eda.profiler."""
from __future__ import annotations

from pathlib import Path

import pandas as pd
import numpy as np
import pytest

from eda.profiler import (
    null_coverage,
    describe_numeric,
    value_counts_pct,
    bin_distribution,
    detect_anomalies,
    plot_dataframe,
    profile_dataframe,
    crosstab_rate,
)


class TestNullCoverage:
    def test_returns_dataframe(self, df_mixed: pd.DataFrame):
        result = null_coverage(df_mixed)
        assert isinstance(result, pd.DataFrame)

    def test_expected_columns(self, df_mixed: pd.DataFrame):
        result = null_coverage(df_mixed)
        assert set(result.columns) >= {"n_null", "pct_null", "n_unique"}

    def test_sorted_by_pct_null_desc(self, df_mixed: pd.DataFrame):
        result = null_coverage(df_mixed)
        assert list(result["pct_null"]) == sorted(result["pct_null"], reverse=True)

    def test_detects_nulls(self, df_mixed: pd.DataFrame):
        result = null_coverage(df_mixed)
        assert result.loc["valor", "n_null"] > 0


class TestDescribeNumeric:
    def test_returns_series(self, df_numeric: pd.DataFrame):
        result = describe_numeric(df_numeric["valor"].dropna())
        assert isinstance(result, pd.Series)

    def test_includes_iqr_and_fences(self, df_numeric: pd.DataFrame):
        result = describe_numeric(df_numeric["valor"].dropna())
        assert "iqr" in result.index
        assert "fence_low" in result.index
        assert "fence_high" in result.index

    def test_outlier_counts_positive(self, df_numeric: pd.DataFrame):
        result = describe_numeric(df_numeric["valor"].dropna())
        assert result["n_outlier_high"] >= 1  # outlier 9999 deve ser detectado


class TestValueCountsPct:
    def test_returns_dataframe(self, df_categorical: pd.DataFrame):
        result = value_counts_pct(df_categorical["regiao"])
        assert isinstance(result, pd.DataFrame)

    def test_pct_sums_to_100(self, df_categorical: pd.DataFrame):
        result = value_counts_pct(df_categorical["status"])
        assert abs(result["%"].sum() - 100) < 1.0


class TestBinDistribution:
    def test_returns_dataframe(self, df_numeric: pd.DataFrame):
        result = bin_distribution(df_numeric["score"])
        assert isinstance(result, pd.DataFrame)

    def test_n_bins(self, df_numeric: pd.DataFrame):
        result = bin_distribution(df_numeric["score"], bins=5)
        assert len(result) == 5


class TestDetectAnomalies:
    def test_detects_constant(self, df_categorical: pd.DataFrame):
        anomalies = detect_anomalies(df_categorical)
        assert any("CONSTANTE" in a for a in anomalies)

    def test_detects_negative(self, df_mixed: pd.DataFrame):
        anomalies = detect_anomalies(df_mixed)
        assert any("NEGATIVO" in a for a in anomalies)

    def test_detects_extreme_outlier(self, df_numeric: pd.DataFrame):
        anomalies = detect_anomalies(df_numeric)
        assert any("OUTLIER EXTREMO" in a for a in anomalies)

    def test_detects_high_cardinality(self, df_mixed: pd.DataFrame):
        anomalies = detect_anomalies(df_mixed)
        assert any("ALTA CARDINALIDADE" in a for a in anomalies)

    def test_no_anomalies_on_clean_data(self):
        df = pd.DataFrame({"x": [1, 2, 3, 4, 5], "cat": ["a", "b", "c", "d", "e"]})
        anomalies = detect_anomalies(df)
        assert not any("CONSTANTE" in a for a in anomalies)


class TestPlotDataframe:
    def test_saves_files(self, df_numeric: pd.DataFrame, tmp_path: Path):
        saved = plot_dataframe(df_numeric, name="test", output_dir=tmp_path)
        assert len(saved) > 0
        assert all(p.exists() for p in saved)

    def test_numeric_generates_hist_box(self, df_numeric: pd.DataFrame, tmp_path: Path):
        saved = plot_dataframe(df_numeric, name="num", output_dir=tmp_path)
        names = [p.name for p in saved]
        assert any("hist_box" in n for n in names)

    def test_categorical_generates_barplot(self, df_categorical: pd.DataFrame, tmp_path: Path):
        saved = plot_dataframe(df_categorical, name="cat", output_dir=tmp_path)
        names = [p.name for p in saved]
        assert any("barplot" in n for n in names)

    def test_skips_high_cardinality_cat(self, tmp_path: Path):
        # Cria DataFrame com coluna categórica de >200 únicos (deve ser pulada)
        df = pd.DataFrame({
            "num":    range(300),
            "cat_hi": [f"VAL_{i}" for i in range(300)],  # 300 únicos > limiar 200
        })
        saved = plot_dataframe(df, name="hi_card", output_dir=tmp_path)
        names = [p.name for p in saved]
        assert not any("cat_hi__barplot" in n for n in names)
        assert any("num__hist_box" in n for n in names)


class TestProfileDataframe:
    def test_returns_dict(self, df_mixed: pd.DataFrame):
        result = profile_dataframe(df_mixed, verbose=False)
        assert isinstance(result, dict)

    def test_required_keys(self, df_mixed: pd.DataFrame):
        result = profile_dataframe(df_mixed, verbose=False)
        assert {"name", "shape", "null_coverage", "numeric", "categorical", "anomalies"} <= result.keys()

    def test_shape_correct(self, df_mixed: pd.DataFrame):
        result = profile_dataframe(df_mixed, name="test", verbose=False)
        assert result["shape"]["rows"] == len(df_mixed)
        assert result["shape"]["cols"] == df_mixed.shape[1]

    def test_numeric_contains_stats(self, df_numeric: pd.DataFrame):
        result = profile_dataframe(df_numeric, verbose=False)
        assert "valor" in result["numeric"]
        assert "stats" in result["numeric"]["valor"]

    def test_categorical_contains_top_values(self, df_categorical: pd.DataFrame):
        result = profile_dataframe(df_categorical, verbose=False)
        assert "regiao" in result["categorical"]
        assert isinstance(result["categorical"]["regiao"]["top_values"], pd.DataFrame)

    def test_target_col_flagged(self, df_numeric: pd.DataFrame):
        result = profile_dataframe(df_numeric, target_col="score", verbose=False)
        assert result["numeric"]["score"]["is_target"] is True
        assert result["numeric"]["valor"]["is_target"] is False

    def test_anomalies_list(self, df_categorical: pd.DataFrame):
        result = profile_dataframe(df_categorical, verbose=False)
        assert isinstance(result["anomalies"], list)


class TestCrosstabRate:
    def test_returns_dataframe(self, df_concentration_left: pd.DataFrame):
        result = crosstab_rate(df_concentration_left, "olt_id", "churn")
        assert isinstance(result, pd.DataFrame)

    def test_columns_present(self, df_concentration_left: pd.DataFrame):
        result = crosstab_rate(df_concentration_left, "olt_id", "churn")
        assert "total" in result.columns
        assert any("taxa" in c for c in result.columns)

    def test_sorted_desc(self, df_concentration_left: pd.DataFrame):
        result = crosstab_rate(df_concentration_left, "olt_id", "churn")
        rate_col = [c for c in result.columns if "taxa" in c][0]
        assert list(result[rate_col]) == sorted(result[rate_col], reverse=True)

    def test_total_uses_all_rows(self, df_concentration_left: pd.DataFrame):
        result = crosstab_rate(df_concentration_left, "olt_id", "churn", min_count=0)
        assert result["total"].sum() == len(df_concentration_left)
