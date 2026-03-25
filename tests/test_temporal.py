"""Testes para eda.temporal."""
from __future__ import annotations

import pandas as pd
import numpy as np
import pytest

from eda.temporal import (
    parse_dates,
    period_distribution,
    hourly_distribution,
    weekday_distribution,
    monthly_heatmap,
    compute_duration,
    trend_by_period,
    category_trend,
)


class TestParseDates:
    def test_converts_string_dates(self, df_temporal: pd.DataFrame):
        df = df_temporal.copy()
        df["data_abertura"] = df["data_abertura"].astype(str)
        result = parse_dates(df, ["data_abertura"])
        assert pd.api.types.is_datetime64_any_dtype(result["data_abertura"])

    def test_coerces_invalid(self):
        df = pd.DataFrame({"dt": ["2023-01-01", "not-a-date", None]})
        result = parse_dates(df, ["dt"])
        assert result["dt"].isna().sum() == 2

    def test_ignores_missing_cols(self, df_temporal: pd.DataFrame):
        result = parse_dates(df_temporal, ["nonexistent_col"])
        assert list(result.columns) == list(df_temporal.columns)


class TestPeriodDistribution:
    def test_returns_dataframe(self, df_temporal: pd.DataFrame):
        result = period_distribution(df_temporal["data_abertura"], freq="M")
        assert isinstance(result, pd.DataFrame)

    def test_columns(self, df_temporal: pd.DataFrame):
        result = period_distribution(df_temporal["data_abertura"], freq="M")
        assert set(result.columns) >= {"n", "%", "cum%"}

    def test_pct_sums_100(self, df_temporal: pd.DataFrame):
        result = period_distribution(df_temporal["data_abertura"], freq="M")
        assert abs(result["%"].sum() - 100) < 0.5

    def test_daily_freq(self, df_temporal: pd.DataFrame):
        result = period_distribution(df_temporal["data_abertura"], freq="D")
        assert len(result) == 120  # 120 datas únicas


class TestHourlyDistribution:
    def test_returns_dataframe(self, df_temporal: pd.DataFrame):
        result = hourly_distribution(df_temporal["data_abertura"])
        assert isinstance(result, pd.DataFrame)

    def test_hours_in_range(self, df_temporal: pd.DataFrame):
        result = hourly_distribution(df_temporal["data_abertura"])
        assert all(0 <= h <= 23 for h in result.index)


class TestWeekdayDistribution:
    def test_returns_dataframe(self, df_temporal: pd.DataFrame):
        result = weekday_distribution(df_temporal["data_abertura"])
        assert isinstance(result, pd.DataFrame)

    def test_correct_order(self, df_temporal: pd.DataFrame):
        result = weekday_distribution(df_temporal["data_abertura"])
        expected = ["Seg", "Ter", "Qua", "Qui", "Sex", "Sáb", "Dom"]
        assert list(result.index) == expected


class TestMonthlyHeatmap:
    def test_returns_dataframe(self, df_temporal: pd.DataFrame):
        result = monthly_heatmap(df_temporal["data_abertura"])
        assert isinstance(result, pd.DataFrame)

    def test_columns_are_months(self, df_temporal: pd.DataFrame):
        result = monthly_heatmap(df_temporal["data_abertura"])
        assert all(1 <= c <= 12 for c in result.columns)


class TestComputeDuration:
    def test_hours(self, df_temporal: pd.DataFrame):
        dur = compute_duration(df_temporal["data_abertura"], df_temporal["data_fechamento"], unit="h")
        assert (dur >= 0).all()

    def test_days(self, df_temporal: pd.DataFrame):
        dur_h = compute_duration(df_temporal["data_abertura"], df_temporal["data_fechamento"], unit="h")
        dur_d = compute_duration(df_temporal["data_abertura"], df_temporal["data_fechamento"], unit="d")
        pd.testing.assert_series_equal(dur_h / 24, dur_d, check_names=False, atol=1e-6)

    def test_invalid_unit_raises(self, df_temporal: pd.DataFrame):
        with pytest.raises(ValueError, match="Unidade inválida"):
            compute_duration(df_temporal["data_abertura"], df_temporal["data_fechamento"], unit="x")


class TestTrendByPeriod:
    def test_returns_series(self, df_temporal: pd.DataFrame):
        result = trend_by_period(df_temporal, "data_abertura", "categoria", agg="count", freq="M")
        assert isinstance(result, pd.Series)

    def test_sorted_index(self, df_temporal: pd.DataFrame):
        result = trend_by_period(df_temporal, "data_abertura", "categoria", agg="count", freq="M")
        assert list(result.index) == sorted(result.index)


class TestCategoryTrend:
    def test_returns_dataframe(self, df_temporal: pd.DataFrame):
        result = category_trend(df_temporal, "data_abertura", "categoria", freq="M")
        assert isinstance(result, pd.DataFrame)

    def test_columns_are_categories(self, df_temporal: pd.DataFrame):
        result = category_trend(df_temporal, "data_abertura", "categoria")
        assert set(result.columns) == {"A", "B", "C"}
