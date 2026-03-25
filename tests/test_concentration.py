"""Testes para eda.concentration."""
from __future__ import annotations

import pandas as pd
import numpy as np
import pytest

from eda.concentration import (
    aggregate_by_key,
    merge_tables,
    compute_risk_score,
    spearman_corr,
    double_high,
    top_n,
    make_gt_pct,
    make_pct_value,
    AGG_REGISTRY,
)


class TestAggregateByKey:
    def test_basic_count(self, df_concentration_left: pd.DataFrame):
        result = aggregate_by_key(
            df_concentration_left, "olt_id",
            {"n_clientes": ("cliente", "nunique")},
        )
        assert "olt_id" in result.columns
        assert "n_clientes" in result.columns
        assert len(result) == df_concentration_left["olt_id"].nunique()

    def test_multiple_metrics(self, df_concentration_left: pd.DataFrame):
        result = aggregate_by_key(
            df_concentration_left, "olt_id",
            {
                "n":       ("cliente", "count"),
                "churn":   ("churn",   "notnull_pct"),
                "media":   ("valor",   "mean"),
            },
        )
        assert {"n", "churn", "media"} <= set(result.columns)

    def test_min_group_size_filters(self, df_concentration_left: pd.DataFrame):
        result = aggregate_by_key(
            df_concentration_left, "olt_id",
            {"n": ("cliente", "count")},
            min_group_size=9999,
        )
        assert len(result) == 0

    def test_unknown_col_skipped(self, df_concentration_left: pd.DataFrame):
        result = aggregate_by_key(
            df_concentration_left, "olt_id",
            {"x": ("nonexistent_col", "count")},
        )
        assert "x" not in result.columns

    def test_callable_func(self, df_concentration_left: pd.DataFrame):
        fn = make_gt_pct(100)
        result = aggregate_by_key(
            df_concentration_left, "olt_id",
            {"pct_alto": ("valor", fn)},
        )
        assert "pct_alto" in result.columns

    def test_all_registry_functions(self, df_concentration_left: pd.DataFrame):
        for fname in AGG_REGISTRY:
            result = aggregate_by_key(
                df_concentration_left, "olt_id",
                {"metric": ("valor", fname)},
            )
            assert "metric" in result.columns

    def test_unknown_func_raises(self, df_concentration_left: pd.DataFrame):
        with pytest.raises(ValueError, match="desconhecida"):
            aggregate_by_key(
                df_concentration_left, "olt_id",
                {"x": ("valor", "nonexistent_func")},
            )


class TestMakeFunctions:
    def test_make_gt_pct(self):
        s = pd.Series([1, 5, 10, 15, 20])
        fn = make_gt_pct(10)
        assert fn(s) == pytest.approx(40.0)  # 15 e 20 > 10

    def test_make_pct_value(self):
        s = pd.Series(["a", "b", "a", "c"])
        fn = make_pct_value("a")
        assert fn(s) == pytest.approx(50.0)


class TestMergeTables:
    def test_basic_merge(self, df_concentration_left, df_concentration_right):
        left_agg = aggregate_by_key(
            df_concentration_left, "olt_id",
            {"n_clientes": ("cliente", "count")},
        )
        right_agg = aggregate_by_key(
            df_concentration_right, "olt_id",
            {"n_incidentes": ("n_incidentes", "sum")},
        )
        painel = merge_tables([left_agg, right_agg], on="olt_id")
        assert "n_clientes" in painel.columns
        assert "n_incidentes" in painel.columns

    def test_empty_list_raises(self):
        with pytest.raises(ValueError):
            merge_tables([], on="olt_id")

    def test_single_table(self, df_concentration_left):
        agg = aggregate_by_key(
            df_concentration_left, "olt_id",
            {"n": ("cliente", "count")},
        )
        result = merge_tables([agg], on="olt_id")
        pd.testing.assert_frame_equal(result, agg)


class TestComputeRiskScore:
    def test_returns_series(self, df_concentration_left, df_concentration_right):
        agg = merge_tables([
            aggregate_by_key(df_concentration_left, "olt_id", {"n": ("cliente", "count")}),
            aggregate_by_key(df_concentration_right, "olt_id", {"inc": ("n_incidentes", "sum")}),
        ], on="olt_id", how="outer")
        score = compute_risk_score(agg, ["n", "inc"])
        assert isinstance(score, pd.Series)
        assert (score >= 0).all()

    def test_range_with_weights(self, df_concentration_left):
        agg = aggregate_by_key(
            df_concentration_left, "olt_id",
            {"n": ("cliente", "count"), "v": ("valor", "mean")},
        )
        score = compute_risk_score(agg, ["n", "v"], weights={"n": 2.0, "v": 1.0})
        # Max possível: 2.0 + 1.0 = 3.0
        assert (score <= 3.0 + 1e-6).all()

    def test_higher_is_worse_false(self, df_concentration_left):
        agg = aggregate_by_key(
            df_concentration_left, "olt_id",
            {"n": ("cliente", "count")},
        )
        score_hiw = compute_risk_score(agg, ["n"], higher_is_worse={"n": True})
        score_liw = compute_risk_score(agg, ["n"], higher_is_worse={"n": False})
        # Os scores devem ser invertidos entre si
        assert abs(score_hiw.sum() + score_liw.sum() - len(agg)) < 0.01


class TestSpearmanCorr:
    def test_returns_dataframe(self, df_concentration_left):
        agg = aggregate_by_key(
            df_concentration_left, "olt_id",
            {"n": ("cliente", "count"), "v": ("valor", "mean")},
        )
        result = spearman_corr(agg, ["n", "v"])
        assert isinstance(result, pd.DataFrame)
        assert result.shape == (2, 2)

    def test_diagonal_is_one(self, df_concentration_left):
        agg = aggregate_by_key(
            df_concentration_left, "olt_id",
            {"n": ("cliente", "count"), "v": ("valor", "mean")},
        )
        result = spearman_corr(agg, ["n", "v"])
        assert abs(result.loc["n", "n"] - 1.0) < 1e-9


class TestDoubleHigh:
    def test_returns_subset(self, df_concentration_left, df_concentration_right):
        agg = merge_tables([
            aggregate_by_key(df_concentration_left, "olt_id", {"n": ("cliente", "count")}),
            aggregate_by_key(df_concentration_right, "olt_id", {"inc": ("n_incidentes", "sum")}),
        ], on="olt_id", how="outer")
        result = double_high(agg, "n", "inc", quantile=0.5)
        assert isinstance(result, pd.DataFrame)
        assert len(result) <= len(agg)


class TestTopN:
    def test_returns_n_rows(self, df_concentration_left):
        agg = aggregate_by_key(
            df_concentration_left, "olt_id",
            {"n": ("cliente", "count")},
        )
        result = top_n(agg, "n", n=2)
        assert len(result) == min(2, len(agg))

    def test_descending_order(self, df_concentration_left):
        agg = aggregate_by_key(
            df_concentration_left, "olt_id",
            {"n": ("cliente", "count")},
        )
        result = top_n(agg, "n", n=len(agg))
        assert list(result["n"]) == sorted(result["n"], reverse=True)

    def test_col_selection(self, df_concentration_left):
        agg = aggregate_by_key(
            df_concentration_left, "olt_id",
            {"n": ("cliente", "count"), "v": ("valor", "mean")},
        )
        result = top_n(agg, "n", cols=["olt_id", "n"], n=2)
        assert list(result.columns) == ["olt_id", "n"]
