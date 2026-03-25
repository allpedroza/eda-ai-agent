"""Análise temporal genérica: distribuições no tempo, durações e padrões sazonais."""

from __future__ import annotations

from typing import List, Optional

import pandas as pd
import numpy as np


_WEEKDAY_MAP = {0: "Seg", 1: "Ter", 2: "Qua", 3: "Qui", 4: "Sex", 5: "Sáb", 6: "Dom"}
_WEEKDAY_ORDER = ["Seg", "Ter", "Qua", "Qui", "Sex", "Sáb", "Dom"]

_DURATION_DIVISORS = {"s": 1, "m": 60, "h": 3600, "d": 86400}


# ─── Parse ───────────────────────────────────────────────────────────────────

def parse_dates(df: pd.DataFrame, cols: List[str]) -> pd.DataFrame:
    """
    Converte colunas de data para datetime. Retorna cópia do DataFrame.
    Aceita formatos mistos (ISO, com vírgula, etc.) via errors='coerce'.
    """
    df = df.copy()
    for col in cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")
    return df


# ─── Distribuições temporais ─────────────────────────────────────────────────

def period_distribution(series: pd.Series, freq: str = "M") -> pd.DataFrame:
    """
    Conta eventos por período (M=mês, W=semana, D=dia, Q=trimestre, Y=ano).

    Retorna DataFrame com colunas 'n' e '%', indexado pelo período.
    """
    dt = pd.to_datetime(series, errors="coerce")
    counts = dt.dt.to_period(freq).value_counts().sort_index()
    total = counts.sum()
    return pd.DataFrame({
        "n":   counts,
        "%":   (counts / total * 100).round(1),
        "cum%": (counts / total * 100).cumsum().round(1),
    })


def hourly_distribution(series: pd.Series) -> pd.DataFrame:
    """Conta eventos por hora do dia (0–23)."""
    dt = pd.to_datetime(series, errors="coerce")
    counts = dt.dt.hour.value_counts().sort_index()
    return pd.DataFrame({"n": counts, "%": (counts / counts.sum() * 100).round(1)})


def weekday_distribution(series: pd.Series) -> pd.DataFrame:
    """Conta eventos por dia da semana, ordenado Seg→Dom."""
    dt = pd.to_datetime(series, errors="coerce")
    counts = dt.dt.dayofweek.map(_WEEKDAY_MAP).value_counts().reindex(_WEEKDAY_ORDER)
    return pd.DataFrame({"n": counts, "%": (counts / counts.sum() * 100).round(1)})


def monthly_heatmap(series: pd.Series) -> pd.DataFrame:
    """
    Retorna tabela ano × mês com contagem de eventos.
    Útil para identificar sazonalidade e anomalias pontuais.
    """
    dt = pd.to_datetime(series, errors="coerce").dropna()
    df = pd.DataFrame({"year": dt.dt.year, "month": dt.dt.month})
    return df.groupby(["year", "month"]).size().unstack(fill_value=0)


# ─── Duração ─────────────────────────────────────────────────────────────────

def compute_duration(
    start: pd.Series,
    end: pd.Series,
    unit: str = "h",
) -> pd.Series:
    """
    Calcula a duração entre duas séries de timestamp.

    Parâmetros:
        start, end — séries de timestamp (string ou datetime)
        unit       — unidade: 's' (segundos), 'm' (minutos), 'h' (horas), 'd' (dias)

    Retorna série numérica com a duração. Valores negativos indicam erro nos dados.
    """
    if unit not in _DURATION_DIVISORS:
        raise ValueError(f"Unidade inválida: '{unit}'. Use: {list(_DURATION_DIVISORS)}")
    s = pd.to_datetime(start, errors="coerce")
    e = pd.to_datetime(end,   errors="coerce")
    return (e - s).dt.total_seconds() / _DURATION_DIVISORS[unit]


def describe_duration(series: pd.Series, unit_label: str = "h") -> None:
    """Imprime estatísticas descritivas de uma série de duração."""
    valid = series.dropna()
    total = len(series)
    print(f"  N válidos: {len(valid):,} / {total:,} ({len(valid)/total*100:.1f}%)")
    print(valid.describe(percentiles=[.25, .5, .75, .9, .95, .99]).round(2).to_string())
    print(f"  Negativos (erro de dados): {(valid < 0).sum():,}")
    for threshold, label in [(1, "1h"), (24, "24h"), (72, "72h"), (168, "7d"), (720, "30d")]:
        n = (valid > threshold).sum()
        pct = n / len(valid) * 100
        print(f"  > {label}: {n:,} ({pct:.1f}%)")


# ─── Tendência ───────────────────────────────────────────────────────────────

def trend_by_period(
    df: pd.DataFrame,
    date_col: str,
    metric_col: str,
    agg: str = "count",
    freq: str = "M",
) -> pd.DataFrame:
    """
    Agrega uma métrica por período de tempo.

    Exemplo:
        trend_by_period(df, "data_abertura", "status_ticket", agg="count", freq="M")
        trend_by_period(df, "mes_referencia", "churn_flag", agg="mean", freq="M")
    """
    df = df.copy()
    df["_period"] = pd.to_datetime(df[date_col], errors="coerce").dt.to_period(freq)
    return df.groupby("_period")[metric_col].agg(agg).sort_index()


def category_trend(
    df: pd.DataFrame,
    date_col: str,
    category_col: str,
    freq: str = "M",
    normalize: bool = True,
) -> pd.DataFrame:
    """
    Mostra a evolução da composição de uma variável categórica ao longo do tempo.

    Exemplo:
        category_trend(df, "mes_referencia", "status_pagamento")
        → tabela período × categoria com % ou contagem
    """
    df = df.copy()
    df["_period"] = pd.to_datetime(df[date_col], errors="coerce").dt.to_period(freq)
    return (
        df.groupby("_period")[category_col]
        .value_counts(normalize=normalize)
        .mul(100 if normalize else 1)
        .round(1)
        .unstack(fill_value=0)
    )
