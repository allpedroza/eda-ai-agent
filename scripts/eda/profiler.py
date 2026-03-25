"""Análise univariada genérica: nulos, distribuições, outliers e anomalias."""

from __future__ import annotations

from typing import List, Optional

import pandas as pd
import numpy as np


# ─── Cobertura de nulos ──────────────────────────────────────────────────────

def null_coverage(df: pd.DataFrame) -> pd.DataFrame:
    """Retorna DataFrame com cobertura de nulos por coluna, ordenado do pior para o melhor."""
    return pd.DataFrame({
        "dtype":     df.dtypes.astype(str),
        "n_null":    df.isnull().sum(),
        "pct_null":  (df.isnull().mean() * 100).round(1),
        "n_unique":  df.nunique(dropna=False),
        "pct_unique": (df.nunique(dropna=False) / max(len(df), 1) * 100).round(1),
    }).sort_values("pct_null", ascending=False)


# ─── Estatísticas descritivas ────────────────────────────────────────────────

def describe_numeric(series: pd.Series, percentiles: List[float] | None = None) -> pd.Series:
    """Estatísticas descritivas ampliadas para séries numéricas, com limites de outlier IQR."""
    pcts = percentiles or [0.01, 0.05, 0.25, 0.5, 0.75, 0.95, 0.99]
    desc = series.describe(percentiles=pcts)
    q1, q3 = series.quantile(0.25), series.quantile(0.75)
    iqr = q3 - q1
    desc["iqr"]            = iqr
    desc["fence_low"]      = q1 - 1.5 * iqr
    desc["fence_high"]     = q3 + 1.5 * iqr
    desc["n_outlier_low"]  = int((series < desc["fence_low"]).sum())
    desc["n_outlier_high"] = int((series > desc["fence_high"]).sum())
    return desc.round(4)


def value_counts_pct(series: pd.Series, top_n: int = 15) -> pd.DataFrame:
    """Value counts com percentual para colunas categóricas."""
    counts = series.value_counts(dropna=False).head(top_n)
    return pd.DataFrame({"n": counts, "%": (counts / len(series) * 100).round(1)})


def bin_distribution(series: pd.Series, bins: int = 10) -> pd.DataFrame:
    """Distribui uma série numérica em bins automáticos."""
    cut = pd.cut(series.dropna(), bins=bins)
    counts = cut.value_counts().sort_index()
    return pd.DataFrame({"n": counts, "%": (counts / counts.sum() * 100).round(1)})


# ─── Detecção de anomalias ───────────────────────────────────────────────────

def detect_anomalies(df: pd.DataFrame) -> List[str]:
    """
    Detecta anomalias estruturais em um DataFrame e retorna lista de mensagens.

    Verifica: colunas constantes, alta taxa de nulos, valores negativos inesperados,
    outliers extremos, alta cardinalidade e possíveis identificadores numéricos.
    """
    anomalies: List[str] = []

    for col in df.columns:
        s = df[col]
        n_unique  = s.nunique(dropna=True)
        pct_null  = s.isnull().mean() * 100
        n         = len(s)

        if n_unique <= 1:
            val = s.dropna().iloc[0] if s.notna().any() else "N/A"
            anomalies.append(f"[CONSTANTE] '{col}': único valor = '{val}'")

        if pct_null > 80:
            anomalies.append(f"[NULOS CRÍTICOS] '{col}': {pct_null:.1f}% nulos")
        elif pct_null > 30:
            anomalies.append(f"[NULOS ALTOS] '{col}': {pct_null:.1f}% nulos")

        if pd.api.types.is_numeric_dtype(s):
            non_null = s.dropna()
            if len(non_null) == 0:
                continue
            if non_null.min() < 0:
                n_neg = (non_null < 0).sum()
                anomalies.append(f"[NEGATIVO] '{col}': {n_neg} valores negativos (min={non_null.min():.2f})")
            p99 = non_null.quantile(0.99)
            if p99 > 0 and non_null.max() > p99 * 10:
                anomalies.append(
                    f"[OUTLIER EXTREMO] '{col}': max={non_null.max():.2f}, p99={p99:.2f} "
                    f"(razão {non_null.max()/p99:.0f}×)"
                )

        if pd.api.types.is_object_dtype(s) or isinstance(s.dtype, pd.CategoricalDtype):
            unique_ratio = n_unique / max(n, 1)
            if unique_ratio > 0.9 and n_unique > 100:
                anomalies.append(
                    f"[ALTA CARDINALIDADE] '{col}': {n_unique} únicos ({unique_ratio*100:.0f}% do total)"
                )

    return anomalies


# ─── Perfil completo ─────────────────────────────────────────────────────────

def profile_dataframe(
    df: pd.DataFrame,
    name: str = "Dataset",
    target_col: Optional[str] = None,
    top_n: int = 10,
) -> None:
    """
    Imprime um perfil exploratório completo de um DataFrame.

    Parâmetros:
        df         — DataFrame a analisar
        name       — nome exibido no cabeçalho
        target_col — coluna alvo (exibida com destaque)
        top_n      — nº de categorias a exibir por coluna
    """
    bar = "=" * 70

    print(f"\n{bar}")
    print(f"  PERFIL: {name}  ({df.shape[0]:,} linhas × {df.shape[1]} colunas)")
    print(bar)

    # Nulos
    coverage = null_coverage(df)
    with_nulls = coverage[coverage["pct_null"] > 0]
    print("\n--- Cobertura de nulos ---")
    if len(with_nulls):
        print(with_nulls.to_string())
    else:
        print("  Sem nulos.")

    # Numéricas
    num_cols = df.select_dtypes(include="number").columns.tolist()
    if num_cols:
        print("\n--- Colunas numéricas ---")
        for col in num_cols:
            label = f" ← TARGET" if col == target_col else ""
            print(f"\n  {col}{label}")
            print(describe_numeric(df[col]).to_string())

    # Categóricas
    cat_cols = df.select_dtypes(exclude="number").columns.tolist()
    if cat_cols:
        print("\n--- Colunas categóricas / object ---")
        for col in cat_cols:
            label = f" ← TARGET" if col == target_col else ""
            n_uniq = df[col].nunique()
            print(f"\n  {col}  ({n_uniq} únicos){label}")
            print(value_counts_pct(df[col], top_n=top_n).to_string())

    # Anomalias
    anomalies = detect_anomalies(df)
    print("\n--- Anomalias detectadas ---")
    if anomalies:
        for a in anomalies:
            print(f"  {a}")
    else:
        print("  Nenhuma anomalia detectada.")


# ─── Cross-tab genérico ──────────────────────────────────────────────────────

def crosstab_rate(
    df: pd.DataFrame,
    group_col: str,
    target_col: str,
    target_is_notnull: bool = True,
    target_value=None,
    min_count: int = 10,
) -> pd.DataFrame:
    """
    Calcula a taxa do target por grupo.

    Modos:
        target_is_notnull=True  → taxa de registros não-nulos em target_col
        target_value=X          → taxa de registros onde target_col == X

    Exemplo:
        crosstab_rate(df, "canal_venda", "tipo_cancelamento", target_is_notnull=True)
        crosstab_rate(df, "estado", "status_pagamento", target_value="não pago/inadimplente")
    """
    def _rate(x: pd.Series) -> float:
        if target_is_notnull:
            return x.notna().mean() * 100
        return (x == target_value).mean() * 100

    result = (
        df.groupby(group_col, dropna=False)
        .agg(
            total=(target_col, "count"),
            positivos=(target_col, _rate),
        )
        .rename(columns={"positivos": f"taxa_{target_col}_%"})
    )
    return (
        result[result["total"] >= min_count]
        .sort_values(f"taxa_{target_col}_%", ascending=False)
        .round(1)
    )
