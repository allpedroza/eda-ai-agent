"""
Análise de concentração por chave de agrupamento.

Permite agregar múltiplas métricas de diferentes DataFrames por uma chave comum,
combinar os resultados e calcular um score de risco composto — tudo sem dependência
de domínio específico.

Uso típico:
    from eda.concentration import aggregate_by_key, merge_tables, compute_risk_score

    olt_contratos = aggregate_by_key(b1, "olt_id", {
        "n_clientes":  ("codigo_cliente",      "nunique"),
        "churn_pct":   ("tipo_cancelamento",   "notnull_pct"),
        "preco_medio": ("preco_banda_larga",   "mean"),
    })

    olt_incidentes = aggregate_by_key(b2, "olt_equipamento", {
        "n_incidentes":    ("motivo_abertura", "count"),
        "indisp_total_h":  ("duracao_h",       "sum"),
    }).rename(columns={"olt_equipamento": "olt_id"})

    painel = merge_tables([olt_contratos, olt_incidentes], on="olt_id")
    painel["score"] = compute_risk_score(painel, ["churn_pct", "n_incidentes", "indisp_total_h"])
"""

from __future__ import annotations

from typing import Callable, Dict, List, Optional, Tuple, Union

import pandas as pd
import numpy as np


# ─── Registro de funções de agregação nomeadas ───────────────────────────────

def _gt_pct(threshold: float) -> Callable[[pd.Series], float]:
    """% de valores acima de threshold."""
    return lambda s: (s > threshold).mean() * 100

def _pct_value(value) -> Callable[[pd.Series], float]:
    """% de valores iguais a value."""
    return lambda s: (s == value).mean() * 100

def _pct_notnull(s: pd.Series) -> float:
    return s.notna().mean() * 100

def _pct_true(s: pd.Series) -> float:
    return s.astype(bool).mean() * 100


AGG_REGISTRY: Dict[str, Callable] = {
    "nunique":     lambda s: s.nunique(),
    "count":       lambda s: s.count(),
    "sum":         lambda s: s.sum(),
    "mean":        lambda s: s.mean(),
    "median":      lambda s: s.median(),
    "max":         lambda s: s.max(),
    "min":         lambda s: s.min(),
    "std":         lambda s: s.std(),
    "p10":         lambda s: s.quantile(0.10),
    "p90":         lambda s: s.quantile(0.90),
    "p95":         lambda s: s.quantile(0.95),
    "notnull_pct": _pct_notnull,
    "pct_true":    _pct_true,
    # fábricas — use make_* para gerar funções parametrizadas
}

# Fábricas de funções de agregação parametrizadas
def make_gt_pct(threshold: float) -> Callable:
    """Retorna função que calcula % de valores > threshold."""
    return _gt_pct(threshold)

def make_pct_value(value) -> Callable:
    """Retorna função que calcula % onde coluna == value."""
    return _pct_value(value)


# ─── Agregação por chave ──────────────────────────────────────────────────────

AggSpec = Dict[str, Tuple[str, Union[str, Callable]]]


def aggregate_by_key(
    df: pd.DataFrame,
    group_col: str,
    metrics: AggSpec,
    min_group_size: int = 0,
    size_col: Optional[str] = None,
) -> pd.DataFrame:
    """
    Agrega um DataFrame por group_col usando as métricas especificadas.

    Parâmetros:
        df              — DataFrame de entrada
        group_col       — coluna de agrupamento
        metrics         — dict {nome_saída: (coluna_fonte, função_ou_nome)}
                          função pode ser string do AGG_REGISTRY ou callable
        min_group_size  — filtra grupos com menos de N registros (usa size_col ou count)
        size_col        — coluna usada para contar o tamanho do grupo (padrão: first metric col)

    Exemplo:
        aggregate_by_key(df, "regiao", {
            "n_clientes":  ("id",      "nunique"),
            "churn_pct":   ("churn",   "notnull_pct"),
            "ticket_medio": ("tickets", "mean"),
        }, min_group_size=30)
    """
    grouped = df.groupby(group_col, dropna=False)
    result: Dict[str, pd.Series] = {}

    for out_col, (src_col, func) in metrics.items():
        if src_col not in df.columns:
            continue
        agg_fn = AGG_REGISTRY.get(func) if isinstance(func, str) else func
        if agg_fn is None:
            raise ValueError(
                f"Função de agregação desconhecida: '{func}'. "
                f"Disponíveis: {sorted(AGG_REGISTRY)}"
            )
        result[out_col] = grouped[src_col].apply(agg_fn)

    out = pd.DataFrame(result).reset_index()

    if min_group_size > 0:
        # usa size_col ou a primeira coluna de resultado para filtrar
        count_col = size_col or list(metrics.keys())[0]
        if count_col in out.columns:
            out = out[out[count_col] >= min_group_size]

    return out


# ─── Merge de tabelas agregadas ───────────────────────────────────────────────

def merge_tables(
    tables: List[pd.DataFrame],
    on: str,
    how: str = "left",
) -> pd.DataFrame:
    """
    Junta uma lista de tabelas agregadas por uma chave comum.

    Exemplo:
        painel = merge_tables([olt_contratos, olt_incidentes, olt_tickets], on="olt_id")
    """
    if not tables:
        raise ValueError("Lista de tabelas vazia.")
    result = tables[0]
    for t in tables[1:]:
        result = result.merge(t, on=on, how=how)
    return result


# ─── Score de risco composto ──────────────────────────────────────────────────

def _normalize(series: pd.Series) -> pd.Series:
    mn, mx = series.min(), series.max()
    if mx == mn:
        return pd.Series(0.0, index=series.index)
    return (series - mn) / (mx - mn)


def compute_risk_score(
    df: pd.DataFrame,
    metric_cols: List[str],
    weights: Optional[Dict[str, float]] = None,
    higher_is_worse: Optional[Dict[str, bool]] = None,
) -> pd.Series:
    """
    Calcula um score de risco composto somando métricas normalizadas (0–1 cada).

    Parâmetros:
        metric_cols      — colunas a incluir no score
        weights          — {col: peso} (padrão: peso 1.0 para todas)
        higher_is_worse  — {col: bool} True = valor alto = maior risco (padrão: True para todas)

    Exemplo:
        df["score"] = compute_risk_score(
            df,
            ["churn_pct", "n_incidentes", "pct_inadimplentes"],
            weights={"churn_pct": 2.0, "n_incidentes": 1.0, "pct_inadimplentes": 1.5},
            higher_is_worse={"churn_pct": True, "n_incidentes": True, "pct_inadimplentes": True},
        )
    """
    score = pd.Series(0.0, index=df.index)

    for col in metric_cols:
        if col not in df.columns:
            continue
        w = (weights or {}).get(col, 1.0)
        worse = (higher_is_worse or {}).get(col, True)
        normed = _normalize(df[col].fillna(0))
        score += (normed if worse else 1 - normed) * w

    return score.round(3)


# ─── Utilitários de análise ───────────────────────────────────────────────────

def spearman_corr(df: pd.DataFrame, cols: List[str]) -> pd.DataFrame:
    """Correlação de Spearman entre as colunas selecionadas, descartando NaN."""
    return df[cols].dropna().corr(method="spearman").round(3)


def double_high(
    df: pd.DataFrame,
    col_a: str,
    col_b: str,
    quantile: float = 0.75,
) -> pd.DataFrame:
    """
    Retorna linhas acima do quantile em ambas as colunas simultaneamente.
    Útil para identificar grupos com duplo risco.

    Exemplo:
        criticos = double_high(painel, "churn_pct", "pct_inadimplentes", quantile=0.75)
    """
    qa = df[col_a].fillna(0).quantile(quantile)
    qb = df[col_b].fillna(0).quantile(quantile)
    return df[(df[col_a].fillna(0) >= qa) & (df[col_b].fillna(0) >= qb)]


def top_n(
    df: pd.DataFrame,
    sort_col: str,
    cols: Optional[List[str]] = None,
    n: int = 20,
    ascending: bool = False,
) -> pd.DataFrame:
    """Retorna as top N linhas ordenadas por sort_col."""
    return df.sort_values(sort_col, ascending=ascending).head(n)[cols or df.columns.tolist()]
