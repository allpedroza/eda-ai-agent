"""Análise de concentração por OLT — usa módulos eda.*.

Cruza todos os blocos via chave de cliente e OLT para identificar
concentrações de churn, incidentes, tickets e inadimplência.

Uso:
    uv run python scripts/eda_por_olt.py
"""

from __future__ import annotations

import sys
import warnings
from pathlib import Path

import pandas as pd

warnings.filterwarnings("ignore")
sys.path.insert(0, str(Path(__file__).parent))

from eda.loader import load_dataset
from eda.temporal import compute_duration
from eda.concentration import (
    aggregate_by_key, merge_tables, compute_risk_score,
    top_n, double_high, spearman_corr, make_gt_pct, make_pct_value,
)


def sep(title: str) -> None:
    print(f"\n{'='*70}\n  {title}\n{'='*70}")


def subsep(title: str) -> None:
    print(f"\n--- {title} ---")


# ─── Carregamento ─────────────────────────────────────────────────────────────
sep("Carregando datasets...")

b1 = load_dataset(
    "inputs/Bloco_1*.csv",
    usecols=["codigo_cliente", "olt_id", "tipo_cancelamento",
             "preco_banda_larga", "velocidade_internet", "estado", "canal_venda"],
)
print(f"  B1: {len(b1):,} linhas")

b2 = load_dataset("inputs/Bloco_2*.csv")
b2["duracao_h"] = compute_duration(b2["inicio_evento"], b2["fim_evento"], unit="h")
print(f"  B2: {len(b2):,} linhas")

b4 = load_dataset(
    "inputs/Bloco_4*.csv",
    usecols=["id_cliente", "status_ticket", "prazo_resolucao", "motivo",
             "flag_rechamada_voz_24h", "qtd_chamados_financeiros_90d"],
)
print(f"  B4: {len(b4):,} linhas")

b5 = load_dataset(
    "inputs/Bloco_5*.csv",
    usecols=["id_cliente", "status_pagamento", "valor_fatura", "dias_atraso"],
)
print(f"  B5: {len(b5):,} linhas")


# ─── PARTE 1: Métricas de contrato por OLT (B1) ──────────────────────────────
sep("PARTE 1 — Métricas de contrato/churn por OLT")

olt_b1 = aggregate_by_key(
    b1.dropna(subset=["olt_id"]),
    group_col="olt_id",
    metrics={
        "n_clientes":   ("codigo_cliente",      "nunique"),
        "churn_pct":    ("tipo_cancelamento",   "notnull_pct"),
        "cancelados":   ("tipo_cancelamento",   "count"),  # workaround: sum de notnull via count após filter
        "preco_mediano":("preco_banda_larga",   "median"),
        "vel_mediana":  ("velocidade_internet", "median"),
    },
)
# corrige: cancelados deve ser sum de notnull
olt_b1["cancelados"] = (
    b1.dropna(subset=["olt_id"])
    .groupby("olt_id")["tipo_cancelamento"].apply(lambda x: x.notna().sum())
    .values
)

subsep("Top 20 OLTs por churn% (mín. 50 clientes)")
print(
    top_n(olt_b1[olt_b1["n_clientes"] >= 50], "churn_pct",
          cols=["olt_id","n_clientes","cancelados","churn_pct","preco_mediano","vel_mediana"])
    .to_string(index=False)
)

subsep("Distribuição de churn% por OLT (percentis)")
print(olt_b1["churn_pct"].describe(percentiles=[.1,.25,.5,.75,.9,.95]).round(1).to_string())


# ─── PARTE 2: Métricas de incidentes por OLT (B2) ────────────────────────────
sep("PARTE 2 — Incidentes por OLT")

olt_b2 = aggregate_by_key(
    b2,
    group_col="olt_equipamento",
    metrics={
        "n_incidentes":       ("motivo_abertura", "count"),
        "indisp_total_h":     ("duracao_h",       "sum"),
        "indisp_mediana_h":   ("duracao_h",       "median"),
        "indisp_p90_h":       ("duracao_h",       "p90"),
        "incid_longos_pct":   ("duracao_h",       make_gt_pct(24)),
    },
).rename(columns={"olt_equipamento": "olt_id"})

subsep("Top 20 por indisponibilidade acumulada (h)")
print(
    top_n(olt_b2, "indisp_total_h",
          cols=["olt_id","n_incidentes","indisp_total_h","indisp_mediana_h","incid_longos_pct"])
    .round(2).to_string(index=False)
)


# ─── PARTE 3: Tickets por OLT via cliente (B4 → B1) ──────────────────────────
sep("PARTE 3 — Tickets por OLT (B4 → B1)")

# Agrega tickets por cliente
cli_b4 = aggregate_by_key(
    b4,
    group_col="id_cliente",
    metrics={
        "n_tickets":         ("status_ticket",           "count"),
        "tickets_abertos":   ("status_ticket",           make_pct_value("Aberto")),
        "prazo_medio":       ("prazo_resolucao",         "mean"),
        "rechamadas_24h":    ("flag_rechamada_voz_24h",  "sum"),
        "tem_financeiro":    ("motivo",                  make_pct_value("FINANCEIRO")),
    },
)

# Mapeia cliente → OLT
cli_olt = (
    b1.dropna(subset=["olt_id"])[["codigo_cliente","olt_id"]]
    .drop_duplicates("codigo_cliente", keep="last")
    .rename(columns={"codigo_cliente": "id_cliente"})
)
cli_b4 = cli_b4.merge(cli_olt, on="id_cliente", how="inner")
print(f"Clientes com ticket + OLT mapeado: {len(cli_b4):,}")

olt_b4 = aggregate_by_key(
    cli_b4,
    group_col="olt_id",
    metrics={
        "n_cli_ticket":      ("id_cliente",      "nunique"),
        "tickets_por_cli":   ("n_tickets",       "mean"),
        "pct_ticket_aberto": ("tickets_abertos", "mean"),
        "prazo_medio_global":("prazo_medio",     "mean"),
        "pct_rechamada_24h": ("rechamadas_24h",  make_gt_pct(0)),
        "pct_financeiro":    ("tem_financeiro",  make_gt_pct(0)),
    },
)

subsep("Top 20 OLTs por tickets por cliente (mín. 20 clientes com ticket)")
print(
    top_n(olt_b4[olt_b4["n_cli_ticket"] >= 20], "tickets_por_cli",
          cols=["olt_id","n_cli_ticket","tickets_por_cli","pct_ticket_aberto","pct_financeiro"])
    .round(2).to_string(index=False)
)


# ─── PARTE 4: Inadimplência por OLT via cliente (B5 → B1) ────────────────────
sep("PARTE 4 — Inadimplência por OLT (B5 → B1)")

cli_b5 = aggregate_by_key(
    b5,
    group_col="id_cliente",
    metrics={
        "n_faturas":      ("valor_fatura",     "count"),
        "inadimplente":   ("status_pagamento", make_pct_value("não pago/inadimplente")),
        "atraso_max":     ("dias_atraso",      "max"),
        "valor_medio":    ("valor_fatura",     "mean"),
    },
)
cli_b5 = cli_b5.merge(cli_olt, on="id_cliente", how="inner")

olt_b5 = aggregate_by_key(
    cli_b5,
    group_col="olt_id",
    metrics={
        "n_cli_fatura":     ("id_cliente",   "nunique"),
        "pct_inadimplentes":("inadimplente", make_gt_pct(0)),
        "atraso_mediano":   ("atraso_max",   "median"),
        "atraso_p90":       ("atraso_max",   "p90"),
        "valor_medio_fat":  ("valor_medio",  "mean"),
    },
)

subsep("Top 20 OLTs por % clientes inadimplentes (mín. 20 clientes)")
print(
    top_n(olt_b5[olt_b5["n_cli_fatura"] >= 20], "pct_inadimplentes",
          cols=["olt_id","n_cli_fatura","pct_inadimplentes","atraso_mediano","valor_medio_fat"])
    .round(1).to_string(index=False)
)


# ─── PARTE 5: Painel consolidado ─────────────────────────────────────────────
sep("PARTE 5 — Painel consolidado + Score de Risco")

painel = merge_tables(
    [
        olt_b1,
        olt_b2,
        olt_b4[["olt_id","n_cli_ticket","tickets_por_cli","pct_financeiro"]],
        olt_b5[["olt_id","n_cli_fatura","pct_inadimplentes","atraso_mediano"]],
    ],
    on="olt_id",
    how="left",
)

painel_ok = painel[painel["n_clientes"] >= 50].copy()

painel_ok["score_risco"] = compute_risk_score(
    painel_ok,
    metric_cols=["churn_pct","n_incidentes","indisp_total_h","tickets_por_cli","pct_inadimplentes"],
    weights={
        "churn_pct":       1.0,
        "n_incidentes":    1.0,
        "indisp_total_h":  1.0,
        "tickets_por_cli": 1.0,
        "pct_inadimplentes": 1.0,
    },
)

cols_show = ["olt_id","n_clientes","churn_pct","n_incidentes",
             "indisp_total_h","tickets_por_cli","pct_inadimplentes","score_risco"]

subsep("Top 30 por score de risco composto")
print(top_n(painel_ok, "score_risco", cols=cols_show, n=30).to_string(index=False))

subsep("Correlações de Spearman entre métricas")
corr_cols = ["churn_pct","n_incidentes","indisp_total_h",
             "tickets_por_cli","pct_inadimplentes","preco_mediano","vel_mediana"]
print(spearman_corr(painel_ok, corr_cols).to_string())

subsep("Duplo risco: alto churn + alta indisponibilidade")
duplo_op = double_high(painel_ok, "churn_pct", "indisp_total_h", quantile=0.75)
print(f"OLTs: {len(duplo_op)}")
print(top_n(duplo_op, "score_risco", cols=cols_show).to_string(index=False))

subsep("Duplo risco: alto churn + alta inadimplência")
duplo_soc = double_high(painel_ok, "churn_pct", "pct_inadimplentes", quantile=0.75)
print(f"OLTs: {len(duplo_soc)}")
print(top_n(duplo_soc, "score_risco", cols=cols_show).to_string(index=False))

sep("FIM")
