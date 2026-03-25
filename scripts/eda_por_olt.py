"""Análise de concentração de métricas por OLT — cruzamento de todos os blocos.

Joins:
  Bloco1.codigo_cliente  ↔  Bloco4.id_cliente   (tickets)
  Bloco1.codigo_cliente  ↔  Bloco5.id_cliente   (faturas)
  Bloco1.olt_id          ↔  Bloco2.olt_equipamento (incidentes)

Uso:
    uv run python scripts/eda_por_olt.py
"""

from __future__ import annotations

import warnings
from pathlib import Path

import pandas as pd
import numpy as np

warnings.filterwarnings("ignore")

INPUTS = Path("inputs")

B1_PATH = sorted(INPUTS.glob("Bloco_1*.csv"))[0]
B2_PATH = sorted(INPUTS.glob("Bloco_2*.csv"))[0]
B4_PATH = sorted(INPUTS.glob("Bloco_4*.csv"))[0]
B5_PATH = sorted(INPUTS.glob("Bloco_5*.csv"))[0]


def sep(title: str) -> None:
    print(f"\n{'='*70}")
    print(f"  {title}")
    print(f"{'='*70}")


def subsep(title: str) -> None:
    print(f"\n--- {title} ---")


# ─────────────────────────────────────────────────────────
# CARREGAMENTO EFICIENTE (só colunas necessárias)
# ─────────────────────────────────────────────────────────
sep("Carregando dados com colunas selecionadas...")

print("→ Bloco 1 (base completa, colunas-chave)...")
b1 = pd.read_csv(
    B1_PATH,
    usecols=["codigo_cliente", "olt_id", "serial_olt", "tipo_cancelamento",
             "preco_banda_larga", "velocidade_internet", "estado",
             "canal_venda", "mes_referencia"],
    low_memory=False,
)
print(f"  {len(b1):,} linhas × {b1.shape[1]} colunas")

print("→ Bloco 2 (incidentes, completo)...")
b2 = pd.read_csv(B2_PATH, low_memory=False)
b2["inicio_dt"] = pd.to_datetime(b2["inicio_evento"], errors="coerce")
b2["fim_dt"]    = pd.to_datetime(b2["fim_evento"],    errors="coerce")
b2["duracao_h"] = (b2["fim_dt"] - b2["inicio_dt"]).dt.total_seconds() / 3600
print(f"  {len(b2):,} linhas × {b2.shape[1]} colunas")

print("→ Bloco 4 (tickets, colunas-chave)...")
b4 = pd.read_csv(
    B4_PATH,
    usecols=["id_cliente", "status_ticket", "prazo_resolucao",
             "motivo", "canal_atendimento", "flag_rechamada_voz_24h",
             "qtd_chamados_financeiros_90d"],
    low_memory=False,
)
print(f"  {len(b4):,} linhas × {b4.shape[1]} colunas")

print("→ Bloco 5 (faturas, colunas-chave)...")
b5 = pd.read_csv(
    B5_PATH,
    usecols=["id_cliente", "status_pagamento", "valor_fatura", "dias_atraso"],
    low_memory=False,
)
print(f"  {len(b5):,} linhas × {b5.shape[1]} colunas")


# ─────────────────────────────────────────────────────────
# PARTE 1 — MÉTRICAS POR OLT A PARTIR DO BLOCO 1
# ─────────────────────────────────────────────────────────
sep("PARTE 1 — Métricas de contrato/churn por OLT (Bloco 1)")

b1["churn"] = b1["tipo_cancelamento"].notna().astype(int)

olt_b1 = (
    b1.dropna(subset=["olt_id"])
    .groupby("olt_id")
    .agg(
        n_registros       = ("codigo_cliente", "count"),
        n_clientes_unicos = ("codigo_cliente", "nunique"),
        churn_pct         = ("churn",          lambda x: round(x.mean() * 100, 1)),
        cancelados        = ("churn",           "sum"),
        preco_mediano     = ("preco_banda_larga", "median"),
        preco_medio       = ("preco_banda_larga", "mean"),
        vel_mediana       = ("velocidade_internet", "median"),
    )
    .reset_index()
)

print(f"\nOLTs distintas em Bloco 1: {olt_b1['olt_id'].nunique():,}")
print(f"Registros sem olt_id: {b1['olt_id'].isna().sum():,} ({b1['olt_id'].isna().mean()*100:.1f}%)")

subsep("Top 20 OLTs por maior taxa de churn (mín. 50 clientes únicos)")
top_churn_olt = (
    olt_b1[olt_b1["n_clientes_unicos"] >= 50]
    .sort_values("churn_pct", ascending=False)
    .head(20)
)
print(top_churn_olt[["olt_id","n_clientes_unicos","cancelados","churn_pct","preco_mediano","vel_mediana"]].to_string(index=False))

subsep("Top 20 OLTs com maior volume absoluto de cancelamentos")
top_cancel_abs = (
    olt_b1.sort_values("cancelados", ascending=False).head(20)
)
print(top_cancel_abs[["olt_id","n_clientes_unicos","cancelados","churn_pct","preco_mediano","vel_mediana"]].to_string(index=False))

subsep("Distribuição de churn_pct por OLT (percentis)")
print(olt_b1["churn_pct"].describe(percentiles=[.1,.25,.5,.75,.9,.95]).round(1).to_string())


# ─────────────────────────────────────────────────────────
# PARTE 2 — MÉTRICAS DE INCIDENTES POR OLT (BLOCO 2)
# ─────────────────────────────────────────────────────────
sep("PARTE 2 — Métricas de incidentes por OLT (Bloco 2)")

olt_b2 = (
    b2.groupby("olt_equipamento")
    .agg(
        n_incidentes         = ("motivo_abertura",  "count"),
        duracao_total_h      = ("duracao_h",        "sum"),
        duracao_mediana_h    = ("duracao_h",        "median"),
        duracao_p90_h        = ("duracao_h",        lambda x: x.quantile(0.9)),
        incidentes_24h_plus  = ("duracao_h",        lambda x: (x > 24).sum()),
    )
    .reset_index()
    .rename(columns={"olt_equipamento": "olt_id"})
)
olt_b2["duracao_total_h"]   = olt_b2["duracao_total_h"].round(1)
olt_b2["duracao_mediana_h"] = olt_b2["duracao_mediana_h"].round(2)
olt_b2["duracao_p90_h"]     = olt_b2["duracao_p90_h"].round(2)

print(f"\nOLTs com incidentes: {len(olt_b2):,}")

subsep("Top 20 OLTs por duração total de indisponibilidade (horas)")
print(
    olt_b2.sort_values("duracao_total_h", ascending=False)
    .head(20)
    .to_string(index=False)
)

subsep("Top 20 OLTs por volume de incidentes")
print(
    olt_b2.sort_values("n_incidentes", ascending=False)
    .head(20)
    .to_string(index=False)
)

subsep("Top 20 OLTs com mais incidentes longos (> 24h)")
print(
    olt_b2[olt_b2["incidentes_24h_plus"] > 0]
    .sort_values("incidentes_24h_plus", ascending=False)
    .head(20)
    .to_string(index=False)
)


# ─────────────────────────────────────────────────────────
# PARTE 3 — MÉTRICAS DE TICKETS POR CLIENTE → OLT (B4→B1)
# ─────────────────────────────────────────────────────────
sep("PARTE 3 — Tickets de atendimento por OLT (Bloco 4 → Bloco 1)")

# Agrega tickets por cliente
cli_b4 = (
    b4.groupby("id_cliente")
    .agg(
        n_tickets              = ("status_ticket",           "count"),
        tickets_abertos        = ("status_ticket",           lambda x: (x == "Aberto").sum()),
        prazo_medio_dias       = ("prazo_resolucao",         "mean"),
        rechamadas_24h         = ("flag_rechamada_voz_24h",  "sum"),
        chamados_fin_90d       = ("qtd_chamados_financeiros_90d", "max"),
        tem_suporte_tecnico    = ("motivo",                  lambda x: (x == "SUPORTE TÉCNICO").any()),
        tem_financeiro         = ("motivo",                  lambda x: (x == "FINANCEIRO").any()),
    )
    .reset_index()
)

# Mapeia cliente → OLT via Bloco 1 (pega o OLT mais recente por cliente)
cli_olt = (
    b1.dropna(subset=["olt_id"])[["codigo_cliente","olt_id"]]
    .drop_duplicates(subset="codigo_cliente", keep="last")
    .rename(columns={"codigo_cliente": "id_cliente"})
)

cli_b4_olt = cli_b4.merge(cli_olt, on="id_cliente", how="inner")
print(f"Clientes com tickets + OLT mapeado: {len(cli_b4_olt):,}")

olt_b4 = (
    cli_b4_olt.groupby("olt_id")
    .agg(
        n_clientes_com_ticket  = ("id_cliente",           "nunique"),
        total_tickets          = ("n_tickets",            "sum"),
        tickets_por_cliente    = ("n_tickets",            "mean"),
        pct_com_ticket_aberto  = ("tickets_abertos",      lambda x: (x > 0).mean() * 100),
        prazo_medio_global     = ("prazo_medio_dias",     "mean"),
        pct_com_rechamada_24h  = ("rechamadas_24h",       lambda x: (x > 0).mean() * 100),
        pct_tem_financeiro     = ("tem_financeiro",       "mean"),
        chamados_fin_p90       = ("chamados_fin_90d",     lambda x: x.quantile(0.9)),
    )
    .reset_index()
)
for col in ["tickets_por_cliente","pct_com_ticket_aberto","prazo_medio_global",
            "pct_com_rechamada_24h","pct_tem_financeiro","chamados_fin_p90"]:
    olt_b4[col] = olt_b4[col].round(2)

subsep("Top 20 OLTs por tickets por cliente (mín. 20 clientes com ticket)")
print(
    olt_b4[olt_b4["n_clientes_com_ticket"] >= 20]
    .sort_values("tickets_por_cliente", ascending=False)
    .head(20)
    .to_string(index=False)
)

subsep("Top 20 OLTs por % de clientes com ticket financeiro")
print(
    olt_b4[olt_b4["n_clientes_com_ticket"] >= 20]
    .sort_values("pct_tem_financeiro", ascending=False)
    .head(20)
    .to_string(index=False)
)


# ─────────────────────────────────────────────────────────
# PARTE 4 — MÉTRICAS DE INADIMPLÊNCIA POR OLT (B5→B1)
# ─────────────────────────────────────────────────────────
sep("PARTE 4 — Inadimplência por OLT (Bloco 5 → Bloco 1)")

cli_b5 = (
    b5.groupby("id_cliente")
    .agg(
        n_faturas       = ("valor_fatura",     "count"),
        valor_medio     = ("valor_fatura",     "mean"),
        inadimplente    = ("status_pagamento", lambda x: (x == "não pago/inadimplente").any()),
        pct_inadimp     = ("status_pagamento", lambda x: (x == "não pago/inadimplente").mean() * 100),
        atraso_max      = ("dias_atraso",      "max"),
        atraso_medio    = ("dias_atraso",      "mean"),
    )
    .reset_index()
)

cli_b5_olt = cli_b5.merge(cli_olt, on="id_cliente", how="inner")
print(f"Clientes com fatura + OLT mapeado: {len(cli_b5_olt):,}")

olt_b5 = (
    cli_b5_olt.groupby("olt_id")
    .agg(
        n_clientes_fatura   = ("id_cliente",    "nunique"),
        pct_inadimplentes   = ("inadimplente",  lambda x: round(x.mean() * 100, 1)),
        pct_faturas_inadimp = ("pct_inadimp",   "mean"),
        atraso_mediano      = ("atraso_max",    "median"),
        atraso_p90          = ("atraso_max",    lambda x: x.quantile(0.9)),
        valor_medio_fatura  = ("valor_medio",   "mean"),
    )
    .reset_index()
)
for col in ["pct_faturas_inadimp","atraso_mediano","atraso_p90","valor_medio_fatura"]:
    olt_b5[col] = olt_b5[col].round(1)

subsep("Top 20 OLTs por % de clientes inadimplentes (mín. 20 clientes)")
print(
    olt_b5[olt_b5["n_clientes_fatura"] >= 20]
    .sort_values("pct_inadimplentes", ascending=False)
    .head(20)
    .to_string(index=False)
)

subsep("Top 20 OLTs por atraso mediano máximo (dias)")
print(
    olt_b5[olt_b5["n_clientes_fatura"] >= 20]
    .sort_values("atraso_mediano", ascending=False)
    .head(20)
    .to_string(index=False)
)


# ─────────────────────────────────────────────────────────
# PARTE 5 — CONSOLIDADO POR OLT (todos os blocos)
# ─────────────────────────────────────────────────────────
sep("PARTE 5 — Painel consolidado por OLT")

painel = (
    olt_b1
    .merge(olt_b2, on="olt_id", how="left")
    .merge(olt_b4[["olt_id","n_clientes_com_ticket","tickets_por_cliente",
                   "pct_com_rechamada_24h","pct_tem_financeiro"]], on="olt_id", how="left")
    .merge(olt_b5[["olt_id","n_clientes_fatura","pct_inadimplentes",
                   "atraso_mediano","valor_medio_fatura"]], on="olt_id", how="left")
)

# Score de risco composto (normalizado 0-1 por métrica, soma simples)
def norm(s: pd.Series) -> pd.Series:
    mn, mx = s.min(), s.max()
    return (s - mn) / (mx - mn) if mx > mn else pd.Series(0.0, index=s.index)

painel_ok = painel[painel["n_clientes_unicos"] >= 50].copy()
painel_ok["score_risco"] = (
    norm(painel_ok["churn_pct"])
    + norm(painel_ok["duracao_total_h"].fillna(0))
    + norm(painel_ok["n_incidentes"].fillna(0))
    + norm(painel_ok["tickets_por_cliente"].fillna(0))
    + norm(painel_ok["pct_inadimplentes"].fillna(0))
).round(3)

subsep("Top 30 OLTs com maior score de risco composto (churn + incidentes + tickets + inadimplência)")
cols_show = ["olt_id","n_clientes_unicos","churn_pct","n_incidentes",
             "duracao_total_h","tickets_por_cliente","pct_inadimplentes","score_risco"]
print(
    painel_ok.sort_values("score_risco", ascending=False)
    .head(30)[cols_show]
    .to_string(index=False)
)

subsep("Correlações entre métricas por OLT")
corr_cols = ["churn_pct","n_incidentes","duracao_total_h",
             "tickets_por_cliente","pct_inadimplentes","preco_mediano","vel_mediana"]
corr_df = painel_ok[corr_cols].dropna()
print(f"OLTs na correlação: {len(corr_df)}")
print(corr_df.corr(method="spearman").round(3).to_string())

subsep("OLTs com alto churn E alta indisponibilidade (duplo risco operacional)")
duplo_risco = painel_ok[
    (painel_ok["churn_pct"] > painel_ok["churn_pct"].quantile(0.75)) &
    (painel_ok["duracao_total_h"].fillna(0) > painel_ok["duracao_total_h"].fillna(0).quantile(0.75))
].sort_values("score_risco", ascending=False)
print(f"OLTs nessa categoria: {len(duplo_risco)}")
print(duplo_risco[cols_show].head(20).to_string(index=False))

subsep("OLTs com alto churn E alta inadimplência (perfil de cliente frágil)")
fragilidade = painel_ok[
    (painel_ok["churn_pct"]        > painel_ok["churn_pct"].quantile(0.75)) &
    (painel_ok["pct_inadimplentes"].fillna(0) > painel_ok["pct_inadimplentes"].fillna(0).quantile(0.75))
].sort_values("score_risco", ascending=False)
print(f"OLTs nessa categoria: {len(fragilidade)}")
print(fragilidade[cols_show].head(20).to_string(index=False))

sep("FIM")
