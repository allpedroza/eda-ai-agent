"""EDA dos 4 blocos de dados de telecom — usa módulos eda.*.

Uso:
    uv run python scripts/eda_full.py
"""

from __future__ import annotations

import sys
import warnings
from pathlib import Path

import pandas as pd

warnings.filterwarnings("ignore")
sys.path.insert(0, str(Path(__file__).parent))

from eda.loader import load_dataset
from eda.profiler import profile_dataframe, crosstab_rate, value_counts_pct
from eda.temporal import (
    period_distribution, hourly_distribution, weekday_distribution,
    compute_duration, describe_duration,
)

INPUTS = Path("inputs")
SAMPLE = 200_000


def sep(title: str) -> None:
    print(f"\n{'='*70}\n  {title}\n{'='*70}")


def subsep(title: str) -> None:
    print(f"\n--- {title} ---")


# ─── BLOCO 1 — Clientes / Contratos ──────────────────────────────────────────
sep("BLOCO 1 — Clientes / Contratos")

b1 = load_dataset(
    "inputs/Bloco_1*.csv",
    sample_rows=SAMPLE,
    usecols=["codigo_cliente", "olt_id", "tipo_cancelamento", "preco_banda_larga",
             "velocidade_internet", "estado", "canal_venda", "mes_referencia",
             "produto_banda_larga", "marca_modem", "modelo_modem",
             "otts", "produtos", "serial_olt", "id_tecnico_instalacao",
             "email_vendedor", "data_entrada_base", "data_cancelamento"],
)

profile_dataframe(b1, name="Bloco 1 — Contratos", target_col="tipo_cancelamento")

subsep("Preço — distribuição por faixas")
bins   = [0, 70, 90, 110, 130, 150, 200, 9999]
labels = ["<70", "70-90", "90-110", "110-130", "130-150", "150-200", ">200"]
b1["faixa_preco"] = pd.cut(b1["preco_banda_larga"], bins=bins, labels=labels)
print(value_counts_pct(b1["faixa_preco"]).to_string())

subsep("Churn por canal de venda")
print(crosstab_rate(b1, "canal_venda", "tipo_cancelamento").to_string())

subsep("Churn por estado")
print(crosstab_rate(b1, "estado", "tipo_cancelamento").to_string())

subsep("Churn por velocidade (Mbps)")
print(crosstab_rate(b1, "velocidade_internet", "tipo_cancelamento").to_string())

subsep("Churn por faixa de preço")
print(crosstab_rate(b1, "faixa_preco", "tipo_cancelamento").to_string())

subsep("Top 10 produtos banda larga")
print(b1["produto_banda_larga"].value_counts().head(10).to_string())

subsep("Distribuição temporal — mes_referencia")
print(period_distribution(b1["mes_referencia"], freq="M").to_string())


# ─── BLOCO 2 — Incidentes de Rede ────────────────────────────────────────────
sep("BLOCO 2 — Incidentes de Rede")

b2 = load_dataset("inputs/Bloco_2*.csv")
b2["duracao_h"] = compute_duration(b2["inicio_evento"], b2["fim_evento"], unit="h")

profile_dataframe(b2, name="Bloco 2 — Incidentes")

subsep("Duração dos incidentes (horas)")
describe_duration(b2["duracao_h"], unit_label="h")

subsep("Distribuição por mês")
print(period_distribution(b2["inicio_evento"], freq="M").to_string())

subsep("Por hora do dia")
print(hourly_distribution(b2["inicio_evento"]).to_string())

subsep("Por dia da semana")
print(weekday_distribution(b2["inicio_evento"]).to_string())

subsep("Top 20 OLTs por volume de incidentes")
print(b2["olt_equipamento"].value_counts().head(20).to_string())

subsep("Top 20 OLTs por duração total acumulada (h)")
print(b2.groupby("olt_equipamento")["duracao_h"].sum().sort_values(ascending=False).head(20).round(1).to_string())


# ─── BLOCO 4 — Tickets de Atendimento ────────────────────────────────────────
sep("BLOCO 4 — Tickets de Atendimento")

b4 = load_dataset(
    "inputs/Bloco_4*.csv",
    sample_rows=SAMPLE,
    usecols=["id_cliente", "status_ticket", "prazo_resolucao", "motivo",
             "canal_atendimento", "status_contrato", "flag_rechamada_voz_24h",
             "flag_rechamada_voz_7d", "qtd_chamados_financeiros_90d",
             "total_interacoes_voz", "data_abertura", "data_conclusao"],
)

profile_dataframe(b4, name="Bloco 4 — Tickets")

subsep("Prazo de resolução por canal")
print(b4.groupby("canal_atendimento")["prazo_resolucao"].agg(["median","mean","max","count"]).round(2).to_string())

subsep("Prazo de resolução por motivo")
print(b4.groupby("motivo")["prazo_resolucao"].agg(["median","mean","max","count"]).round(2).to_string())

subsep("Distribuição temporal — abertura de tickets")
print(period_distribution(b4["data_abertura"], freq="M").to_string())


# ─── BLOCO 5 — Faturas / Pagamentos ──────────────────────────────────────────
sep("BLOCO 5 — Faturas e Pagamentos")

b5 = load_dataset(
    "inputs/Bloco_5*.csv",
    sample_rows=SAMPLE,
    usecols=["id_cliente", "mes_referencia", "valor_fatura", "data_vencimento",
             "data_pagamento", "dias_atraso", "status_pagamento", "flag_mudanca_plano"],
)

profile_dataframe(b5, name="Bloco 5 — Faturas", target_col="status_pagamento")

subsep("Inadimplência por faixa de fatura")
bins_f   = [float("-inf"), 50, 90, 110, 130, 150, 200, float("inf")]
labels_f = ["<50","50-90","90-110","110-130","130-150","150-200",">200"]
b5["faixa_fatura"] = pd.cut(b5["valor_fatura"], bins=bins_f, labels=labels_f)
print(crosstab_rate(b5, "faixa_fatura", "status_pagamento",
                    target_is_notnull=False, target_value="não pago/inadimplente").to_string())

subsep("Dias de atraso — distribuição detalhada")
describe_duration(b5["dias_atraso"], unit_label="dias")

sep("FIM")
