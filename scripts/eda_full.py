"""EDA completa dos 4 blocos de dados de telecom.

Uso:
    uv run python scripts/eda_full.py
"""

from __future__ import annotations

import warnings
from pathlib import Path

import pandas as pd
import numpy as np

warnings.filterwarnings("ignore")

INPUTS = Path("inputs")
SAMPLE = 200_000  # linhas para arquivos grandes

B1 = sorted(INPUTS.glob("Bloco_1*.csv"))[0]
B2 = sorted(INPUTS.glob("Bloco_2*.csv"))[0]
B4 = sorted(INPUTS.glob("Bloco_4*.csv"))[0]
B5 = sorted(INPUTS.glob("Bloco_5*.csv"))[0]


def sep(title: str) -> None:
    print(f"\n{'='*70}")
    print(f"  {title}")
    print(f"{'='*70}")


def subsep(title: str) -> None:
    print(f"\n--- {title} ---")


def pct(series: pd.Series) -> pd.DataFrame:
    counts = series.value_counts(dropna=False)
    pcts = (counts / len(series) * 100).round(1)
    return pd.DataFrame({"n": counts, "%": pcts})


# ─────────────────────────────────────────────
# BLOCO 1 — Base de clientes / contratos
# ─────────────────────────────────────────────
sep("BLOCO 1 — Clientes / Contratos (amostra de 200k linhas)")

b1 = pd.read_csv(B1, nrows=SAMPLE, low_memory=False)
print(f"Shape: {b1.shape}")
print(f"Período (mes_referencia): {sorted(b1['mes_referencia'].unique())}")

subsep("Cobertura de nulos por coluna")
null_pct = (b1.isnull().mean() * 100).round(1).sort_values(ascending=False)
print(null_pct[null_pct > 0].to_string())

subsep("Coluna 'produtos' — aparentemente vazia")
print(b1["produtos"].value_counts(dropna=False).head(10))

subsep("Preço banda larga — estatísticas")
print(b1["preco_banda_larga"].describe().round(2))
print("\nDistribuição por faixas de preço:")
bins = [0, 70, 90, 110, 130, 150, 200, 9999]
labels = ["<70", "70-90", "90-110", "110-130", "130-150", "150-200", ">200"]
b1["faixa_preco"] = pd.cut(b1["preco_banda_larga"], bins=bins, labels=labels)
print(pct(b1["faixa_preco"]).to_string())

subsep("Velocidade da internet (Mbps)")
print(pct(b1["velocidade_internet"]).to_string())

subsep("Cancelamentos")
total = len(b1)
cancelados = b1["tipo_cancelamento"].notna().sum()
print(f"Registros com cancelamento: {cancelados:,} ({cancelados/total*100:.1f}%)")
print("\nTipo de cancelamento:")
print(pct(b1["tipo_cancelamento"]).to_string())

subsep("Canal de venda")
print(pct(b1["canal_venda"]).to_string())

subsep("Estado (UF)")
print(pct(b1["estado"]).to_string())

subsep("Marca do modem (cobertura ~59%)")
print(pct(b1["marca_modem"]).to_string())

subsep("Modelo do modem (cobertura ~68%)")
print(pct(b1["modelo_modem"]).to_string())

subsep("OLT — cobertura e exemplos")
print(f"Cobertura olt_id: {b1['olt_id'].notna().mean()*100:.1f}%")
print(f"Cobertura serial_olt: {b1['serial_olt'].notna().mean()*100:.1f}%")

subsep("Top 10 produtos banda larga (por frequência)")
top_prod = b1["produto_banda_larga"].value_counts().head(10)
print(top_prod.to_string())

subsep("Churn por canal de venda (% de registros cancelados)")
canal_churn = b1.groupby("canal_venda", dropna=False).apply(
    lambda x: pd.Series({
        "total": len(x),
        "cancelados": x["tipo_cancelamento"].notna().sum(),
        "churn_%": round(x["tipo_cancelamento"].notna().mean() * 100, 1),
    })
).sort_values("churn_%", ascending=False)
print(canal_churn.to_string())

subsep("Churn por estado (% de registros cancelados)")
estado_churn = b1.groupby("estado", dropna=False).apply(
    lambda x: pd.Series({
        "total": len(x),
        "cancelados": x["tipo_cancelamento"].notna().sum(),
        "churn_%": round(x["tipo_cancelamento"].notna().mean() * 100, 1),
    })
).sort_values("churn_%", ascending=False)
print(estado_churn.to_string())

subsep("Churn por faixa de preço")
preco_churn = b1.groupby("faixa_preco", dropna=False).apply(
    lambda x: pd.Series({
        "total": len(x),
        "cancelados": x["tipo_cancelamento"].notna().sum(),
        "churn_%": round(x["tipo_cancelamento"].notna().mean() * 100, 1),
    })
)
print(preco_churn.to_string())

subsep("Churn por velocidade (Mbps)")
vel_churn = b1.groupby("velocidade_internet", dropna=False).apply(
    lambda x: pd.Series({
        "total": len(x),
        "cancelados": x["tipo_cancelamento"].notna().sum(),
        "churn_%": round(x["tipo_cancelamento"].notna().mean() * 100, 1),
    })
).sort_values("churn_%", ascending=False)
print(vel_churn.to_string())


# ─────────────────────────────────────────────
# BLOCO 2 — Incidentes de rede
# ─────────────────────────────────────────────
sep("BLOCO 2 — Incidentes de Rede (arquivo completo)")

b2 = pd.read_csv(B2, low_memory=False)
print(f"Shape: {b2.shape}")

subsep("Cobertura de nulos")
null_b2 = (b2.isnull().mean() * 100).round(1).sort_values(ascending=False)
print(null_b2[null_b2 > 0].to_string())

subsep("Motivo de abertura")
print(pct(b2["motivo_abertura"]).to_string())

subsep("Parsing de timestamps")
# Detectar formato: a coluna pode ter vírgula separando data e hora
sample_val = b2["inicio_evento"].dropna().iloc[0]
print(f"Exemplo inicio_evento: '{sample_val}'")
b2["inicio_dt"] = pd.to_datetime(b2["inicio_evento"], errors="coerce")
b2["fim_dt"] = pd.to_datetime(b2["fim_evento"], errors="coerce")
print(f"inicio_evento parseado: {b2['inicio_dt'].notna().mean()*100:.1f}%")
print(f"fim_evento parseado:    {b2['fim_dt'].notna().mean()*100:.1f}%")

subsep("Duração dos incidentes (horas)")
b2["duracao_h"] = (b2["fim_dt"] - b2["inicio_dt"]).dt.total_seconds() / 3600
dur = b2["duracao_h"].dropna()
print(f"N com duração calculável: {len(dur):,}")
print(dur.describe().round(2).to_string())
print(f"\nIncidentes > 24h: {(dur > 24).sum():,} ({(dur > 24).mean()*100:.1f}%)")
print(f"Incidentes > 72h: {(dur > 72).sum():,} ({(dur > 72).mean()*100:.1f}%)")
print(f"Duração negativa (erro de dados): {(dur < 0).sum():,}")

subsep("Incidentes sem fim_evento (em aberto)")
em_aberto = b2["fim_dt"].isna()
print(f"Em aberto: {em_aberto.sum():,} ({em_aberto.mean()*100:.1f}%)")

subsep("Top 20 OLTs por volume de incidentes")
print(b2["olt_equipamento"].value_counts().head(20).to_string())

subsep("Top 20 OLTs por duração total acumulada (horas)")
top_dur = b2.groupby("olt_equipamento")["duracao_h"].sum().sort_values(ascending=False).head(20)
print(top_dur.round(1).to_string())

subsep("Distribuição temporal — mês/ano do incidente")
b2["mes_inicio"] = b2["inicio_dt"].dt.to_period("M")
print(b2["mes_inicio"].value_counts().sort_index().to_string())

subsep("Incidentes por hora do dia")
b2["hora"] = b2["inicio_dt"].dt.hour
print(b2["hora"].value_counts().sort_index().to_string())

subsep("Incidentes por dia da semana")
dias = {0:"Seg",1:"Ter",2:"Qua",3:"Qui",4:"Sex",5:"Sáb",6:"Dom"}
b2["dia_semana"] = b2["inicio_dt"].dt.dayofweek.map(dias)
ordem = ["Seg","Ter","Qua","Qui","Sex","Sáb","Dom"]
print(b2["dia_semana"].value_counts().reindex(ordem).to_string())


# ─────────────────────────────────────────────
# BLOCO 4 — Tickets de atendimento
# ─────────────────────────────────────────────
sep("BLOCO 4 — Tickets de Atendimento (amostra de 200k linhas)")

b4 = pd.read_csv(B4, nrows=SAMPLE, low_memory=False)
print(f"Shape: {b4.shape}")

subsep("Cobertura de nulos")
null_b4 = (b4.isnull().mean() * 100).round(1).sort_values(ascending=False)
print(null_b4[null_b4 > 0].to_string())

subsep("Motivo do ticket")
print(pct(b4["motivo"]).to_string())

subsep("Canal de atendimento")
print(pct(b4["canal_atendimento"]).to_string())

subsep("Status do ticket")
print(pct(b4["status_ticket"]).to_string())

subsep("Status do contrato no momento do ticket")
print(pct(b4["status_contrato"]).to_string())

subsep("Prazo de resolução (dias) — estatísticas")
print(b4["prazo_resolucao"].describe().round(2).to_string())
print(f"\nResolvidos em 0 dias: {(b4['prazo_resolucao']==0).sum():,} ({(b4['prazo_resolucao']==0).mean()*100:.1f}%)")
print(f"Prazo > 7 dias:       {(b4['prazo_resolucao']>7).sum():,} ({(b4['prazo_resolucao']>7).mean()*100:.1f}%)")
print(f"Prazo > 30 dias:      {(b4['prazo_resolucao']>30).sum():,} ({(b4['prazo_resolucao']>30).mean()*100:.1f}%)")

subsep("Chamados financeiros (90 dias) — distribuição")
print(b4["qtd_chamados_financeiros_90d"].value_counts().sort_index().head(20).to_string())

subsep("Flags de rechamada em voz")
print(f"Rechamada 24h: {b4['flag_rechamada_voz_24h'].mean()*100:.1f}% dos tickets")
print(f"Rechamada 7d:  {b4['flag_rechamada_voz_7d'].mean()*100:.1f}% dos tickets")

subsep("Total de interações por voz — distribuição")
print(b4["total_interacoes_voz"].describe().round(2).to_string())

subsep("Prazo de resolução por canal de atendimento")
prazo_canal = b4.groupby("canal_atendimento")["prazo_resolucao"].agg(["median","mean","max","count"]).round(2)
print(prazo_canal.to_string())

subsep("Prazo de resolução por motivo")
prazo_motivo = b4.groupby("motivo")["prazo_resolucao"].agg(["median","mean","max","count"]).round(2)
print(prazo_motivo.to_string())

subsep("Distribuição temporal — mês de abertura")
b4["data_abertura_dt"] = pd.to_datetime(b4["data_abertura"], errors="coerce")
b4["mes_abertura"] = b4["data_abertura_dt"].dt.to_period("M")
print(b4["mes_abertura"].value_counts().sort_index().to_string())


# ─────────────────────────────────────────────
# BLOCO 5 — Faturas / Pagamentos
# ─────────────────────────────────────────────
sep("BLOCO 5 — Faturas e Pagamentos (amostra de 200k linhas)")

b5 = pd.read_csv(B5, nrows=SAMPLE, low_memory=False)
print(f"Shape: {b5.shape}")

subsep("Cobertura de nulos")
null_b5 = (b5.isnull().mean() * 100).round(1).sort_values(ascending=False)
print(null_b5[null_b5 > 0].to_string())

subsep("mes_referencia — valores únicos (atenção: formato '2019-00' suspeito)")
print(b5["mes_referencia"].value_counts().sort_index().to_string())

subsep("Status de pagamento")
print(pct(b5["status_pagamento"]).to_string())

subsep("Valor da fatura — estatísticas")
print(b5["valor_fatura"].describe().round(2).to_string())
print("\nDistribuição por faixas:")
bins_fat = [0, 50, 90, 110, 130, 150, 200, 9999]
labels_fat = ["<50", "50-90", "90-110", "110-130", "130-150", "150-200", ">200"]
b5["faixa_fatura"] = pd.cut(b5["valor_fatura"], bins=bins_fat, labels=labels_fat)
print(pct(b5["faixa_fatura"]).to_string())

subsep("Dias de atraso — estatísticas (atenção: valores altos suspeitos)")
print(b5["dias_atraso"].describe().round(2).to_string())
print(f"\nSem atraso (0 dias):  {(b5['dias_atraso']==0).sum():,} ({(b5['dias_atraso']==0).mean()*100:.1f}%)")
print(f"Atraso > 30 dias:     {(b5['dias_atraso']>30).sum():,} ({(b5['dias_atraso']>30).mean()*100:.1f}%)")
print(f"Atraso > 365 dias:    {(b5['dias_atraso']>365).sum():,} ({(b5['dias_atraso']>365).mean()*100:.1f}%)")
print(f"Atraso > 1000 dias:   {(b5['dias_atraso']>1000).sum():,} ({(b5['dias_atraso']>1000).mean()*100:.1f}%)")
print(f"\nTop 10 maiores atrasos:")
print(b5["dias_atraso"].nlargest(10).to_string())

subsep("flag_mudanca_plano — constante?")
print(b5["flag_mudanca_plano"].value_counts(dropna=False).to_string())

subsep("Inadimplência por faixa de fatura")
b5["inadimplente"] = b5["status_pagamento"] == "não pago/inadimplente"
inad_fatura = b5.groupby("faixa_fatura", dropna=False).apply(
    lambda x: pd.Series({
        "total": len(x),
        "inadimplentes": x["inadimplente"].sum(),
        "inadimplencia_%": round(x["inadimplente"].mean() * 100, 1),
    })
)
print(inad_fatura.to_string())

subsep("Status de pagamento por período (mes_referencia)")
pag_periodo = b5.groupby("mes_referencia")["status_pagamento"].value_counts(normalize=True).unstack(fill_value=0).round(3)
print(pag_periodo.to_string())


# ─────────────────────────────────────────────
# ANOMALIAS E ALERTAS CONSOLIDADOS
# ─────────────────────────────────────────────
sep("ANOMALIAS E ALERTAS CONSOLIDADOS")

print("""
BLOCO 1:
  [!] Coluna 'produtos': 0% de cobertura — praticamente vazia, checar fonte
  [!] 'otts': apenas 5,7% de cobertura — baixa utilidade sem enriquecimento
  [!] 'marca_modem' e 'modelo_modem': ~41% e ~32% de nulos — lacuna operacional
  [!] Datas armazenadas como string (object) — precisam de parse antes de modelagem
  [!] 'codigo_cliente' e 'codigo_contrato_air' classificados como numérica — são IDs

BLOCO 2:
  [!] 'inicio_evento' e 'fim_evento' com formato não-padrão (vírgula entre data e hora) — checar separador no CSV
  [!] Verificar duração negativa (erro de sequência início > fim)
  [!] OLTs sem correspondência direta com Bloco 1 — checar chave de join (olt_id x olt_equipamento)

BLOCO 4:
  [!] 'data_conclusao': 13% de nulos — tickets sem encerramento registrado
  [!] Checar consistência: status_ticket=Fechado com data_conclusao nula

BLOCO 5:
  [!] 'mes_referencia' com formato '2019-00', '2020-00' — mês "00" é inválido, provável erro de geração
  [!] 'dias_atraso': valores acima de 1000 dias — checar se são faturas históricas ou erro de cálculo
  [!] 'flag_mudanca_plano': coluna completamente constante ("não") — sem valor preditivo na amostra
  [!] 'data_pagamento': 19,7% de nulos — coerente com inadimplentes, mas confirmar
""")

sep("FIM DA EDA")
