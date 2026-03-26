"""
Gerador de tabelas auxiliares Excel — Análise Gravidade OLT × Churn
Alloha Telecom | Gerado em: 2026-03-26

Abas:
  1. LEIAME           — índice e dicionário de variáveis
  2. Painel_OLT_50    — painel completo ≥50 clientes por OLT
  3. Painel_OLT_100   — painel completo ≥100 clientes por OLT
  4. Top50_Gravidade  — ranking top-50 OLTs por gravidade
  5. OLTs_Alto_Risco  — 7 OLTs críticas (alta gravidade + alto churn voluntário)
  6. Serie_Mensal     — série temporal OLT × mês (churn + incidentes)
  7. Efeito_Defasado  — churn T, T+1, T+2, T+3 após incidente grave
  8. Correlacoes      — tabela Spearman completa (≥50 e ≥100)
"""

import pandas as pd
import numpy as np
from pathlib import Path
from scipy import stats

OUTPUT_PATH = Path("outputs/tabelas_auxiliares_rede_churn.xlsx")

# ─────────────────────────────────────────────
# 1. Carrega dados
# ─────────────────────────────────────────────
painel_50 = pd.read_parquet("/tmp/painel_olt_50v2.parquet")
painel_100 = pd.read_parquet("/tmp/painel_olt_100v2.parquet")
top50 = pd.read_parquet("/tmp/top50_ranking.parquet")
serie = pd.read_parquet("/tmp/serie_churn_mensal.parquet")
incid = pd.read_parquet("/tmp/incid_mensal.parquet")

# ─────────────────────────────────────────────
# 2. Enriquece painel: renomeia e formata colunas
# ─────────────────────────────────────────────
RENAME_PAINEL = {
    "olt_id":               "OLT",
    "n_clientes_total":     "N_Clientes_Total",
    "n_clientes_ativos":    "N_Clientes_Ativos",
    "n_voluntario":         "N_Churn_Voluntario",
    "n_involuntario":       "N_Churn_Involuntario",
    "churn_pct_total":      "Churn_Total_%",
    "churn_pct_voluntario": "Churn_Voluntario_%",
    "n_incidentes":         "N_Incidentes",
    "indisp_total_h":       "Indisp_Total_h",
    "indisp_mediana_h":     "Indisp_Mediana_h",
    "n_incid_comercial":    "N_Incid_Comercial",
    "n_incid_entret":       "N_Incid_Entretenimento",
    "n_incid_madrugada":    "N_Incid_Madrugada",
    "indisp_h_comercial":   "Indisp_h_Comercial",
    "indisp_h_entret":      "Indisp_h_Entretenimento",
    "indisp_h_madrugada":   "Indisp_h_Madrugada",
    "gravidade_ativos":     "Gravidade_Ativos",
    "gravidade_total":      "Gravidade_Total",
    "gravidade_comercial":  "Gravidade_Comercial",
    "gravidade_entret":     "Gravidade_Entretenimento",
    "perfil_predominante":  "Perfil_Horario_Predominante",
    "tem_incid_critico":    "Tem_Incidente_Critico",
}

p50 = painel_50.rename(columns=RENAME_PAINEL).sort_values("Gravidade_Ativos", ascending=False)
p100 = painel_100.rename(columns=RENAME_PAINEL).sort_values("Gravidade_Ativos", ascending=False)

# ─────────────────────────────────────────────
# 3. Top-50 renomeado
# ─────────────────────────────────────────────
RENAME_TOP50 = {
    "olt_id":               "OLT",
    "n_clientes_ativos":    "N_Clientes_Ativos",
    "n_clientes_total":     "N_Clientes_Total",
    "n_incidentes":         "N_Incidentes",
    "indisp_total_h":       "Indisp_Total_h",
    "indisp_mediana_h":     "Indisp_Mediana_h",
    "gravidade_ativos":     "Gravidade_Ativos",
    "gravidade_total":      "Gravidade_Total",
    "churn_pct_total":      "Churn_Total_%",
    "churn_pct_voluntario": "Churn_Voluntario_%",
    "n_incid_comercial":    "N_Incid_Comercial",
    "n_incid_entret":       "N_Incid_Entretenimento",
    "indisp_h_comercial":   "Indisp_h_Comercial",
    "gravidade_comercial":  "Gravidade_Comercial",
}
t50 = top50.rename(columns={c: RENAME_TOP50.get(c, c) for c in top50.columns})
t50.insert(0, "Rank", range(1, len(t50) + 1))

# ─────────────────────────────────────────────
# 4. OLTs Alto Risco
# ─────────────────────────────────────────────
alto_risco_ids = [
    "VIP-PAL-SPO-OHW-01",
    "OLT-FH-SPA01-03",
    "VIP-SZN-SPO-OHW-01",
    "VIP-SZN-SPO-OHW-02",
    "OLT-FH-BASJ05-01",
    "VIP-DDA-SER-OHW-02",
    "VIP-GRU-2-SPO-ONK-02",
]
alto_risco = p50[p50["OLT"].isin(alto_risco_ids)].copy()
alto_risco = alto_risco.sort_values("Churn_Voluntario_%", ascending=False)
alto_risco.insert(0, "Prioridade", range(1, len(alto_risco) + 1))

# ─────────────────────────────────────────────
# 5. Série temporal OLT × Mês
# ─────────────────────────────────────────────
serie_fmt = serie.copy()
incid_fmt = incid.copy()

# Merge série com incidentes mensais
serie_full = serie_fmt.merge(
    incid_fmt,
    left_on=["olt_id", "mes_ref_dt"],
    right_on=["olt_id", "mes_incid"],
    how="left"
).drop(columns=["mes_incid"])

serie_full = serie_full.rename(columns={
    "olt_id":          "OLT",
    "mes_ref_dt":      "Mes_Referencia",
    "mes_cancel":      "Mes_Cancelamento",
    "n_clientes":      "N_Clientes_Mes",
    "n_churn_vol":     "N_Churn_Voluntario",
    "churn_pct_vol":   "Churn_Voluntario_%",
    "n_incidentes":    "N_Incidentes_Mes",
    "indisp_h_mes":    "Indisp_h_Mes",
    "mes_grave_0h":    "Flag_Grave_0h",
    "mes_grave_10h":   "Flag_Grave_10h",
    "mes_grave_24h":   "Flag_Grave_24h",
})

serie_full["Mes_Referencia"] = serie_full["Mes_Referencia"].dt.strftime("%Y-%m")
if "Mes_Cancelamento" in serie_full.columns:
    serie_full["Mes_Cancelamento"] = serie_full["Mes_Cancelamento"].dt.strftime("%Y-%m")

serie_full = serie_full.sort_values(["OLT", "Mes_Referencia"])

# ─────────────────────────────────────────────
# 6. Efeito Defasado — recalcula da série completa
# ─────────────────────────────────────────────
# Junta série com incidentes e calcula lag
s = serie.copy().rename(columns={"mes_ref_dt": "mes"})
i = incid.copy().rename(columns={"mes_incid": "mes"})
panel = s.merge(i, on=["olt_id", "mes"], how="left").fillna({
    "n_incidentes": 0, "indisp_h_mes": 0,
    "mes_grave_0h": 0, "mes_grave_10h": 0, "mes_grave_24h": 0
})

panel = panel.sort_values(["olt_id", "mes"])

# Para cada OLT×mês, verifica se meses anteriores tiveram incidente grave (>24h)
panel["grave_este_mes"] = panel["mes_grave_24h"] > 0

# Baseline: OLT×mês sem incidente grave nos últimos 3 meses
panel["grave_lag1"] = panel.groupby("olt_id")["grave_este_mes"].shift(1).fillna(False)
panel["grave_lag2"] = panel.groupby("olt_id")["grave_este_mes"].shift(2).fillna(False)
panel["grave_lag3"] = panel.groupby("olt_id")["grave_este_mes"].shift(3).fillna(False)

baseline = panel[~panel["grave_este_mes"] & ~panel["grave_lag1"] & ~panel["grave_lag2"] & ~panel["grave_lag3"]]
t0_obs   = panel[panel["grave_este_mes"]]
t1_obs   = panel[panel["grave_lag1"] & ~panel["grave_este_mes"]]
t2_obs   = panel[panel["grave_lag2"] & ~panel["grave_este_mes"] & ~panel["grave_lag1"]]
t3_obs   = panel[panel["grave_lag3"] & ~panel["grave_este_mes"] & ~panel["grave_lag1"] & ~panel["grave_lag2"]]

def lag_summary(df, label, baseline_mean):
    n = len(df)
    mean = df["churn_pct_vol"].mean()
    median = df["churn_pct_vol"].median()
    std = df["churn_pct_vol"].std()
    delta_pp = mean - baseline_mean
    delta_pct = (delta_pp / baseline_mean) * 100 if baseline_mean > 0 else np.nan
    stat, p = stats.mannwhitneyu(df["churn_pct_vol"].dropna(), baseline["churn_pct_vol"].dropna(), alternative="greater")
    return {
        "Periodo": label,
        "N_Observacoes": n,
        "Churn_Medio_%": round(mean * 100, 4),
        "Churn_Mediana_%": round(median * 100, 4),
        "Desvio_Padrao": round(std * 100, 4),
        "Delta_vs_Baseline_pp": round(delta_pp * 100, 4),
        "Delta_vs_Baseline_%": round(delta_pct, 2),
        "Mann_Whitney_p": round(p, 6),
        "Significativo_001": "Sim" if p < 0.001 else "Não",
    }

baseline_mean = baseline["churn_pct_vol"].mean()

lag_rows = [
    {
        "Periodo": "Baseline (sem incidente grave nos últimos 3m)",
        "N_Observacoes": len(baseline),
        "Churn_Medio_%": round(baseline_mean * 100, 4),
        "Churn_Mediana_%": round(baseline["churn_pct_vol"].median() * 100, 4),
        "Desvio_Padrao": round(baseline["churn_pct_vol"].std() * 100, 4),
        "Delta_vs_Baseline_pp": 0.0,
        "Delta_vs_Baseline_%": 0.0,
        "Mann_Whitney_p": None,
        "Significativo_001": "— (referência)",
    },
    lag_summary(t0_obs, "T — mês do incidente grave", baseline_mean),
    lag_summary(t1_obs, "T+1 — mês seguinte", baseline_mean),
    lag_summary(t2_obs, "T+2 — 2 meses após", baseline_mean),
    lag_summary(t3_obs, "T+3 — 3 meses após", baseline_mean),
]

efeito_defasado = pd.DataFrame(lag_rows)

# ─────────────────────────────────────────────
# 7. Correlações — concatena ≥50 e ≥100 com threshold explícito
# ─────────────────────────────────────────────
corr_50_raw = pd.read_parquet("/tmp/corr_50.parquet").assign(Threshold="≥50 clientes")
corr_100_raw = pd.read_parquet("/tmp/corr_100.parquet").assign(Threshold="≥100 clientes")
corr_all = pd.concat([corr_50_raw, corr_100_raw], ignore_index=True)
corr_all = corr_all.rename(columns={
    "X": "Variavel_X",
    "Y": "Variavel_Y",
    "Escopo": "Escopo",
    "r": "r_Spearman",
    "p": "p_valor",
    "n": "N_OLTs",
    "sig": "Significancia",
})
# Adiciona tabela de correlações horárias manualmente (da análise)
extra_corr = pd.DataFrame([
    {"Variavel_X": "gravidade_comercial",    "Variavel_Y": "churn_pct_voluntario", "r_Spearman": 0.134, "p_valor": "<0.01",  "N_OLTs": 502, "Significancia": "**",  "Threshold": "≥50 clientes"},
    {"Variavel_X": "gravidade_entret",       "Variavel_Y": "churn_pct_voluntario", "r_Spearman": 0.098, "p_valor": "<0.05",  "N_OLTs": 502, "Significancia": "*",   "Threshold": "≥50 clientes"},
    {"Variavel_X": "indisp_h_comercial",     "Variavel_Y": "churn_pct_voluntario", "r_Spearman": 0.199, "p_valor": "<0.001", "N_OLTs": 502, "Significancia": "***", "Threshold": "≥50 clientes"},
    {"Variavel_X": "indisp_h_entret",        "Variavel_Y": "churn_pct_voluntario", "r_Spearman": 0.132, "p_valor": "<0.01",  "N_OLTs": 502, "Significancia": "**",  "Threshold": "≥50 clientes"},
    {"Variavel_X": "gravidade_comercial",    "Variavel_Y": "churn_pct_voluntario", "r_Spearman": 0.160, "p_valor": "<0.01",  "N_OLTs": 372, "Significancia": "**",  "Threshold": "≥100 clientes"},
    {"Variavel_X": "gravidade_entret",       "Variavel_Y": "churn_pct_voluntario", "r_Spearman": 0.129, "p_valor": "<0.05",  "N_OLTs": 372, "Significancia": "*",   "Threshold": "≥100 clientes"},
    {"Variavel_X": "indisp_h_comercial",     "Variavel_Y": "churn_pct_voluntario", "r_Spearman": 0.223, "p_valor": "<0.001", "N_OLTs": 372, "Significancia": "***", "Threshold": "≥100 clientes"},
    {"Variavel_X": "indisp_h_entret",        "Variavel_Y": "churn_pct_voluntario", "r_Spearman": 0.188, "p_valor": "<0.001", "N_OLTs": 372, "Significancia": "***", "Threshold": "≥100 clientes"},
])
corr_all = pd.concat([corr_all, extra_corr], ignore_index=True)
corr_all = corr_all[["Threshold", "Variavel_X", "Variavel_Y", "r_Spearman", "p_valor", "Significancia", "N_OLTs", "Escopo"]]

# ─────────────────────────────────────────────
# 8. LEIAME
# ─────────────────────────────────────────────
leiame_data = {
    "Aba": [
        "Painel_OLT_50",
        "Painel_OLT_100",
        "Top50_Gravidade",
        "OLTs_Alto_Risco",
        "Serie_Mensal",
        "Efeito_Defasado",
        "Correlacoes",
    ],
    "Descricao": [
        "Painel completo com todos os indicadores por OLT — threshold ≥50 clientes. 502 OLTs. Ordenado por Gravidade_Ativos.",
        "Painel completo com todos os indicadores por OLT — threshold ≥100 clientes. 372 OLTs. Ordenado por Gravidade_Ativos.",
        "Ranking Top-50 OLTs com maior Gravidade_Ativos (n_clientes_ativos × indisp_total_h). Threshold ≥50 clientes.",
        "As 7 OLTs que combinam alta gravidade (>500k) E alto churn voluntário (>20%). Alvos prioritários para ação operacional.",
        "Série temporal mensal por OLT: churn voluntário + incidentes por mês. Útil para análise de evolução ao longo do tempo.",
        "Impacto no churn voluntário mensal em T, T+1, T+2, T+3 após meses com incidente grave (>24h de indisponibilidade). Comparado ao baseline (meses sem incidente grave nos últimos 3 meses).",
        "Correlações Spearman entre métricas de rede e churn. Testadas para ≥50 e ≥100 clientes. *** p<0.001 / ** p<0.01 / * p<0.05.",
    ],
    "N_Linhas": [
        len(p50), len(p100), len(t50), len(alto_risco), len(serie_full), len(efeito_defasado), len(corr_all)
    ],
}

dicionario = pd.DataFrame([
    ("OLT", "Identificador da OLT (Optical Line Terminal) — equipamento físico de rede"),
    ("N_Clientes_Total", "Total de clientes vinculados à OLT na janela analisada (ativos + cancelados)"),
    ("N_Clientes_Ativos", "Clientes ativos no snapshot final de B1"),
    ("N_Churn_Voluntario", "Clientes que cancelaram voluntariamente (decisão do cliente)"),
    ("N_Churn_Involuntario", "Clientes que foram cancelados pelo operador (inadimplência)"),
    ("Churn_Total_%", "% de cancelamento total = (vol + invol) / total"),
    ("Churn_Voluntario_%", "% de cancelamento voluntário = vol / total"),
    ("N_Incidentes", "Número de incidentes de rede registrados nos últimos 12 meses (B2)"),
    ("Indisp_Total_h", "Soma de horas de indisponibilidade de todos os incidentes"),
    ("Indisp_Mediana_h", "Mediana da duração dos incidentes (em horas)"),
    ("Indisp_h_Comercial", "Horas de indisponibilidade em horário comercial (6h–19h)"),
    ("Indisp_h_Entretenimento", "Horas de indisponibilidade em horário de entretenimento (20h–23h)"),
    ("Indisp_h_Madrugada", "Horas de indisponibilidade em madrugada (0h–5h)"),
    ("Gravidade_Ativos", "KPI principal: N_Clientes_Ativos × Indisp_Total_h — impacto ponderado pelo tamanho da OLT"),
    ("Gravidade_Total", "N_Clientes_Total × Indisp_Total_h — versão alternativa do KPI"),
    ("Gravidade_Comercial", "N_Clientes_Ativos × Indisp_h_Comercial — gravidade em horário comercial"),
    ("Gravidade_Entretenimento", "N_Clientes_Ativos × Indisp_h_Entretenimento — gravidade em horário de entretenimento"),
    ("Perfil_Horario_Predominante", "Faixa horária com maior concentração de incidentes (≥50% dos incidentes)"),
    ("N_Incid_Comercial", "Quantidade de incidentes iniciados entre 6h–19h"),
    ("N_Incid_Entretenimento", "Quantidade de incidentes iniciados entre 20h–23h"),
    ("N_Incid_Madrugada", "Quantidade de incidentes iniciados entre 0h–5h"),
    ("Flag_Grave_0h", "1 se mês teve incidente com >0h acumuladas de indisponibilidade"),
    ("Flag_Grave_10h", "1 se mês teve incidente grave (>10h acumuladas)"),
    ("Flag_Grave_24h", "1 se mês teve incidente grave (>24h acumuladas) — limiar do efeito defasado"),
    ("Churn_Voluntario_%_mensal", "Taxa de cancelamento voluntário no mês (N_Churn / N_Clientes_Mes)"),
    ("Delta_vs_Baseline_pp", "Diferença em pontos percentuais em relação ao baseline (meses sem incidente)"),
    ("Delta_vs_Baseline_%", "Variação relativa em % em relação ao baseline"),
    ("Mann_Whitney_p", "p-valor do teste Mann-Whitney unilateral vs baseline"),
    ("r_Spearman", "Coeficiente de correlação de Spearman (rank-based, não-paramétrico)"),
], columns=["Variavel", "Descricao"])

leiame = pd.DataFrame(leiame_data)

# ─────────────────────────────────────────────
# 9. Exporta Excel
# ─────────────────────────────────────────────
with pd.ExcelWriter(OUTPUT_PATH, engine="xlsxwriter") as writer:
    wb = writer.book

    # Formatos
    fmt_header = wb.add_format({"bold": True, "bg_color": "#1F3864", "font_color": "white", "border": 1, "text_wrap": True, "valign": "vcenter"})
    fmt_header_orange = wb.add_format({"bold": True, "bg_color": "#C65911", "font_color": "white", "border": 1, "text_wrap": True, "valign": "vcenter"})
    fmt_pct = wb.add_format({"num_format": "0.00%", "border": 1})
    fmt_num = wb.add_format({"num_format": "#,##0", "border": 1})
    fmt_dec = wb.add_format({"num_format": "0.000", "border": 1})
    fmt_cell = wb.add_format({"border": 1})
    fmt_alto_risco = wb.add_format({"bg_color": "#FFE0E0", "border": 1})
    fmt_wrap = wb.add_format({"text_wrap": True, "border": 1, "valign": "top"})

    def write_df(sheet_name, df, header_fmt=fmt_header, freeze_col=1):
        df.to_excel(writer, sheet_name=sheet_name, index=False)
        ws = writer.sheets[sheet_name]
        # Header
        for col_num, col_name in enumerate(df.columns):
            ws.write(0, col_num, col_name, header_fmt)
        # Freeze
        ws.freeze_panes(1, freeze_col)
        ws.autofilter(0, 0, 0, len(df.columns) - 1)
        # Col width auto
        for col_num, col_name in enumerate(df.columns):
            max_len = max(len(str(col_name)), df[col_name].astype(str).map(len).max() if len(df) > 0 else 0)
            ws.set_column(col_num, col_num, min(max_len + 2, 40))
        return ws

    # ── LEIAME ──
    leiame.to_excel(writer, sheet_name="LEIAME", index=False, startrow=0)
    ws = writer.sheets["LEIAME"]
    ws.write(0, 0, "Aba", fmt_header)
    ws.write(0, 1, "Descricao", fmt_header)
    ws.write(0, 2, "N_Linhas", fmt_header)
    ws.set_column(0, 0, 25)
    ws.set_column(1, 1, 80)
    ws.set_column(2, 2, 12)

    ws.write(len(leiame) + 2, 0, "DICIONÁRIO DE VARIÁVEIS", fmt_header)
    dicionario.to_excel(writer, sheet_name="LEIAME", index=False, startrow=len(leiame) + 3)
    ws.write(len(leiame) + 3, 0, "Variavel", fmt_header)
    ws.write(len(leiame) + 3, 1, "Descricao", fmt_header)

    # ── PAINEL OLT ≥50 ──
    ws50 = write_df("Painel_OLT_50", p50, freeze_col=1)

    # ── PAINEL OLT ≥100 ──
    ws100 = write_df("Painel_OLT_100", p100, freeze_col=1)

    # ── TOP-50 ──
    write_df("Top50_Gravidade", t50, freeze_col=2)

    # ── OLTs ALTO RISCO ──
    ws_ar = write_df("OLTs_Alto_Risco", alto_risco, header_fmt=fmt_header_orange, freeze_col=2)
    # Destaca linha inteira em vermelho claro
    for row_num in range(1, len(alto_risco) + 1):
        for col_num in range(len(alto_risco.columns)):
            val = alto_risco.iloc[row_num - 1, col_num]
            ws_ar.write(row_num, col_num, val, fmt_alto_risco)

    # ── SÉRIE MENSAL ──
    write_df("Serie_Mensal", serie_full, freeze_col=2)

    # ── EFEITO DEFASADO ──
    write_df("Efeito_Defasado", efeito_defasado, freeze_col=1)

    # ── CORRELAÇÕES ──
    write_df("Correlacoes", corr_all, freeze_col=2)

print(f"Excel gerado em: {OUTPUT_PATH}")
print(f"Abas: LEIAME, Painel_OLT_50 ({len(p50)} OLTs), Painel_OLT_100 ({len(p100)} OLTs), "
      f"Top50_Gravidade, OLTs_Alto_Risco ({len(alto_risco)}), "
      f"Serie_Mensal ({len(serie_full)} linhas), Efeito_Defasado, Correlacoes ({len(corr_all)} linhas)")
