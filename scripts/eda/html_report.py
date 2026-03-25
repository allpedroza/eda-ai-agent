"""
Gerador de HTML profile interativo — substitui ydata-profiling/sweetviz.

Usa apenas plotly (já instalado) + a infraestrutura do próprio eda.profiler,
sem nenhuma dependência de pkg_resources. Compatível com Python 3.13+.

Uso direto:
    from eda.html_report import generate_html_profile
    generate_html_profile(df, name="contratos", output_path=Path("outputs/profiles/contratos.html"))

Via run_eda.py:
    uv run python scripts/run_eda.py --config configs/example.yaml
"""
from __future__ import annotations

import html
from datetime import date
from pathlib import Path
from typing import Optional

import pandas as pd
import plotly.graph_objects as go
import plotly.io as pio

from eda.profiler import profile_dataframe


# ─── Helpers de renderização ─────────────────────────────────────────────────

def _df_to_html_table(df: pd.DataFrame, index: bool = False) -> str:
    """Converte DataFrame para tabela HTML estilizada."""
    return df.to_html(
        index=index,
        border=0,
        classes="eda-table",
        float_format=lambda x: f"{x:,.2f}",
        na_rep="—",
    )


def _plotly_div(fig: go.Figure, div_id: str) -> str:
    """Serializa figura plotly como div HTML (sem o <script> do plotlyjs)."""
    return pio.to_html(
        fig,
        full_html=False,
        include_plotlyjs=False,
        div_id=div_id,
        config={"displayModeBar": False, "responsive": True},
    )


def _alert_html(messages: list[str], level: str = "warning") -> str:
    colors = {
        "warning": "#fff3cd",
        "error":   "#f8d7da",
        "info":    "#d1ecf1",
        "success": "#d4edda",
    }
    bg = colors.get(level, colors["warning"])
    items = "".join(f'<li style="margin:4px 0">{html.escape(m)}</li>' for m in messages)
    return f'<ul style="background:{bg};padding:10px 16px;border-radius:6px;list-style:none;margin:8px 0">{items}</ul>'


# ─── Gráficos por coluna ──────────────────────────────────────────────────────

def _numeric_fig(series: pd.Series, col: str) -> go.Figure:
    clean = series.dropna()
    q1, q3 = clean.quantile(0.25), clean.quantile(0.75)
    iqr = q3 - q1

    fig = go.Figure()

    # Histograma
    fig.add_trace(go.Histogram(
        x=clean, name="Histograma",
        marker_color="steelblue", opacity=0.8,
        xbins=dict(size=(clean.max() - clean.min()) / 40 if clean.max() != clean.min() else 1),
        hovertemplate="%{x:.2f}<br>count: %{y}<extra></extra>",
    ))

    fig.update_layout(
        title=dict(text=col, font_size=13),
        xaxis_title=col,
        yaxis_title="Frequência",
        height=280,
        margin=dict(l=40, r=20, t=40, b=40),
        paper_bgcolor="white",
        plot_bgcolor="#f8f9fa",
        showlegend=False,
        bargap=0.05,
    )

    # Linhas de fence IQR
    for fence, label in [(q1 - 1.5 * iqr, "fence low"), (q3 + 1.5 * iqr, "fence high")]:
        fig.add_vline(x=float(fence), line_dash="dash", line_color="red",
                      line_width=1, annotation_text=label, annotation_font_size=9)

    return fig


def _categorical_fig(series: pd.Series, col: str, top_n: int = 20) -> go.Figure:
    counts = series.value_counts(dropna=False).head(top_n)
    labels = [str(v) for v in counts.index]
    pcts = (counts / counts.sum() * 100).round(1)

    fig = go.Figure(go.Bar(
        x=counts.values,
        y=labels,
        orientation="h",
        marker_color="steelblue",
        text=[f"{p}%" for p in pcts],
        textposition="outside",
        hovertemplate="%{y}: %{x:,} (%{text})<extra></extra>",
    ))
    height = max(200, len(counts) * 28 + 60)
    fig.update_layout(
        title=dict(text=col, font_size=13),
        xaxis_title="Contagem",
        height=height,
        margin=dict(l=160, r=60, t=40, b=30),
        paper_bgcolor="white",
        plot_bgcolor="#f8f9fa",
        yaxis=dict(autorange="reversed"),
    )
    return fig


# ─── Template HTML ────────────────────────────────────────────────────────────

_HTML_TEMPLATE = """\
<!DOCTYPE html>
<html lang="pt-BR">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{title}</title>
  <script src="https://cdn.plot.ly/plotly-2.27.0.min.js"></script>
  <style>
    body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
            background: #f0f2f5; margin: 0; padding: 0; color: #212529; }}
    .container {{ max-width: 1100px; margin: 0 auto; padding: 24px 16px; }}
    h1 {{ font-size: 1.6rem; margin-bottom: 4px; }}
    h2 {{ font-size: 1.2rem; border-bottom: 2px solid #dee2e6; padding-bottom: 6px;
           margin-top: 32px; color: #343a40; }}
    h3 {{ font-size: 1rem; color: #495057; margin: 20px 0 8px; }}
    .meta {{ color: #6c757d; font-size: .85rem; margin-bottom: 24px; }}
    .card {{ background: white; border-radius: 8px; padding: 20px; margin-bottom: 16px;
              box-shadow: 0 1px 4px rgba(0,0,0,.08); }}
    .metrics {{ display: flex; gap: 16px; flex-wrap: wrap; margin-bottom: 20px; }}
    .metric-box {{ background: #e9ecef; border-radius: 6px; padding: 12px 20px; min-width: 120px; }}
    .metric-box .val {{ font-size: 1.5rem; font-weight: 700; color: #0d6efd; }}
    .metric-box .lbl {{ font-size: .8rem; color: #6c757d; }}
    .eda-table {{ border-collapse: collapse; width: 100%; font-size: .82rem; }}
    .eda-table th {{ background: #343a40; color: white; padding: 6px 10px; text-align: right; }}
    .eda-table th:first-child {{ text-align: left; }}
    .eda-table td {{ padding: 5px 10px; border-bottom: 1px solid #dee2e6; text-align: right; }}
    .eda-table td:first-child {{ text-align: left; font-weight: 500; }}
    .eda-table tr:hover td {{ background: #f8f9fa; }}
    .col-grid {{ display: grid; grid-template-columns: repeat(auto-fill, minmax(480px, 1fr)); gap: 16px; }}
    .col-card {{ background: white; border-radius: 8px; padding: 16px;
                  box-shadow: 0 1px 3px rgba(0,0,0,.07); }}
  </style>
</head>
<body>
<div class="container">
  <h1>{title}</h1>
  <div class="meta">Gerado em {date} &nbsp;|&nbsp; Config: <code>{config}</code></div>

  <div class="metrics">
    <div class="metric-box"><div class="val">{n_rows}</div><div class="lbl">Linhas</div></div>
    <div class="metric-box"><div class="val">{n_cols}</div><div class="lbl">Colunas</div></div>
    <div class="metric-box"><div class="val">{n_num}</div><div class="lbl">Numéricas</div></div>
    <div class="metric-box"><div class="val">{n_cat}</div><div class="lbl">Categóricas</div></div>
    <div class="metric-box"><div class="val">{pct_null}%</div><div class="lbl">Nulos (médio)</div></div>
  </div>

  {anomalies_section}

  <h2>Cobertura de Nulos</h2>
  <div class="card">{null_table}</div>

  {numeric_section}
  {categorical_section}

</div>
</body>
</html>
"""


# ─── Função principal ─────────────────────────────────────────────────────────

def generate_html_profile(
    df: pd.DataFrame,
    name: str,
    output_path: Path,
    target_col: Optional[str] = None,
    title: Optional[str] = None,
    config_label: str = "",
) -> Path:
    """
    Gera um HTML profile interativo de um DataFrame usando plotly.

    Parâmetros:
        df           — DataFrame a perfilar
        name         — nome do dataset (usado no título)
        output_path  — caminho de saída do arquivo HTML
        target_col   — coluna alvo (destacada visualmente)
        title        — título customizado (padrão: "Profile — <name>")
        config_label — label de config exibido no cabeçalho

    Retorna o Path do arquivo salvo.
    """
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    title = title or f"Profile — {name}"
    profile = profile_dataframe(df, name=name, target_col=target_col, verbose=False)

    # ── Métricas de cabeçalho ──
    n_rows = f"{profile['shape']['rows']:,}"
    n_cols = str(profile['shape']['cols'])
    n_num  = str(len(profile['numeric']))
    n_cat  = str(len(profile['categorical']))
    avg_null = profile['null_coverage']['pct_null'].mean()
    pct_null = f"{avg_null:.1f}"

    # ── Anomalias ──
    anomalies_section = ""
    if profile['anomalies']:
        anomalies_section = f"<h2>Anomalias</h2><div class='card'>{_alert_html(profile['anomalies'])}</div>"

    # ── Tabela de nulos ──
    null_table = _df_to_html_table(profile['null_coverage'].reset_index().rename(columns={"index": "coluna"}))

    # ── Colunas numéricas ──
    numeric_cards = []
    for col, info in profile['numeric'].items():
        target_badge = " <span style='color:#dc3545;font-size:.75rem'>[TARGET]</span>" if info['is_target'] else ""
        stats_html = _df_to_html_table(info['stats'].to_frame(name="valor"))
        fig = _numeric_fig(df[col], col)
        chart_div = _plotly_div(fig, f"num_{col.replace(' ', '_')}")
        numeric_cards.append(f"""
        <div class="col-card">
          <h3>{html.escape(col)}{target_badge}</h3>
          <div style="display:grid;grid-template-columns:1fr 1fr;gap:12px;align-items:start">
            <div style="overflow-x:auto">{stats_html}</div>
            <div>{chart_div}</div>
          </div>
        </div>""")

    numeric_section = ""
    if numeric_cards:
        cards_html = "\n".join(numeric_cards)
        numeric_section = f"<h2>Colunas Numéricas</h2><div class='col-grid'>{cards_html}</div>"

    # ── Colunas categóricas ──
    cat_cards = []
    for col, info in profile['categorical'].items():
        if info['n_unique'] > 200:
            continue
        target_badge = " <span style='color:#dc3545;font-size:.75rem'>[TARGET]</span>" if info['is_target'] else ""
        top_html = _df_to_html_table(info['top_values'].reset_index().rename(columns={"index": col}))
        fig = _categorical_fig(df[col], col)
        chart_div = _plotly_div(fig, f"cat_{col.replace(' ', '_')}")
        n_u = info['n_unique']
        cat_cards.append(f"""
        <div class="col-card">
          <h3>{html.escape(col)}{target_badge} <span style="color:#6c757d;font-size:.8rem">({n_u} únicos)</span></h3>
          {chart_div}
          <div style="overflow-x:auto;margin-top:8px">{top_html}</div>
        </div>""")

    categorical_section = ""
    if cat_cards:
        cards_html = "\n".join(cat_cards)
        categorical_section = f"<h2>Colunas Categóricas</h2><div class='col-grid'>{cards_html}</div>"

    # ── Monta HTML final ──
    html_content = _HTML_TEMPLATE.format(
        title=html.escape(title),
        date=date.today().isoformat(),
        config=html.escape(config_label),
        n_rows=n_rows,
        n_cols=n_cols,
        n_num=n_num,
        n_cat=n_cat,
        pct_null=pct_null,
        anomalies_section=anomalies_section,
        null_table=null_table,
        numeric_section=numeric_section,
        categorical_section=categorical_section,
    )

    output_path.write_text(html_content, encoding="utf-8")
    return output_path
