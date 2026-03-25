"""
Runner genérico de EDA — carrega datasets via config YAML e gera relatório.

Uso:
    uv run python scripts/run_eda.py --config configs/telecom_alloha.yaml
    uv run python scripts/run_eda.py --config configs/telecom_alloha.yaml --output outputs/report.md
"""

from __future__ import annotations

import argparse
import sys
import warnings
from pathlib import Path
from typing import Any, Dict

import pandas as pd
import yaml

warnings.filterwarnings("ignore")

# Garante que scripts/ está no path para importar eda.*
sys.path.insert(0, str(Path(__file__).parent))

from eda.loader import load_dataset
from eda.profiler import profile_dataframe, null_coverage, detect_anomalies, crosstab_rate
from eda.temporal import (
    period_distribution, hourly_distribution, weekday_distribution,
    compute_duration, describe_duration,
)
from eda.concentration import (
    aggregate_by_key, merge_tables, compute_risk_score, top_n,
    double_high, spearman_corr, AGG_REGISTRY, make_gt_pct,
)
from eda.report import MarkdownReport


# ─── Registro de funções especiais que não estão no AGG_REGISTRY padrão ──────

def _resolve_func(func_name: str):
    """Resolve nome de função, incluindo fábricas parametrizadas."""
    if func_name in AGG_REGISTRY:
        return func_name
    # gt24_pct → make_gt_pct(24)
    if func_name.startswith("gt") and func_name.endswith("_pct"):
        threshold = float(func_name[2:-4])
        return make_gt_pct(threshold)
    raise ValueError(f"Função desconhecida: '{func_name}'")


# ─── Carregamento dos datasets ────────────────────────────────────────────────

def load_datasets(config: Dict[str, Any]) -> Dict[str, pd.DataFrame]:
    """Carrega todos os datasets definidos na config."""
    datasets: Dict[str, pd.DataFrame] = {}

    for name, cfg in config.get("datasets", {}).items():
        print(f"  → Carregando '{name}' ({cfg['path']})...")
        df = load_dataset(
            cfg["path"],
            sample_rows=cfg.get("sample_rows"),
            usecols=cfg.get("usecols"),
        )

        # Calcula duração se configurada
        dur_cfg = cfg.get("duration")
        if dur_cfg:
            df[dur_cfg["output_col"]] = compute_duration(
                df[dur_cfg["start_col"]], df[dur_cfg["end_col"]], unit=dur_cfg.get("unit", "h")
            )

        datasets[name] = df
        print(f"     {len(df):,} linhas × {df.shape[1]} colunas")

    return datasets


# ─── Análise de concentração ──────────────────────────────────────────────────

def run_concentration(
    config: Dict[str, Any],
    datasets: Dict[str, pd.DataFrame],
    report: MarkdownReport,
) -> None:
    """Executa a análise de concentração por chave e adiciona ao relatório."""
    conc_cfg = config.get("concentration", {})
    if not conc_cfg:
        return

    group_col   = conc_cfg["group_col"]
    min_size    = conc_cfg.get("min_group_size", 0)
    group_dfs   = []

    report.h2("Análise de Concentração")
    report.text(f"Chave de agrupamento: `{group_col}` | Tamanho mínimo de grupo: {min_size}")

    for group_name, gcfg in conc_cfg.get("groups", {}).items():
        ds_name    = gcfg["dataset"]
        src_col    = gcfg["group_col"]
        rename_to  = gcfg.get("rename_group_col")
        raw_metrics = gcfg.get("metrics", {})

        if ds_name not in datasets:
            print(f"  ⚠ Dataset '{ds_name}' não encontrado, pulando.")
            continue

        df = datasets[ds_name]
        metrics = {
            out: (m["col"], _resolve_func(m["func"]))
            for out, m in raw_metrics.items()
        }

        agg = aggregate_by_key(df, src_col, metrics, min_group_size=0)
        if rename_to:
            agg = agg.rename(columns={src_col: rename_to})

        group_dfs.append(agg)
        report.h3(f"Métricas — {group_name}")
        report.table(agg.sort_values(list(metrics.keys())[0], ascending=False).head(20))

    if not group_dfs:
        return

    painel = merge_tables(group_dfs, on=group_col, how="outer")
    painel = painel[painel.select_dtypes("number").iloc[:, 0].fillna(0) >= min_size]

    # Score de risco
    score_cfg = conc_cfg.get("risk_score", {})
    metric_cols     = [m["col"] for m in score_cfg.get("metrics", [])]
    weights         = {m["col"]: m.get("weight", 1.0) for m in score_cfg.get("metrics", [])}
    higher_is_worse = {m["col"]: m.get("higher_is_worse", True) for m in score_cfg.get("metrics", [])}

    if metric_cols:
        painel["score_risco"] = compute_risk_score(
            painel, metric_cols, weights=weights, higher_is_worse=higher_is_worse
        )

    report.h3("Painel Consolidado — Top 30 por Score de Risco")
    show_cols = [group_col] + [c for c in painel.columns if c != group_col]
    report.table(top_n(painel, "score_risco", cols=show_cols, n=30))

    # Correlações
    num_cols = painel.select_dtypes("number").columns.tolist()
    if len(num_cols) >= 2:
        report.h3("Correlações de Spearman entre métricas")
        report.table(spearman_corr(painel, num_cols), index=True)


# ─── Análise por dataset ──────────────────────────────────────────────────────

def run_dataset_analysis(
    name: str,
    df: pd.DataFrame,
    cfg: Dict[str, Any],
    report: MarkdownReport,
) -> None:
    """Executa análise exploratória de um único dataset e adiciona ao relatório."""
    target_col = cfg.get("target_col")
    primary_date = cfg.get("primary_date_col")
    anomalies = detect_anomalies(df)

    report.h2(f"Dataset: {name}")
    report.metric("Linhas", f"{len(df):,}")
    report.metric("Colunas", df.shape[1])
    report.metric("Arquivo", cfg["path"])

    # Nulos
    coverage = null_coverage(df)
    with_nulls = coverage[coverage["pct_null"] > 0]
    report.h3("Cobertura de nulos")
    if len(with_nulls):
        report.table(with_nulls)
    else:
        report.text("Sem nulos.")

    # Numéricas
    num_cols = df.select_dtypes("number").columns.tolist()
    if num_cols:
        report.h3("Estatísticas — colunas numéricas")
        report.table(df[num_cols].describe(percentiles=[.25, .5, .75, .9]).round(2).T)

    # Categóricas (top values por coluna)
    cat_cols = df.select_dtypes(exclude="number").columns.tolist()
    for col in cat_cols:
        n_uniq = df[col].nunique()
        if n_uniq > 200:
            continue  # skip colunas de altíssima cardinalidade
        counts = df[col].value_counts(dropna=False).head(10)
        tbl = pd.DataFrame({"n": counts, "%": (counts / len(df) * 100).round(1)})
        report.h4(f"{col} ({n_uniq} únicos)")
        report.table(tbl)

    # Distribuição temporal
    if primary_date and primary_date in df.columns:
        report.h3(f"Distribuição temporal — {primary_date}")
        dist = period_distribution(df[primary_date], freq="M")
        report.table(dist.head(24))

    # Cross-tab target
    if target_col:
        report.h3(f"Taxa de '{target_col}' por agrupador")
        for col in [cfg.get("group_col")] + (cfg.get("group_cols") or []):
            if col and col in df.columns and df[col].nunique() < 100:
                ct = crosstab_rate(df, col, target_col)
                report.h4(f"por {col}")
                report.table(ct)

    # Anomalias
    report.h3("Anomalias detectadas")
    if anomalies:
        report.alerts(anomalies, level="warning")
    else:
        report.text("Nenhuma anomalia detectada.")


# ─── CLI ─────────────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Runner genérico de EDA via config YAML.")
    p.add_argument("--config", type=Path, required=True, help="Caminho para o arquivo YAML de configuração.")
    p.add_argument("--output", type=Path, default=None, help="Caminho do relatório Markdown de saída.")
    return p.parse_args()


def main() -> int:
    args = parse_args()

    if not args.config.exists():
        print(f"Erro: arquivo de configuração não encontrado: {args.config}", file=sys.stderr)
        return 1

    config = yaml.safe_load(args.config.read_text(encoding="utf-8"))
    name        = config.get("name", "EDA")
    output_dir  = Path(config.get("output_dir", "outputs"))
    output_path = args.output or output_dir / "relatorio_eda.md"

    print(f"\n{'='*60}")
    print(f"  {name}")
    print(f"{'='*60}")

    report = MarkdownReport(name, description=f"Config: `{args.config}`")

    # Carrega datasets
    print("\nCarregando datasets...")
    datasets = load_datasets(config)

    # Analisa cada dataset
    for ds_name, df in datasets.items():
        ds_cfg = config["datasets"][ds_name]
        print(f"\nAnalisando '{ds_name}'...")
        run_dataset_analysis(ds_name, df, ds_cfg, report)

    # Concentração
    print("\nExecutando análise de concentração...")
    run_concentration(config, datasets, report)

    report.separator()
    report.text(f"*Gerado por `run_eda.py` com config `{args.config.name}`*")
    report.save(output_path)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
