"""
Runner genérico de EDA — carrega datasets via config YAML e gera relatório.

Uso:
    uv run python scripts/run_eda.py --config configs/example.yaml
    uv run python scripts/run_eda.py --config configs/example.yaml --output outputs/report.md
"""

from __future__ import annotations

import argparse
import sys
import warnings
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import yaml

warnings.filterwarnings("ignore")

sys.path.insert(0, str(Path(__file__).parent))

from eda.loader import load_dataset
from eda.profiler import profile_dataframe, plot_dataframe, null_coverage, detect_anomalies, crosstab_rate
from eda.temporal import period_distribution, compute_duration
from eda.concentration import (
    aggregate_by_key, merge_tables, compute_risk_score, top_n,
    double_high, spearman_corr, AGG_REGISTRY, make_gt_pct,
)
from eda.report import MarkdownReport


# ─── Funções auxiliares ───────────────────────────────────────────────────────

def _resolve_func(func_name: str):
    """Resolve nome de função, incluindo fábricas parametrizadas (ex: gt24_pct)."""
    if func_name in AGG_REGISTRY:
        return func_name
    if func_name.startswith("gt") and func_name.endswith("_pct"):
        try:
            threshold = float(func_name[2:-4])
            return make_gt_pct(threshold)
        except ValueError:
            pass
    raise ValueError(f"Função desconhecida: '{func_name}'. Disponíveis: {sorted(AGG_REGISTRY)}")


def _resolve_via_key(
    df: pd.DataFrame,
    gcfg: Dict[str, Any],
    datasets: Dict[str, pd.DataFrame],
) -> Tuple[pd.DataFrame, str]:
    """
    Aplica joins multi-hop definidos pela chave 'via' na config do grupo.

    A chave 'via' é uma lista de hops, cada um com:
        dataset   — nome do dataset ponte (já carregado em `datasets`)
        left_on   — chave no dataset atual (df)
        right_on  — chave no dataset ponte
        carry     — coluna a trazer do dataset ponte (torna-se o novo group_col)

    Retorna (df_enriquecido, effective_group_col).
    Se 'via' não estiver presente, retorna (df, gcfg['group_col']) inalterado.

    Exemplo de config YAML:
        tickets:
          dataset: tickets
          group_col: id_cliente
          via:
            - dataset: contratos
              left_on: id_cliente
              right_on: codigo_cliente
              carry: olt_id
          metrics:
            n_tickets: {col: status_ticket, func: count}
    """
    hops: List[Dict] = gcfg.get("via", [])
    if not hops:
        return df, gcfg["group_col"]

    current_df   = df.copy()
    current_key  = gcfg["group_col"]

    for hop in hops:
        bridge_name = hop["dataset"]
        left_on     = hop["left_on"]
        right_on    = hop["right_on"]
        carry       = hop["carry"]

        if bridge_name not in datasets:
            raise KeyError(f"Dataset ponte '{bridge_name}' não encontrado. Disponíveis: {list(datasets)}")

        bridge = (
            datasets[bridge_name][[right_on, carry]]
            .drop_duplicates(subset=right_on)
        )
        current_df = current_df.merge(
            bridge,
            left_on=left_on,
            right_on=right_on,
            how="left",
        )
        if right_on != left_on and right_on in current_df.columns:
            current_df = current_df.drop(columns=[right_on])
        current_key = carry

    return current_df, current_key


# ─── ydata-profiling ─────────────────────────────────────────────────────────

def run_html_profile(
    datasets: Dict[str, pd.DataFrame],
    output_dir: Path,
    config: Dict[str, Any],
) -> None:
    """
    Gera um relatório HTML do ydata-profiling para cada dataset.

    Salva em <output_dir>/profiles/<dataset_name>_profile.html.
    Aceita configuração opcional por dataset via chave 'profiling':
        profiling:
          mode: minimal      # 'minimal' | 'explorative' (padrão: minimal)
          title: "Meu título"
    """
    try:
        from ydata_profiling import ProfileReport
    except ImportError:
        print("  ⚠ ydata-profiling não instalado. Pulando geração de HTML profiles.")
        return

    profile_dir = output_dir / "profiles"
    profile_dir.mkdir(parents=True, exist_ok=True)

    for ds_name, df in datasets.items():
        ds_cfg   = config.get("datasets", {}).get(ds_name, {})
        prof_cfg = ds_cfg.get("profiling", {})
        mode     = prof_cfg.get("mode", "minimal")
        title    = prof_cfg.get("title", f"EDA — {ds_name}")
        minimal  = (mode == "minimal")

        print(f"  → {ds_name} ({len(df):,} linhas)...")
        profile  = ProfileReport(df, title=title, minimal=minimal, progress_bar=False)
        out_path = profile_dir / f"{ds_name}_profile.html"
        profile.to_file(out_path)
        print(f"     salvo em: {out_path}")


# ─── Análise por dataset ──────────────────────────────────────────────────────

def run_dataset_analysis(
    name: str,
    df: pd.DataFrame,
    cfg: Dict[str, Any],
    report: MarkdownReport,
    plots_dir: Path,
) -> None:
    """Executa análise exploratória de um dataset e adiciona ao relatório."""
    target_col   = cfg.get("target_col")
    primary_date = cfg.get("primary_date_col")

    # Perfil estruturado
    prof = profile_dataframe(df, name=name, target_col=target_col, verbose=False)

    report.h2(f"Dataset: {name}")
    report.metric("Linhas", f"{len(df):,}")
    report.metric("Colunas", df.shape[1])
    report.metric("Arquivo", cfg["path"])

    # Nulos
    with_nulls = prof["null_coverage"][prof["null_coverage"]["pct_null"] > 0]
    report.h3("Cobertura de nulos")
    if len(with_nulls):
        report.table(with_nulls)
    else:
        report.text("Sem nulos.")

    # Numéricas
    if prof["numeric"]:
        report.h3("Estatísticas — colunas numéricas")
        stats_rows = {col: info["stats"] for col, info in prof["numeric"].items()}
        report.table(pd.DataFrame(stats_rows).T.round(2))

    # Categóricas (top values)
    for col, info in prof["categorical"].items():
        if info["n_unique"] > 200:
            continue
        report.h4(f"{col} ({info['n_unique']} únicos)")
        report.table(info["top_values"])

    # Distribuição temporal
    if primary_date and primary_date in df.columns:
        report.h3(f"Distribuição temporal — {primary_date}")
        dist = period_distribution(df[primary_date], freq="M")
        report.table(dist.head(24))

    # Cross-tab target
    if target_col:
        group_col = cfg.get("group_col")
        if group_col and group_col in df.columns and df[group_col].nunique() < 100:
            report.h3(f"Taxa de '{target_col}' por {group_col}")
            report.table(crosstab_rate(df, group_col, target_col))

    # Anomalias
    report.h3("Anomalias detectadas")
    if prof["anomalies"]:
        report.alerts(prof["anomalies"], level="warning")
    else:
        report.text("Nenhuma anomalia detectada.")

    # Plots
    saved = plot_dataframe(df, name=name, output_dir=plots_dir)
    if saved:
        report.h3("Gráficos gerados")
        report.bullet([f"`{p.name}`" for p in saved])


# ─── Análise de concentração ──────────────────────────────────────────────────

def run_concentration(
    config: Dict[str, Any],
    datasets: Dict[str, pd.DataFrame],
    report: MarkdownReport,
) -> None:
    """Executa análise de concentração por chave e adiciona ao relatório."""
    conc_cfg = config.get("concentration", {})
    if not conc_cfg:
        return

    group_col   = conc_cfg["group_col"]
    min_size    = conc_cfg.get("min_group_size", 0)
    group_dfs: List[pd.DataFrame] = []

    report.h2("Análise de Concentração")
    report.text(f"Chave de agrupamento: `{group_col}` | Mínimo de registros por grupo: {min_size}")

    for group_name, gcfg in conc_cfg.get("groups", {}).items():
        ds_name    = gcfg["dataset"]
        rename_to  = gcfg.get("rename_group_col")
        raw_metrics = gcfg.get("metrics", {})

        if ds_name not in datasets:
            print(f"  ⚠ Dataset '{ds_name}' não encontrado, pulando '{group_name}'.")
            continue

        # Resolve joins multi-hop ('via')
        df, src_col = _resolve_via_key(datasets[ds_name], gcfg, datasets)

        metrics = {
            out: (m["col"], _resolve_func(m["func"]))
            for out, m in raw_metrics.items()
        }

        agg = aggregate_by_key(df, src_col, metrics, min_group_size=0)
        if rename_to:
            agg = agg.rename(columns={src_col: rename_to})

        group_dfs.append(agg)
        report.h3(f"Métricas — {group_name}")
        first_metric = list(metrics.keys())[0] if metrics else agg.columns[-1]
        report.table(agg.sort_values(first_metric, ascending=False).head(20))

    if not group_dfs:
        return

    # Painel consolidado
    painel = merge_tables(group_dfs, on=group_col, how="outer")
    num_cols = painel.select_dtypes("number").columns.tolist()
    if num_cols:
        painel = painel[painel[num_cols[0]].fillna(0) >= min_size]

    # Score de risco
    score_cfg   = conc_cfg.get("risk_score", {})
    metric_cols = [m["col"] for m in score_cfg.get("metrics", [])]
    weights     = {m["col"]: m.get("weight", 1.0) for m in score_cfg.get("metrics", [])}
    hiw         = {m["col"]: m.get("higher_is_worse", True) for m in score_cfg.get("metrics", [])}

    if metric_cols:
        painel["score_risco"] = compute_risk_score(
            painel, metric_cols, weights=weights, higher_is_worse=hiw
        )

    report.h3("Painel Consolidado — Top 30 por Score de Risco")
    show_cols = [group_col] + [c for c in painel.columns if c != group_col]
    report.table(top_n(painel, "score_risco", cols=show_cols, n=30))

    # Correlações
    num_cols = painel.select_dtypes("number").columns.tolist()
    if len(num_cols) >= 2:
        report.h3("Correlações de Spearman entre métricas")
        report.table(spearman_corr(painel, num_cols), index=True)


# ─── Carregamento ─────────────────────────────────────────────────────────────

def load_datasets(config: Dict[str, Any]) -> Dict[str, pd.DataFrame]:
    """Carrega todos os datasets definidos na config."""
    datasets: Dict[str, pd.DataFrame] = {}
    for name, cfg in config.get("datasets", {}).items():
        print(f"  → Carregando '{name}' ({cfg['path']})...")
        df = load_dataset(cfg["path"], sample_rows=cfg.get("sample_rows"), usecols=cfg.get("usecols"))
        dur = cfg.get("duration")
        if dur:
            df[dur["output_col"]] = compute_duration(
                df[dur["start_col"]], df[dur["end_col"]], unit=dur.get("unit", "h")
            )
        datasets[name] = df
        print(f"     {len(df):,} linhas × {df.shape[1]} colunas")
    return datasets


# ─── CLI ─────────────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Runner genérico de EDA via config YAML.")
    p.add_argument("--config", type=Path, required=True, help="Caminho para o YAML de configuração.")
    p.add_argument("--output", type=Path, default=None, help="Caminho do relatório Markdown de saída.")
    p.add_argument("--no-plots",   action="store_true", help="Desativa geração de gráficos.")
    p.add_argument("--no-profile", action="store_true", help="Desativa geração de HTML do ydata-profiling.")
    return p.parse_args()


def main() -> int:
    args = parse_args()

    if not args.config.exists():
        print(f"Erro: config não encontrada: {args.config}", file=sys.stderr)
        return 1

    config      = yaml.safe_load(args.config.read_text(encoding="utf-8"))
    name        = config.get("name", "EDA")
    output_dir  = Path(config.get("output_dir", "outputs"))
    output_path = args.output or output_dir / "relatorio_eda.md"
    plots_dir   = output_dir / "plots"

    print(f"\n{'='*60}\n  {name}\n{'='*60}")

    report = MarkdownReport(name, description=f"Config: `{args.config}`")

    print("\nCarregando datasets...")
    datasets = load_datasets(config)

    if not args.no_profile:
        print("\nGerando perfis HTML (ydata-profiling)...")
        run_html_profile(datasets, output_dir, config)

    for ds_name, df in datasets.items():
        ds_cfg = config["datasets"][ds_name]
        print(f"\nAnalisando '{ds_name}'...")
        run_dataset_analysis(
            ds_name, df, ds_cfg, report,
            plots_dir=(Path("dummy") if args.no_plots else plots_dir),
        )

    print("\nExecutando análise de concentração...")
    run_concentration(config, datasets, report)

    report.separator()
    report.text(f"*Gerado por `run_eda.py` com config `{args.config.name}`*")
    report.save(output_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
