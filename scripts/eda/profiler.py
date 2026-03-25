"""Análise univariada genérica: nulos, distribuições, outliers, anomalias e plots."""

from __future__ import annotations

from pathlib import Path
from typing import Dict, List, Optional

import matplotlib
matplotlib.use("Agg")  # backend não-interativo; seguro em scripts e headless
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
import numpy as np


# ─── Cobertura de nulos ──────────────────────────────────────────────────────

def null_coverage(df: pd.DataFrame) -> pd.DataFrame:
    """Retorna DataFrame com cobertura de nulos por coluna, ordenado do pior para o melhor."""
    return pd.DataFrame({
        "dtype":      df.dtypes.astype(str),
        "n_null":     df.isnull().sum(),
        "pct_null":   (df.isnull().mean() * 100).round(1),
        "n_unique":   df.nunique(dropna=False),
        "pct_unique": (df.nunique(dropna=False) / max(len(df), 1) * 100).round(1),
    }).sort_values("pct_null", ascending=False)


# ─── Estatísticas descritivas ────────────────────────────────────────────────

def describe_numeric(series: pd.Series, percentiles: Optional[List[float]] = None) -> pd.Series:
    """Estatísticas descritivas ampliadas com limites de outlier por IQR."""
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
    Detecta anomalias estruturais e retorna lista de mensagens.

    Verifica: constantes, alta taxa de nulos, valores negativos,
    outliers extremos e alta cardinalidade.
    """
    anomalies: List[str] = []
    for col in df.columns:
        s        = df[col]
        n_unique = s.nunique(dropna=True)
        pct_null = s.isnull().mean() * 100
        n        = len(s)

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
                n_neg = int((non_null < 0).sum())
                anomalies.append(
                    f"[NEGATIVO] '{col}': {n_neg} valor(es) negativo(s) (min={non_null.min():.2f})"
                )
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
                    f"[ALTA CARDINALIDADE] '{col}': {n_unique} únicos ({unique_ratio*100:.0f}%)"
                )

    return anomalies


# ─── Plots ───────────────────────────────────────────────────────────────────

def _safe_name(s: str) -> str:
    return s.replace(" ", "_").replace("/", "_").replace("\\", "_")


def plot_dataframe(
    df: pd.DataFrame,
    name: str = "dataset",
    output_dir: str | Path = "outputs/plots",
    max_categories: int = 20,
    figsize_numeric: tuple = (10, 4),
    figsize_cat: tuple = (10, 4),
    dpi: int = 100,
) -> List[Path]:
    """
    Gera e salva gráficos de distribuição para todas as colunas do DataFrame.

    Colunas numéricas   → histograma + boxplot (lado a lado)
    Colunas categóricas → barplot horizontal (top N categorias)

    Arquivos salvos como:
        <output_dir>/<name>__<col>__hist_box.png
        <output_dir>/<name>__<col>__barplot.png

    Parâmetros:
        df              — DataFrame a plotar
        name            — prefixo usado nos nomes dos arquivos
        output_dir      — diretório de saída (criado automaticamente)
        max_categories  — máximo de barras exibidas em colunas categóricas
        figsize_numeric — (largura, altura) em polegadas para plots numéricos
        figsize_cat     — (largura, altura) em polegadas para barplots
        dpi             — resolução das imagens salvas

    Retorna lista de Path dos arquivos gerados.
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    saved: List[Path] = []
    safe_ds = _safe_name(name)

    sns.set_theme(style="whitegrid", palette="muted")

    # Numéricas
    for col in df.select_dtypes(include="number").columns:
        series = df[col].dropna()
        if len(series) == 0:
            continue

        fig, axes = plt.subplots(1, 2, figsize=figsize_numeric)
        fig.suptitle(f"{name}  —  {col}", fontsize=12)

        sns.histplot(series, kde=True, ax=axes[0], color="steelblue")
        axes[0].set_title("Histograma + KDE")
        axes[0].set_xlabel(col)
        axes[0].set_ylabel("Frequência")

        sns.boxplot(y=series, ax=axes[1], color="steelblue", width=0.4)
        axes[1].set_title("Boxplot (IQR fences a vermelho)")
        axes[1].set_ylabel(col)
        q1, q3 = series.quantile(0.25), series.quantile(0.75)
        iqr = q3 - q1
        for fence in (q1 - 1.5 * iqr, q3 + 1.5 * iqr):
            axes[1].axhline(fence, color="red", linestyle="--", linewidth=0.8, alpha=0.7)

        fig.tight_layout()
        path = output_dir / f"{safe_ds}__{_safe_name(col)}__hist_box.png"
        fig.savefig(path, dpi=dpi, bbox_inches="tight")
        plt.close(fig)
        saved.append(path)

    # Categóricas
    for col in df.select_dtypes(exclude="number").columns:
        n_unique = df[col].nunique(dropna=False)
        if n_unique > 200:
            continue
        counts = df[col].value_counts(dropna=False).head(max_categories)
        if counts.empty:
            continue

        height = max(4, len(counts) * 0.45)
        fig, ax = plt.subplots(figsize=(figsize_cat[0], height))
        ax.set_title(f"{name}  —  {col}  ({n_unique} únicos)", fontsize=12)

        bars = ax.barh(
            [str(v) for v in counts.index],
            counts.values,
            color=sns.color_palette("muted")[0],
        )
        total = counts.sum()
        for bar, count in zip(bars, counts.values):
            ax.text(
                bar.get_width() + total * 0.005,
                bar.get_y() + bar.get_height() / 2,
                f"{count / total * 100:.1f}%",
                va="center",
                fontsize=8,
            )
        ax.set_xlabel("Contagem")
        ax.invert_yaxis()
        fig.tight_layout()
        path = output_dir / f"{safe_ds}__{_safe_name(col)}__barplot.png"
        fig.savefig(path, dpi=dpi, bbox_inches="tight")
        plt.close(fig)
        saved.append(path)

    return saved


# ─── Perfil completo ─────────────────────────────────────────────────────────

def profile_dataframe(
    df: pd.DataFrame,
    name: str = "Dataset",
    target_col: Optional[str] = None,
    top_n: int = 10,
    verbose: bool = True,
) -> Dict:
    """
    Computa e retorna um perfil exploratório completo de um DataFrame.

    Parâmetros:
        df         — DataFrame a analisar
        name       — label do dataset (usado em cabeçalhos e na chave do dict)
        target_col — coluna alvo (destacada no output e no dict)
        top_n      — nº de categorias a exibir por coluna categórica
        verbose    — se True, imprime o relatório formatado no stdout

    Retorna dict com estrutura:
        {
          "name":         str,
          "shape":        {"rows": int, "cols": int},
          "null_coverage": pd.DataFrame,
          "numeric":      {col: {"stats": pd.Series, "is_target": bool}},
          "categorical":  {col: {"n_unique": int, "top_values": pd.DataFrame, "is_target": bool}},
          "anomalies":    List[str],
        }
    """
    coverage  = null_coverage(df)
    anomalies = detect_anomalies(df)
    num_cols  = df.select_dtypes(include="number").columns.tolist()
    cat_cols  = df.select_dtypes(exclude="number").columns.tolist()

    profile: Dict = {
        "name":  name,
        "shape": {"rows": len(df), "cols": df.shape[1]},
        "null_coverage": coverage,
        "numeric": {
            col: {
                "stats":     describe_numeric(df[col]),
                "is_target": col == target_col,
            }
            for col in num_cols
        },
        "categorical": {
            col: {
                "n_unique":   int(df[col].nunique()),
                "top_values": value_counts_pct(df[col], top_n=top_n),
                "is_target":  col == target_col,
            }
            for col in cat_cols
        },
        "anomalies": anomalies,
    }

    if verbose:
        print_profile(profile)

    return profile


def print_profile(profile: Dict) -> None:
    """Imprime um profile dict gerado por profile_dataframe()."""
    bar   = "=" * 70
    name  = profile["name"]
    shape = profile["shape"]

    print(f"\n{bar}")
    print(f"  PERFIL: {name}  ({shape['rows']:,} linhas × {shape['cols']} colunas)")
    print(f"{bar}")

    with_nulls = profile["null_coverage"][profile["null_coverage"]["pct_null"] > 0]
    print("\n--- Cobertura de nulos ---")
    print(with_nulls.to_string() if len(with_nulls) else "  Sem nulos.")

    if profile["numeric"]:
        print("\n--- Colunas numéricas ---")
        for col, info in profile["numeric"].items():
            label = " ← TARGET" if info["is_target"] else ""
            print(f"\n  {col}{label}")
            print(info["stats"].to_string())

    if profile["categorical"]:
        print("\n--- Colunas categóricas / object ---")
        for col, info in profile["categorical"].items():
            label = " ← TARGET" if info["is_target"] else ""
            print(f"\n  {col}  ({info['n_unique']} únicos){label}")
            print(info["top_values"].to_string())

    print("\n--- Anomalias detectadas ---")
    if profile["anomalies"]:
        for a in profile["anomalies"]:
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

    Retorna DataFrame ordenado desc por taxa, filtrando grupos < min_count.
    """
    def _rate(x: pd.Series) -> float:
        if target_is_notnull:
            return x.notna().mean() * 100
        return (x == target_value).mean() * 100

    result = (
        df.groupby(group_col, dropna=False)
        .agg(
            total    =(target_col, "size"),
            positivos=(target_col, _rate),
        )
        .rename(columns={"positivos": f"taxa_{target_col}_%"})
    )
    return (
        result[result["total"] >= min_count]
        .sort_values(f"taxa_{target_col}_%", ascending=False)
        .round(1)
    )
