"""Fixtures sintéticas compartilhadas entre todos os testes."""
from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import numpy as np
import pytest

# Garante que scripts/ está no path para importar eda.*
sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))


# ─── DataFrames sintéticos ───────────────────────────────────────────────────

@pytest.fixture
def df_numeric() -> pd.DataFrame:
    """DataFrame apenas com colunas numéricas, incluindo um outlier extremo."""
    rng = np.random.default_rng(42)
    n = 200
    data = {
        "valor":    rng.normal(100, 15, n).tolist() + [9999],   # outlier extremo
        "duracao":  rng.exponential(5, n + 1),
        "score":    rng.uniform(0, 1, n + 1),
    }
    df = pd.DataFrame(data)
    df.loc[10, "valor"] = None  # um nulo
    return df


@pytest.fixture
def df_categorical() -> pd.DataFrame:
    """DataFrame apenas com colunas categóricas, incluindo coluna constante."""
    rng = np.random.default_rng(0)
    n = 100
    return pd.DataFrame({
        "regiao":    rng.choice(["Norte", "Sul", "Leste", "Oeste"], n),
        "status":    rng.choice(["ativo", "cancelado", "suspenso"], n),
        "constante": ["fixo"] * n,
    })


@pytest.fixture
def df_mixed() -> pd.DataFrame:
    """DataFrame misto com nulos, negativos e alta cardinalidade."""
    rng = np.random.default_rng(7)
    n = 150
    ids = [f"ID-{i:05d}" for i in range(n)]  # alta cardinalidade
    valores = rng.normal(50, 10, n)
    valores[5] = -99  # valor negativo
    nulos_mask = rng.choice(n, 30, replace=False)
    valores[nulos_mask] = np.nan  # >20% nulos
    return pd.DataFrame({
        "id":     ids,
        "valor":  valores,
        "status": rng.choice(["A", "B"], n),
    })


@pytest.fixture
def df_temporal() -> pd.DataFrame:
    """DataFrame com colunas de data para testes temporais."""
    dates = pd.date_range("2023-01-01", periods=120, freq="D")
    return pd.DataFrame({
        "data_abertura":   dates,
        "data_fechamento": dates + pd.to_timedelta(np.random.randint(1, 72, 120), unit="h"),
        "categoria":       np.random.choice(["A", "B", "C"], 120),
    })


@pytest.fixture
def tmp_csv(tmp_path: Path, df_mixed: pd.DataFrame) -> Path:
    """Arquivo CSV temporário com df_mixed."""
    p = tmp_path / "test_data.csv"
    df_mixed.to_csv(p, index=False)
    return p


@pytest.fixture
def tmp_parquet(tmp_path: Path, df_numeric: pd.DataFrame) -> Path:
    """Arquivo Parquet temporário com df_numeric."""
    p = tmp_path / "test_data.parquet"
    df_numeric.to_parquet(p, index=False)
    return p


# ─── Fixtures para concentration ─────────────────────────────────────────────

@pytest.fixture
def df_concentration_left() -> pd.DataFrame:
    """Tabela principal para testes de agregação por chave."""
    rng = np.random.default_rng(1)
    n = 300
    return pd.DataFrame({
        "olt_id":   rng.choice(["OLT-A", "OLT-B", "OLT-C"], n),
        "cliente":  [f"C{i:04d}" for i in range(n)],
        "churn":    rng.choice([None, "cancelado"], n, p=[0.7, 0.3]),
        "valor":    rng.uniform(50, 500, n),
        "flag":     rng.choice([True, False], n),
    })


@pytest.fixture
def df_concentration_right() -> pd.DataFrame:
    """Tabela de incidentes para testes de merge."""
    rng = np.random.default_rng(2)
    n = 120
    return pd.DataFrame({
        "olt_id":      rng.choice(["OLT-A", "OLT-B", "OLT-C", "OLT-D"], n),
        "n_incidentes": rng.integers(0, 10, n),
        "duracao_h":   rng.exponential(3, n),
    })


@pytest.fixture
def df_bridge() -> pd.DataFrame:
    """Tabela ponte para testes de join via multi-hop."""
    return pd.DataFrame({
        "codigo_cliente": [f"C{i:04d}" for i in range(300)],
        "olt_id":         (["OLT-A"] * 100 + ["OLT-B"] * 100 + ["OLT-C"] * 100),
    })
