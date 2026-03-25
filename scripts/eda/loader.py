"""Carregamento genérico de arquivos tabulares com suporte a glob e sampling."""

from __future__ import annotations

from pathlib import Path
from typing import List, Optional, Sequence

import pandas as pd

SUPPORTED_EXTENSIONS = {".csv", ".parquet", ".feather", ".xlsx", ".xls"}


def find_files(pattern: str, base_dir: Path = Path(".")) -> List[Path]:
    """Resolve um glob pattern e retorna lista de paths ordenados."""
    paths = sorted(base_dir.glob(pattern))
    if not paths:
        # tenta como path absoluto ou relativo ao cwd
        paths = sorted(Path(".").glob(pattern))
    return [p for p in paths if p.suffix.lower() in SUPPORTED_EXTENSIONS]


def load_file(
    path: Path,
    sample_rows: Optional[int] = None,
    usecols: Optional[Sequence[str]] = None,
    **kwargs,
) -> pd.DataFrame:
    """Carrega um único arquivo tabular no formato detectado pela extensão."""
    suffix = path.suffix.lower()
    if suffix == ".csv":
        return pd.read_csv(path, nrows=sample_rows, usecols=usecols, low_memory=False, **kwargs)
    if suffix == ".parquet":
        df = pd.read_parquet(path, columns=usecols, **kwargs)
        return df.head(sample_rows) if sample_rows else df
    if suffix == ".feather":
        df = pd.read_feather(path, columns=usecols, **kwargs)
        return df.head(sample_rows) if sample_rows else df
    if suffix in {".xlsx", ".xls"}:
        return pd.read_excel(path, nrows=sample_rows, usecols=usecols, **kwargs)
    raise ValueError(f"Extensão não suportada: {suffix}")


def load_dataset(
    pattern: str,
    sample_rows: Optional[int] = None,
    usecols: Optional[Sequence[str]] = None,
    base_dir: Path = Path("."),
    **kwargs,
) -> pd.DataFrame:
    """
    Carrega o primeiro arquivo que corresponde ao glob pattern.

    Exemplo:
        df = load_dataset("inputs/Bloco_1*.csv", sample_rows=200_000,
                          usecols=["id", "status"])
    """
    files = find_files(pattern, base_dir=base_dir)
    if not files:
        raise FileNotFoundError(f"Nenhum arquivo encontrado para o padrão: '{pattern}'")
    return load_file(files[0], sample_rows=sample_rows, usecols=usecols, **kwargs)


def load_all(
    pattern: str,
    sample_rows: Optional[int] = None,
    usecols: Optional[Sequence[str]] = None,
    base_dir: Path = Path("."),
    **kwargs,
) -> pd.DataFrame:
    """
    Carrega e concatena todos os arquivos que correspondem ao glob pattern.
    Útil quando os dados estão particionados em múltiplos arquivos.
    """
    files = find_files(pattern, base_dir=base_dir)
    if not files:
        raise FileNotFoundError(f"Nenhum arquivo encontrado para o padrão: '{pattern}'")
    frames = [load_file(f, sample_rows=sample_rows, usecols=usecols, **kwargs) for f in files]
    return pd.concat(frames, ignore_index=True)
