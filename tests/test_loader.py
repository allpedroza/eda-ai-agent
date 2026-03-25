"""Testes para eda.loader."""
from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from eda.loader import find_files, load_file, load_dataset, load_all


class TestFindFiles:
    def test_finds_csv(self, tmp_csv: Path):
        results = find_files("*.csv", base_dir=tmp_csv.parent)
        assert any(p == tmp_csv for p in results)

    def test_finds_parquet(self, tmp_parquet: Path):
        results = find_files("*.parquet", base_dir=tmp_parquet.parent)
        assert any(p == tmp_parquet for p in results)

    def test_returns_empty_for_missing_pattern(self, tmp_path: Path):
        results = find_files("*.nonexistent", base_dir=tmp_path)
        assert results == []

    def test_filters_unsupported_extensions(self, tmp_path: Path):
        (tmp_path / "file.json").write_text("{}")
        results = find_files("*.json", base_dir=tmp_path)
        assert results == []


class TestLoadFile:
    def test_load_csv(self, tmp_csv: Path):
        df = load_file(tmp_csv)
        assert isinstance(df, pd.DataFrame)
        assert len(df) > 0

    def test_load_csv_sample_rows(self, tmp_csv: Path):
        df = load_file(tmp_csv, sample_rows=5)
        assert len(df) == 5

    def test_load_parquet(self, tmp_parquet: Path):
        df = load_file(tmp_parquet)
        assert isinstance(df, pd.DataFrame)
        assert len(df) > 0

    def test_load_parquet_usecols(self, tmp_parquet: Path):
        df = load_file(tmp_parquet, usecols=["valor"])
        assert list(df.columns) == ["valor"]

    def test_unsupported_extension_raises(self, tmp_path: Path):
        bad = tmp_path / "file.json"
        bad.write_text("{}")
        with pytest.raises(ValueError, match="Extensão não suportada"):
            load_file(bad)


class TestLoadDataset:
    def test_loads_first_match(self, tmp_csv: Path):
        df = load_dataset("*.csv", base_dir=tmp_csv.parent)
        assert isinstance(df, pd.DataFrame)

    def test_raises_on_no_match(self, tmp_path: Path):
        with pytest.raises(FileNotFoundError):
            load_dataset("*.csv", base_dir=tmp_path)


class TestLoadAll:
    def test_concatenates_multiple_files(self, tmp_path: Path):
        for i in range(3):
            pd.DataFrame({"x": [i]}).to_csv(tmp_path / f"part_{i}.csv", index=False)
        df = load_all("part_*.csv", base_dir=tmp_path)
        assert len(df) == 3

    def test_raises_on_no_match(self, tmp_path: Path):
        with pytest.raises(FileNotFoundError):
            load_all("nonexistent_*.csv", base_dir=tmp_path)
