"""Schema inspector for datasets in the data directory.

This script scans a directory for tabular files (CSV, Parquet, Excel) and
creates a consolidated summary highlighting potential modeling variables.

Usage:
    python scripts/schema_inspector.py --data-dir data --target target_column

The summary is printed to stdout and can optionally be saved to a Markdown file
with ``--output``.
"""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Dict, Iterable, List, Optional

import pandas as pd


SUPPORTED_EXTENSIONS = {".csv", ".parquet", ".feather", ".xlsx", ".xls"}


@dataclass
class ColumnSummary:
    name: str
    dtype: str
    role: str
    non_null_pct: float
    unique_values: int
    example_values: List[str]
    notes: List[str]

    def to_markdown_row(self) -> str:
        notes_text = "; ".join(self.notes) if self.notes else ""
        examples = ", ".join(self.example_values)
        return (
            f"| {self.name} | {self.dtype} | {self.role} | "
            f"{self.non_null_pct:.1f}% | {self.unique_values} | {examples} | {notes_text} |"
        )


@dataclass
class TableSummary:
    file_path: Path
    row_count: int
    column_summaries: List[ColumnSummary]

    def to_markdown(self) -> str:
        header = f"### {self.file_path.name} ({self.row_count:,} linhas)\n"
        table_header = (
            "| Coluna | Tipo | Papel sugerido | Cobertura | Valores únicos | Exemplos | Observações |\n"
            "| --- | --- | --- | --- | --- | --- | --- |\n"
        )
        rows = "\n".join(col.to_markdown_row() for col in self.column_summaries)
        return header + table_header + rows + "\n"


def infer_role(column: pd.Series, name: str, target: Optional[str]) -> (str, List[str]):
    lowered = name.lower()
    notes: List[str] = []

    if target and name == target:
        return "target", notes

    if "id" in lowered or "uuid" in lowered or lowered.endswith("_cd"):
        notes.append("possível identificador")
        return "id", notes

    if pd.api.types.is_datetime64_any_dtype(column):
        return "datetime", notes

    if pd.api.types.is_numeric_dtype(column):
        non_null = column.dropna()
        unique_values = non_null.nunique()
        if unique_values <= 1:
            notes.append("coluna constante")
        if unique_values == 2:
            notes.append("binária")
        if "percent" in lowered or "rate" in lowered:
            notes.append("verificar escala percentual")
        return "numérica", notes

    if pd.api.types.is_bool_dtype(column):
        return "binária", notes

    if pd.api.types.is_string_dtype(column) or pd.api.types.is_categorical_dtype(column):
        unique_ratio = column.nunique(dropna=False) / max(len(column), 1)
        if unique_ratio > 0.9:
            notes.append("alta cardinalidade")
        return "categórica", notes

    return "desconhecido", notes


def load_table(path: Path, sample_rows: Optional[int]) -> pd.DataFrame:
    suffix = path.suffix.lower()
    if suffix == ".csv":
        return pd.read_csv(path, nrows=sample_rows, low_memory=False)
    if suffix == ".parquet":
        return pd.read_parquet(path)
    if suffix == ".feather":
        return pd.read_feather(path)
    if suffix in {".xlsx", ".xls"}:
        return pd.read_excel(path, nrows=sample_rows)
    raise ValueError(f"Unsupported file type: {suffix}")


def summarize_table(path: Path, target: Optional[str], sample_rows: Optional[int]) -> TableSummary:
    df = load_table(path, sample_rows)
    row_count = len(df)
    summaries: List[ColumnSummary] = []

    for column_name in df.columns:
        series = df[column_name]
        role, notes = infer_role(series, column_name, target)

        non_null_pct = float(series.notna().mean() * 100) if len(series) else 0.0
        unique_values = int(series.nunique(dropna=False)) if len(series) else 0
        example_values = [str(val) for val in series.dropna().unique()[:3]]

        summaries.append(
            ColumnSummary(
                name=column_name,
                dtype=str(series.dtype),
                role=role,
                non_null_pct=non_null_pct,
                unique_values=unique_values,
                example_values=example_values,
                notes=notes,
            )
        )

    return TableSummary(file_path=path, row_count=row_count, column_summaries=summaries)


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Summarize schemas for datasets in a directory.")
    parser.add_argument("--data-dir", type=Path, default=Path("data"), help="Directory with source datasets.")
    parser.add_argument("--target", type=str, default=None, help="Target column for modeling (optional).")
    parser.add_argument(
        "--sample-rows",
        type=int,
        default=None,
        help="Number of rows to sample when loading large files.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Optional path to save the summary as Markdown.",
    )
    parser.add_argument(
        "--json-output",
        type=Path,
        default=None,
        help="Optional path to save the summary as JSON.",
    )
    return parser


def scan_data_dir(data_dir: Path) -> Iterable[Path]:
    for file_path in sorted(data_dir.glob("**/*")):
        if file_path.is_file() and file_path.suffix.lower() in SUPPORTED_EXTENSIONS:
            yield file_path


def main(argv: Optional[List[str]] = None) -> int:
    parser = build_argument_parser()
    args = parser.parse_args(argv)

    if not args.data_dir.exists():
        parser.error(f"Data directory '{args.data_dir}' does not exist.")

    table_paths = list(scan_data_dir(args.data_dir))
    if not table_paths:
        print("Nenhum arquivo tabular suportado foi encontrado.", file=sys.stderr)
        return 1

    table_summaries: List[TableSummary] = []
    for path in table_paths:
        try:
            table_summary = summarize_table(path, args.target, args.sample_rows)
            table_summaries.append(table_summary)
        except Exception as exc:  # pragma: no cover - diagnostic output
            print(f"Falha ao ler {path}: {exc}", file=sys.stderr)

    if not table_summaries:
        print("Não foi possível gerar resumo para nenhum arquivo.", file=sys.stderr)
        return 1

    markdown_parts = ["## Resumo de Schemas\n"]
    modeling_candidates: Dict[str, Dict[str, float]] = {}

    for table_summary in table_summaries:
        markdown_parts.append(table_summary.to_markdown())
        for col in table_summary.column_summaries:
            if col.role in {"numérica", "categórica", "binária"} and col.non_null_pct >= 60:
                key = f"{table_summary.file_path.name}::{col.name}"
                modeling_candidates[key] = {
                    "non_null_pct": col.non_null_pct,
                    "unique_values": col.unique_values,
                    "role": col.role,
                }

    if modeling_candidates:
        markdown_parts.append("### Variáveis candidatas para modelagem\n")
        markdown_parts.append(
            "| Variável | Papel | Cobertura | Valores únicos |\n| --- | --- | --- | --- |\n"
        )
        for name, info in sorted(
            modeling_candidates.items(),
            key=lambda item: (-item[1]["non_null_pct"], item[0]),
        ):
            markdown_parts.append(
                f"| {name} | {info['role']} | {info['non_null_pct']:.1f}% | {info['unique_values']} |\n"
            )

    markdown_output = "\n".join(markdown_parts)
    print(markdown_output)

    if args.output:
        args.output.write_text(markdown_output, encoding="utf-8")

    if args.json_output:
        json_payload = [
            {
                "file_path": str(summary.file_path),
                "row_count": summary.row_count,
                "columns": [asdict(col) for col in summary.column_summaries],
            }
            for summary in table_summaries
        ]
        args.json_output.write_text(json.dumps(json_payload, indent=2, ensure_ascii=False), encoding="utf-8")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
