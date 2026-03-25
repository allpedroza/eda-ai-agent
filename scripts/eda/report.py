"""Construtor de relatórios Markdown com API fluente."""

from __future__ import annotations

import datetime
from pathlib import Path
from typing import List, Optional

import pandas as pd


class MarkdownReport:
    """
    Constrói relatórios Markdown programaticamente com API fluente.

    Exemplo:
        report = (
            MarkdownReport("Análise de Clientes", "Relatório gerado automaticamente.")
            .h2("Distribuição de Churn")
            .text("Taxa de churn geral: 27,7%")
            .table(churn_df)
            .alert("Coluna 'produtos' está 100% nula.", level="warning")
            .h2("Anomalias")
            .code("uv run python scripts/run_eda.py --config configs/foo.yaml")
        )
        report.save("outputs/relatorio.md")
    """

    def __init__(self, title: str, description: str = "", date: bool = True) -> None:
        self._parts: List[str] = []
        self._parts.append(f"# {title}\n")
        if date:
            today = datetime.date.today().isoformat()
            self._parts.append(f"**Gerado em:** {today}\n")
        if description:
            self._parts.append(f"\n{description}\n")

    # ── Estrutura ──────────────────────────────────────────────────────────

    def h2(self, title: str) -> "MarkdownReport":
        self._parts.append(f"\n## {title}\n")
        return self

    def h3(self, title: str) -> "MarkdownReport":
        self._parts.append(f"\n### {title}\n")
        return self

    def h4(self, title: str) -> "MarkdownReport":
        self._parts.append(f"\n#### {title}\n")
        return self

    # ── Conteúdo ──────────────────────────────────────────────────────────

    def text(self, content: str) -> "MarkdownReport":
        """Adiciona parágrafo de texto."""
        self._parts.append(f"{content}\n")
        return self

    def bullet(self, items: List[str]) -> "MarkdownReport":
        """Adiciona lista com marcadores."""
        self._parts.append("\n".join(f"- {item}" for item in items) + "\n")
        return self

    def table(
        self,
        df: pd.DataFrame,
        index: bool = False,
        float_fmt: str = ".2f",
    ) -> "MarkdownReport":
        """Adiciona DataFrame como tabela Markdown."""
        try:
            md = df.to_markdown(index=index, floatfmt=float_fmt)
        except ImportError:
            # fallback sem tabulate
            md = df.to_string(index=index)
        self._parts.append(md + "\n")
        return self

    def alert(self, message: str, level: str = "warning") -> "MarkdownReport":
        """
        Adiciona bloco de alerta.

        level: 'warning' | 'error' | 'info' | 'success'
        """
        icons = {"warning": "⚠️", "error": "🔴", "info": "ℹ️", "success": "✅"}
        icon = icons.get(level, "•")
        self._parts.append(f"> {icon} **{message}**\n")
        return self

    def alerts(self, messages: List[str], level: str = "warning") -> "MarkdownReport":
        """Adiciona múltiplos alertas."""
        for msg in messages:
            self.alert(msg, level=level)
        return self

    def code(self, content: str, lang: str = "bash") -> "MarkdownReport":
        """Adiciona bloco de código."""
        self._parts.append(f"```{lang}\n{content}\n```\n")
        return self

    def separator(self) -> "MarkdownReport":
        """Adiciona linha horizontal."""
        self._parts.append("\n---\n")
        return self

    def metric(self, label: str, value, unit: str = "") -> "MarkdownReport":
        """Adiciona linha de métrica formatada."""
        self._parts.append(f"**{label}:** {value}{' ' + unit if unit else ''}\n")
        return self

    # ── Output ────────────────────────────────────────────────────────────

    def build(self) -> str:
        """Retorna o relatório como string Markdown."""
        return "\n".join(self._parts)

    def save(self, path: str | Path) -> "MarkdownReport":
        """Salva o relatório em arquivo. Cria diretórios intermediários se necessário."""
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(self.build(), encoding="utf-8")
        print(f"Relatório salvo em: {path}")
        return self

    def print(self) -> "MarkdownReport":
        """Imprime o relatório no stdout."""
        print(self.build())
        return self
