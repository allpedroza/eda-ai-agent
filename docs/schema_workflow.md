# Fluxo para inspeção de schemas na pasta `data/`

Este guia explica como gerar um resumo rápido dos arquivos disponíveis em `data/`
para apoiar a priorização de variáveis antes da etapa de modelagem.

## 1. Preparar o ambiente

1. Garanta que as dependências principais estejam instaladas (Python 3.9+, `pandas`).
2. Verifique se os arquivos tabulares (CSV, Parquet, Excel) estão salvos dentro de `data/`
ou de subpastas.

## 2. Executar o inspetor de schema

```bash
python scripts/schema_inspector.py --data-dir data --output schema_summary.md --json-output schema_summary.json
```

Argumentos úteis:

- `--target`: informe o nome da coluna alvo para que ela seja marcada explicitamente no resumo.
- `--sample-rows`: limite o número de linhas carregadas por arquivo caso os datasets sejam muito grandes.
- `--output`: caminho opcional para salvar um relatório em Markdown.
- `--json-output`: caminho opcional para exportar o resumo estruturado em JSON.

## 3. Interpretar o relatório

O relatório em Markdown contém, para cada tabela encontrada:

- **Papel sugerido**: indicação automática se a coluna parece um identificador, variável numérica,
  categórica, datetime ou candidata a target.
- **Cobertura**: percentual de valores não nulos para avaliar qualidade da variável.
- **Valores únicos** e exemplos: ajudam a entender a granularidade e a necessidade de codificação.
- **Observações**: alertas de cardinalidade, constância ou possíveis identificadores.

Ao final, há uma tabela "Variáveis candidatas para modelagem" que lista os campos com pelo menos 60% de
cobertura e tipos adequados (`numérica`, `categórica`, `binária`). Esse ranking inicial orienta quais
variáveis devem ser priorizadas na análise estatística e nos modelos.

## 4. Próximos passos sugeridos

1. Revisar manualmente as variáveis marcadas como "possível identificador" para confirmar se devem ser
   excluídas ou transformadas.
2. Validar com o time de negócio se as variáveis categóricas de alta cardinalidade precisam de agrupamentos
   (por exemplo, consolidar tipos de instituição ou canais de indicação).
3. Utilizar o JSON exportado para alimentar notebooks de EDA automatizada ou pipelines de feature store,
   garantindo rastreabilidade das decisões.
4. Atualizar o `eda_framework_plan.md` com observações específicas obtidas a partir do relatório para que
   as equipes de Marketing e Ciência de Dados tenham um mapa claro dos dados disponíveis.

Com esse fluxo, conseguimos responder rapidamente à pergunta sobre "quais variáveis temos à disposição"
assim que os arquivos são disponibilizados em `data/`, acelerando a priorização de hipóteses e features.
