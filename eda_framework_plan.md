# Plano de EDA — Dados de Telecom (Banda Larga + Incidentes de Rede)

Este documento descreve o roteiro de análise exploratória aplicado aos datasets disponíveis em `inputs/`:

- **Bloco_1**: Base mensal de clientes de banda larga (~3,9 M linhas) — contratos, preços, velocidades, cancelamentos, modens, canais de venda, geografia.
- **Bloco_2**: Log de incidentes de rede por equipamento OLT (~101 k linhas) — timestamps de abertura/encerramento, motivo, identificador do equipamento.

O objetivo é identificar padrões de churn, qualidade de serviço, confiabilidade da infraestrutura e oportunidades de melhoria operacional.

---

## 1. Preparação do ambiente e inventário de dados

1. Instalar dependências listadas em `requirements.txt`.
2. Rodar o inspetor de schema para mapear colunas, cobertura e tipos:
   ```bash
   python scripts/schema_inspector.py \
     --data-dir inputs \
     --sample-rows 50000 \
     --output schema_summary.md \
     --json-output schema_summary.json
   ```
3. Revisar `schema_summary.md` para identificar colunas com alta taxa de nulos, constantes ou de alta cardinalidade antes de qualquer análise.

---

## 2. Carregamento e integridade dos dados

1. Carregar `Bloco_1` com amostragem inicial (ex.: 200 k linhas) para verificação rápida; carregar completo para análises definitivas.
2. Carregar `Bloco_2` integralmente (8 MB).
3. Verificar:
   - Contagem de linhas e colunas por arquivo.
   - Tipos de dados inferidos vs. esperados (ex.: `mes_referencia` como string vs. período).
   - Duplicatas de chave primária (`codigo_cliente` + `mes_referencia` em Bloco_1; `olt_equipamento` + `inicio_evento` em Bloco_2).
   - Intervalo temporal coberto pelos dados.

---

## 3. Análise exploratória — Bloco_1 (Clientes)

### 3.1 Distribuições univariadas
- `preco_banda_larga`: histograma, boxplot — identificar clusters de preço e outliers.
- `velocidade_internet`: frequência por faixa de velocidade.
- `produtos` / `produto_banda_larga`: distribuição de mix de produtos.
- `otts`: cobertura e distribuição de serviços OTT contratados.
- `canal_venda`: volume e mix de canais.
- `estado` / `regiao_comercial`: distribuição geográfica.

### 3.2 Análise temporal
- Evolução mensal de `mes_referencia`: base ativa, entradas e saídas.
- Curva de permanência: tempo entre `data_entrada_base` e `data_cancelamento`.
- Sazonalidade de cancelamentos por mês/trimestre.

### 3.3 Análise de churn
- Taxa de cancelamento por `tipos_cancelamento` — quais motivos dominam?
- Churn por `velocidade_internet`, `preco_banda_larga`, `canal_venda`, `estado`.
- Perfil de clientes cancelados vs. ativos: preço médio, velocidade, tempo de base.

### 3.4 Qualidade dos dados
- Cobertura de `email_vendedor`, `id_tecnico_instalacao`, `bairro` — campos frequentemente incompletos em bases operacionais.
- Consistência de `marca_modem` / `modelo_modem` — padronização de nomes.
- Valores nulos em `data_cancelamento` (esperado para clientes ativos) vs. `tipos_cancelamento`.

---

## 4. Análise exploratória — Bloco_2 (Incidentes de Rede)

### 4.1 Distribuições univariadas
- `motivo_abertura`: frequência por tipo de incidente.
- `olt_equipamento`: ranking de OLTs com maior número de incidentes.

### 4.2 Análise de duração
- Calcular duração dos incidentes: `fim_evento - inicio_evento`.
- Distribuição de duração (histograma, percentis p50/p90/p99).
- Incidentes sem `fim_evento` (em aberto) — quantidade e equipamentos afetados.

### 4.3 Análise temporal
- Frequência de incidentes por hora do dia e dia da semana — identificar janelas críticas.
- Tendência mensal: volume de incidentes crescendo ou decrescendo?
- Identificar períodos de alta concentração de incidentes (possíveis eventos maiores).

### 4.4 Equipamentos críticos
- Top OLTs por volume de incidentes e por duração total acumulada.
- OLTs com incidentes recorrentes em janela curta (possível instabilidade crônica).

---

## 5. Análise cruzada Bloco_1 × Bloco_2

1. Fazer join via `olt_id` / `serial_olt` (Bloco_1) ↔ `olt_equipamento` (Bloco_2).
2. Avaliar se clientes em OLTs com alta taxa de incidentes têm maior taxa de cancelamento.
3. Medir o impacto de incidentes de longa duração no churn no mês seguinte.
4. Identificar regiões geográficas onde a qualidade de rede (incidentes) e o churn estão correlacionados.

---

## 6. Hipóteses prioritárias

| # | Hipótese | Métricas | Abordagem |
|---|----------|----------|-----------|
| H1 | OLTs com mais incidentes concentram maior churn | Taxa de churn por OLT vs. nº de incidentes | Correlação de Spearman, scatter plot |
| H2 | Clientes de entrada recente (< 3 meses) têm churn maior | Curva de sobrevivência por coorte de entrada | Kaplan-Meier ou tabela de coorte |
| H3 | Canais de venda diferem na qualidade do cliente retido | Churn por canal ao longo do tempo | ANOVA / teste qui-quadrado |
| H4 | Preço acima da mediana reduz churn (percepção de qualidade) | Churn por faixa de preço | Regressão logística simples |
| H5 | Incidentes noturnos têm maior impacto no churn que diurnos | Duração × horário × churn subsequente | Análise de correlação |

---

## 7. Priorização de variáveis para modelagem

1. Construir tabela analítica com unidade `codigo_cliente × mes_referencia`, incluindo features derivadas de incidentes (nº de incidentes no mês, duração total acumulada).
2. Calcular correlação com variável alvo (`churn_flag` derivada de `data_cancelamento`).
3. Executar feature importance com modelo baseline (Random Forest ou LightGBM com 5-fold CV).
4. Gerar ranking final com evidências estatísticas e gráficos de suporte.

---

## 8. Governança e documentação

1. Versionar notebooks, scripts e relatórios no repositório Git.
2. Registrar decisões de limpeza e transformações no próprio notebook (células markdown).
3. Preparar sumário executivo com:
   - Principais padrões identificados
   - Hipóteses confirmadas/refutadas
   - Top variáveis para modelagem de churn
   - Gaps de dados que exigem coleta adicional (ex.: dados de atendimento ao cliente, reclamações)

---

## 9. Próximos passos

1. Validar achados com times de negócio (Operações, Comercial, Engenharia de Rede).
2. Definir target de modelagem e período de observação (ex.: churn nos próximos 30/60/90 dias).
3. Construir pipeline de feature engineering a partir do schema validado.
4. Avaliar necessidade de dados complementares (ex.: histórico de atendimento, tickets de suporte, velocidade real medida vs. contratada).
