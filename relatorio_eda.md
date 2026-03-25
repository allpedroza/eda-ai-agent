# Relatório de EDA — Dados de Telecom (Banda Larga)

**Data de geração:** 2026-03-24
**Arquivos analisados:** Bloco_1 (3,9M linhas), Bloco_2 (101k linhas), Bloco_4 (9,5M linhas), Bloco_5 (17,3M linhas)
**Ferramenta:** `uv run python scripts/eda_full.py` + `uv run python scripts/eda_por_olt.py`

---

## Sumário Executivo

A base cobre uma operadora de banda larga com presença em 12 estados brasileiros e ~1.024 OLTs ativas. A análise revelou dois perfis distintos de risco que demandam ações diferentes:

- **Risco operacional:** OLTs com alta frequência e duração de incidentes de rede, gerando volumes anômalos de tickets de atendimento — problema de infraestrutura.
- **Risco socioeconômico:** OLTs com churn e inadimplência estruturalmente altos, independente da qualidade de rede — problema de estratégia comercial e perfil do público captado.

A correlação entre incidentes de rede e churn é surpreendentemente fraca (Spearman = -0,08), indicando que a qualidade de rede **não é o principal driver de cancelamento** na média do portfólio. O churn é mais explicado pelo canal de venda e pela região geográfica do que pela infraestrutura.

---

## 1. Bloco 1 — Base de Clientes e Contratos

### 1.1 Visão Geral

| Métrica | Valor |
|---|---|
| Total de registros | 3.903.444 |
| Período coberto | Jan/2025 – Fev/2026 |
| Estados ativos | 12 (SP, CE, RJ, ES, MG, MS, DF, SE, PR, PI, PE, MA) |
| OLTs distintas | 1.024 |
| Registros sem OLT | 188.495 (4,8%) |

> **Anomalia:** `mes_referencia` contém o valor `'R9'` — dado inválido que deve ser investigado na origem.

### 1.2 Preço e Velocidade

**Preço banda larga:**

| Faixa (R$) | % clientes |
|---|---|
| 90–110 | 42,8% |
| 70–90 | 29,6% |
| 110–130 | 18,6% |
| < 70 | 5,7% |
| 130–200 | 2,7% |
| > 200 | 0,0% (47 registros — possíveis erros) |

- Preço médio: **R$ 100,95** | Mediana: **R$ 99,99**
- Mínimo de R$ 0,00 e máximo de R$ 2.476 — outliers que precisam de validação

**Velocidade (Mbps):**

| Velocidade | % clientes |
|---|---|
| 700 Mbps | 28,0% |
| 800 Mbps | 17,4% |
| 500 Mbps | 11,8% |
| 300 Mbps | 10,7% |
| 450 Mbps | 9,6% |
| 600 Mbps | 9,1% |
| 1024 Mbps | 6,0% |
| ≤ 100 Mbps | 2,8% |

### 1.3 Churn

- **Taxa geral de churn:** 27,7% dos registros (18,6% involuntário, 9,1% voluntário)
- Churn involuntário é **2× maior** que voluntário — inadimplência e suspensão são o mecanismo dominante de saída

**Por canal de venda:**

| Canal | Churn% | Observação |
|---|---|---|
| Marketing | 37,1% | Base pequena (89 reg.) |
| PAP Indireto | 34,0% | Maior volume absoluto (61k reg.) |
| Chatbot | 33,4% | |
| PAP FTTH | 33,2% | |
| WhatsApp Orgânico | 26,1% | 2º maior volume (44k reg.) |
| Agente Digital | 25,8% | |
| Loja | 23,7% | |
| Receptivo | 21,8% | |
| Indique um Amigo | 14,4% | Melhor qualidade de cliente |
| SEM ALOCAÇÃO | 0,0% | Sem cancelamentos registrados — verificar |

> PAP Indireto concentra 30,5% da base e 34% de churn — canal de maior risco e maior volume simultaneamente.

**Por estado:**

| Estado | Churn% |
|---|---|
| PE | **49,2%** |
| MA | 37,3% |
| SE | 37,0% |
| PI | 35,3% |
| MS | 33,6% |
| DF | 31,4% |
| CE | 28,5% |
| SP | 27,3% |
| RJ | 23,2% |
| PR | **21,8%** |

> PE, MA e SE têm churn quase o dobro dos estados mais maduros da base (RJ, PR).

**Por velocidade:**

| Velocidade (Mbps) | Churn% | Observação |
|---|---|---|
| 80 | **100%** | Apenas 70 registros — produto descontinuado? |
| 300 | 40,8% | |
| 450 | 39,5% | |
| 600 | 36,6% | |
| 700 | 26,3% | |
| 800 | 26,1% | |
| 500 | 21,6% | |
| 1024 | 20,8% | |
| 400 | 9,8% | |
| 50 | 0,0% | Produto URBE — sem cancelamentos |

> Planos de entrada (300–600 Mbps) têm churn estruturalmente maior. Velocidades mais altas retêm melhor.

**Por faixa de preço:**

| Faixa (R$) | Churn% | Observação |
|---|---|---|
| 70–90 | **35,3%** | Pior faixa — contra-intuitivo |
| 110–130 | 28,0% | |
| 90–110 | 24,5% | |
| 130–150 | 26,3% | |
| < 70 | 16,9% | |
| 150–200 | 18,5% | |

> A faixa 70–90 tem o maior churn apesar de não ser a mais barata. Provavelmente corresponde a planos de entrada em regiões de maior inadimplência captados por PAP Indireto.

### 1.4 Qualidade dos Dados — Alertas

| Coluna | Problema | Impacto |
|---|---|---|
| `produtos` | 100% nulos na prática | Coluna inutilizável |
| `otts` | 94,3% nulos | Baixa utilidade |
| `marca_modem` | 45% nulos | Lacuna operacional |
| `modelo_modem` | 32% nulos | Lacuna operacional |
| Todas as datas | Tipo `object` (string) | Requerem parse antes de modelagem |
| `codigo_cliente` | Classificado como numérico | É um identificador |
| `mes_referencia` | Valor `'R9'` presente | Dado inválido na origem |

---

## 2. Bloco 2 — Incidentes de Rede

### 2.1 Visão Geral

| Métrica | Valor |
|---|---|
| Total de incidentes | 101.408 |
| Período | Jan/2025 – Mar/2026 |
| OLTs com incidentes | 1.079 |
| Incidentes sem encerramento | 67 (0,1%) |

### 2.2 Motivos de Abertura

| Motivo | % |
|---|---|
| Incidente de Rede | **98,6%** |
| Manutenção Emergencial | 1,2% |
| Incidente sem Acionamento | 0,1% |
| Manutenção Programada | 0,1% |

> Granularidade muito baixa — 98,6% em uma categoria só. O campo `motivo_abertura` tem pouco valor analítico sem subcategorização.

### 2.3 Duração dos Incidentes

| Métrica | Valor |
|---|---|
| Duração mediana | **13 min** |
| Duração média | 4,15h |
| Duração máxima | 2.270h (~94 dias) |
| Incidentes > 24h | 3.098 (3,1%) |
| Incidentes > 72h | 468 (0,5%) |
| Durações negativas | 0 |

> A distribuição é extremamente assimétrica: a maioria dos incidentes é curta, mas a cauda de eventos longos acumula a maior parte do impacto sobre o cliente.

### 2.4 Tendência Temporal

| Período | Incidentes |
|---|---|
| Jan–Jun/2025 | ~4.000–6.000/mês |
| Jul–Dez/2025 | **crescimento contínuo**, pico em Nov/2025 (11.352) |
| Jan–Mar/2026 | ~7.800–4.900/mês (queda ou base menor) |

> Volume de incidentes quase dobrou ao longo de 2025 — possível expansão de rede sem proporcional maturação da infraestrutura.

### 2.5 Padrão Horário

- Pico de incidentes entre **10h–17h** (janela comercial)
- Mínimo entre 3h–5h (madrugada)
- Sugere que **manutenções e intervenções humanas** são a principal causa, não falhas espontâneas

### 2.6 OLTs com Maior Indisponibilidade Acumulada

| OLT | Incidentes | Indisp. Total (h) | Incid. > 24h |
|---|---|---|---|
| `br.ce.fla.mtb.gp.04` | 67 | **8.343** | 15 |
| `OLT-FH-SPA01-03` | 328 | 8.038 | 33 |
| `OLT-FH-SPA01-02` | 261 | 8.018 | 15 |
| `VIP-SZN-SPO-OHW-02` | 1.025 | 6.160 | 70 |
| `VIP-SZN-SPO-OHW-01` | 1.295 | 5.775 | 78 |
| `OLT-FH-UNAR05-01` | 120 | 5.631 | 9 |

> `br.ce.fla.mtb.gp.04` é o outlier crítico: apenas 67 incidentes, mas duração média de **124h por evento** — praticamente 5 dias por ocorrência.

---

## 3. Bloco 4 — Tickets de Atendimento

### 3.1 Visão Geral

| Métrica | Valor |
|---|---|
| Total de tickets | 9.524.820 |
| Período | Jan/2025 – Mar/2026 |
| Cobertura `data_conclusao` | 87% (13% sem encerramento) |

### 3.2 Composição dos Tickets

**Por motivo:**

| Motivo | % |
|---|---|
| Suporte Técnico | 71,4% |
| CHD_ST | 17,2% |
| Financeiro | 5,7% |
| Análise Interna | 4,7% |

**Por canal:**

| Canal | % |
|---|---|
| Humano | **97,8%** |
| Ativo | 1,2% |
| App | 0,5% |

> 97,8% dos atendimentos são por canal humano — automação praticamente inexistente.

**Por status do contrato:**

| Status | % | Observação |
|---|---|---|
| ST_CONT_CANCELADO | **86,5%** | Base de tickets majoritariamente pós-churn |
| ST_CONT_SUSP_DEBITO | 13,0% | Suspensão por inadimplência |

> 86,5% dos tickets são de contratos já cancelados — a operação de atendimento está concentrada em cobrança/retenção pós-cancelamento, não em prevenção.

### 3.3 Prazo de Resolução

| Métrica | Valor |
|---|---|
| Mediana | 1 dia |
| Média | 1,8 dias |
| Máximo | 400 dias |
| Resolvidos no dia | 23,9% |
| Prazo > 7 dias | 3,9% |
| Prazo > 30 dias | 0,4% |

**Por canal:**

| Canal | Mediana (dias) | Observação |
|---|---|---|
| Ativo | **7,0** | 7× acima da mediana geral |
| App | 1,0 | |
| Humano | 1,0 | |

**Por motivo:**

| Motivo | Mediana (dias) |
|---|---|
| Análise Interna | 0 |
| Financeiro | 0 |
| Suporte Técnico | **1** |
| CHD_ST | 1 |

### 3.4 Rechamadas e Reincidência

- **Rechamada em 24h:** 3,8% dos tickets — cliente precisou ligar novamente no dia seguinte
- **Rechamada em 7 dias:** 4,2% — resolução de 1º contato falhou
- **OLTs com > 20 tickets por cliente** sugerem problemas crônicos não resolvidos

### 3.5 Tendência de Volume

| Período | Tickets/mês |
|---|---|
| Jan/2025 | 19.227 |
| Jun/2025 | 14.458 |
| Dez/2025 | 10.283 |
| Mar/2026 | 5.558 |

> Volume caindo 71% entre jan/25 e mar/26. Pode indicar melhora operacional, mas provavelmente reflete redução da base ativa por churn.

---

## 4. Bloco 5 — Faturas e Pagamentos

### 4.1 Visão Geral

| Métrica | Valor |
|---|---|
| Total de registros | 17.288.575 |
| Clientes únicos | estimado > 500k |
| Cobertura `valor_fatura` | 99,1% |
| Cobertura `data_pagamento` | 80,6% |

> **Anomalia crítica:** `mes_referencia` usa formato `'YYYY-00'` (mês 00) em todos os registros — valor inválido. Provavelmente um bug na geração do arquivo.

### 4.2 Status de Pagamento

| Status | % |
|---|---|
| Pago | 43,5% |
| Pago em atraso | 37,1% |
| Não pago / Inadimplente | **19,4%** |

> Apenas **43,5% das faturas foram pagas em dia**. Somando inadimplentes + atraso, quase **57% das faturas têm algum grau de problema**.

### 4.3 Valor da Fatura

| Métrica | Valor |
|---|---|
| Média | R$ 106,11 |
| Mediana | R$ 99,99 |
| Mínimo | **-R$ 1.099** (crédito/estorno) |
| Máximo | **R$ 60.000** (verificar) |
| Desvio padrão | R$ 268 — alta variância |

**Inadimplência por faixa:**

| Faixa (R$) | Inadimplência% | Observação |
|---|---|---|
| > 200 | **83,4%** | Provável cobrança de multa/rescisão |
| < 50 | 52,8% | Planos de entrada muito sensíveis ao preço |
| 50–90 | 20,2% | |
| 150–200 | 20,0% | |
| 90–110 | 13,2% | Menor inadimplência na faixa principal |
| 110–130 | 8,8% | |

> Faturas acima de R$ 200 têm inadimplência de 83,4% — são provavelmente cobranças de rescisão/multa que o cliente já não tem intenção de pagar.

### 4.4 Dias de Atraso

| Métrica | Valor |
|---|---|
| Mediana | 2 dias |
| Média | 133 dias |
| Máximo | **5.755 dias** (~15 anos) |
| Atraso > 365 dias | 12,5% das faturas |
| Atraso > 1.000 dias | 4,4% das faturas |

> A presença de atrasos acima de 1.000 dias indica que a base contém **carteira histórica irrecuperável** que distorce as métricas. Recomenda-se segmentar a análise por vintage.

### 4.5 Alertas de Qualidade

| Coluna | Problema |
|---|---|
| `mes_referencia` | Formato `'YYYY-00'` — mês inválido em 100% dos registros |
| `flag_mudanca_plano` | Constante (`'não'`) em toda a amostra — sem valor preditivo |
| `dias_atraso` | Valores até 5.755 dias — mistura de carteira ativa e histórica |
| `valor_fatura` | Negativos (estornos) e R$ 60k (outlier) sem tratamento |

---

## 5. Análise por OLT — Concentrações

### 5.1 Score de Risco Composto

Score calculado pela soma de 5 métricas normalizadas (0–1 cada): churn%, incidentes, indisponibilidade, tickets/cliente e % inadimplentes. OLTs com mínimo de 50 clientes únicos.

**Top 10 OLTs por risco composto:**

| OLT | Clientes | Churn% | Incidentes | Indisp.(h) | Tickets/cli. | Inadimp.% | Score |
|---|---|---|---|---|---|---|---|
| `VIP-SZN-SPO-OHW-02` | 438 | 28,7% | 1.025 | 6.160 | 20,7x | 41,8% | **2,95** |
| `VIP-SZN-SPO-OHW-01` | 499 | 27,0% | 1.295 | 5.775 | 15,6x | 44,4% | **2,91** |
| `VIP-GRU-2-SPO-ONK-02` | 359 | 29,8% | 865 | 3.338 | 18,0x | 52,7% | **2,56** |
| `br.ce.fla.mtb.gp.04` | 89 | 33,6% | 67 | 8.343 | 10,4x | 66,1% | **2,49** |
| `NIU-JVR-RPO-OFH-02` | 70 | 58,3% | 111 | 725 | 15,0x | 78,6% | **2,46** |
| `NIU-JVR-RPO-OFH-01` | 182 | 54,8% | 171 | 1.457 | 13,2x | 78,6% | **2,45** |
| `NIU-RBV-RPO-OFH-01` | 54 | 55,3% | 130 | 750 | 14,1x | 83,3% | **2,45** |
| `OLT-FH-SPA01-03` | 846 | 31,6% | 328 | 8.038 | 14,4x | 37,3% | **2,37** |
| `VIP-PAL-SPO-OHW-01` | 608 | 21,5% | 769 | 4.942 | 18,7x | 39,7% | **2,36** |
| `NIU-PBR-RPO-OFH-01` | 130 | 55,7% | 127 | 715 | 9,3x | 79,7% | **2,20** |

### 5.2 Correlações entre Métricas por OLT (Spearman, n=517 OLTs)

|  | Churn% | Incidentes | Indisp.(h) | Tickets/cli | Inadimp.% | Preço | Velocidade |
|---|---|---|---|---|---|---|---|
| **Churn%** | 1,00 | -0,08 | -0,10 | -0,16 | 0,02 | 0,18 | 0,19 |
| **Incidentes** | -0,08 | 1,00 | **0,81** | 0,23 | -0,01 | 0,24 | 0,24 |
| **Indisp.(h)** | -0,10 | **0,81** | 1,00 | 0,19 | 0,00 | 0,22 | 0,21 |
| **Tickets/cli** | -0,16 | 0,23 | 0,19 | 1,00 | **-0,40** | 0,35 | 0,31 |
| **Inadimp.%** | 0,02 | -0,01 | 0,00 | -0,40 | 1,00 | **-0,63** | **-0,52** |

**Interpretações-chave:**
- `incidentes ↔ indisponibilidade`: **0,81** — esperado, as duas métricas de rede caminham juntas
- `inadimplência ↔ preço`: **-0,63** — OLTs com planos mais baratos têm inadimplência muito maior; preço é proxy do perfil socioeconômico do cliente
- `inadimplência ↔ tickets/cliente`: **-0,40** — clientes inadimplentes abrem menos chamados (já desistiram do serviço)
- `churn ↔ incidentes`: **-0,08** — correlação **quase nula**: qualidade de rede não explica churn no nível de OLT

### 5.3 Duplo Risco Operacional — Alto Churn + Alta Indisponibilidade (23 OLTs)

OLTs acima do percentil 75 simultaneamente em churn% e indisponibilidade — problema de infraestrutura somado a perfil de cliente frágil:

| OLT | Clientes | Churn% | Indisp.(h) | Inadimp.% |
|---|---|---|---|---|
| `VIP-SZN-SPO-OHW-02` | 438 | 28,7% | 6.160 | 41,8% |
| `VIP-GRU-2-SPO-ONK-02` | 359 | 29,8% | 3.338 | 52,7% |
| `br.ce.fla.mtb.gp.04` | 89 | 33,6% | 8.343 | 66,1% |
| `NIU-JVR-RPO-OFH-02` | 70 | 58,3% | 725 | 78,6% |
| `NIU-JVR-RPO-OFH-01` | 182 | 54,8% | 1.457 | 78,6% |
| `OLT-FH-SPA01-03` | 846 | 31,6% | 8.038 | 37,3% |
| `OLT-FH-UNAR05-01` | 234 | 27,6% | 5.631 | 47,2% |
| `OLT-NK-AMA02-03` | 200 | 27,7% | 3.532 | 43,1% |

### 5.4 Duplo Risco Socioeconômico — Alto Churn + Alta Inadimplência (39 OLTs)

OLTs com perfil de cliente mais vulnerável — independente da qualidade de rede:

| OLT | Clientes | Churn% | Inadimp.% | Atraso Mediano (dias) |
|---|---|---|---|---|
| `br.ce.cuc.*` (5 OLTs) | 24–262 | ~30% | **83–91%** | **445** |
| `NIU-JVR-RPO-OFH-01/02` | 70–182 | 54–58% | 78,6% | 432 |
| `NIU-RBV-RPO-OFH-01` | 54 | 55,3% | 83,3% | 419 |
| `NIU-PBR-RPO-OFH-01` | 130 | 55,7% | 79,7% | 390 |
| `br.pe.pui.mob.gp.01` | 76 | 47,4% | 83,7% | 315 |
| `br.se.lat.mob.gp.01` | 83 | 50,2% | 73,1% | — |
| `br.pe.goi.pie.gp.01` | 97 | 35,4% | 76,3% | 399 |

### 5.5 Inadimplência Estrutural — Cluster `br.ce.cuc.*`

5 OLTs na região de Caucaia (CE) com perfil crítico:

| OLT | Clientes | Inadimp.% | Atraso Mediano | Fatura Média |
|---|---|---|---|---|
| `br.ce.cuc.pcc.gp.01` | 34 | **91,2%** | 445 dias | R$ 68,70 |
| `br.ce.cuc.ara.gp.03` | 252 | 87,3% | 445 dias | R$ 61,40 |
| `br.ce.cuc.sol.gp.01` | 140 | 85,7% | 445 dias | R$ 62,50 |
| `br.ce.cuc.cto.gp.03` | 93 | 83,9% | 445 dias | R$ 69,80 |
| `br.ce.cuc.ara.gp.02` | 262 | 83,2% | 445 dias | R$ 73,90 |

> Atraso mediano de **445 dias = ~1,2 anos** em toda a região. Com fatura média abaixo de R$ 75, essa carteira está essencialmente perdida. Recomenda-se avaliar baixa contábil ou venda de carteira.

### 5.6 OLTs com Maior Demanda de Atendimento por Cliente

| OLT | Clientes c/ ticket | Tickets/cliente | Ticket aberto% |
|---|---|---|---|
| `OLT_NK_CPE_B2` | 28 | **25,8x** | 67,9% |
| `OLT-FH-COLG01-01` | 75 | 25,8x | 70,7% |
| `OLT-UNIVOX-FH-SSP-003` | 55 | 25,5x | 74,6% |
| `NIU-SCP-PGD-OZT-01` | 57 | 24,9x | 84,2% |
| `VIP-SZN-SPO-OHW-02` | 212 | 20,7x | 67,0% |

> Em OLTs normais a relação tickets/cliente gira em torno de 3–5x. Valores acima de 20x indicam problema crônico não resolvido.

---

## 6. Perfis de Risco por Cluster de OLT

### Perfil A — Risco Operacional Puro
**Exemplos:** `VIP-SZN-*`, `OLT-FH-SPA01-*`, `VIP-PAL-*`
**Características:** alto volume de incidentes, longa indisponibilidade, muitos tickets por cliente, churn e inadimplência em nível moderado
**Diagnóstico:** problema de infraestrutura — rede instável gerando cascata de atendimentos
**Ação sugerida:** prioridade de manutenção preventiva, SLA de resolução de incidentes, automação de atendimento para esse perfil de chamado

### Perfil B — Risco Socioeconômico Puro
**Exemplos:** `br.ce.cuc.*`, `br.pe.pui.*`, `br.se.lat.*`
**Características:** churn > 45%, inadimplência > 75%, atraso mediano > 300 dias, fatura baixa
**Diagnóstico:** captação em regiões/públicos com baixa capacidade de pagamento — provavelmente via PAP Indireto
**Ação sugerida:** revisão da política comercial por região, análise do canal de venda que originou esses clientes, avaliação de baixa contábil da carteira mais antiga

### Perfil C — Duplo Risco (Operacional + Socioeconômico)
**Exemplos:** `NIU-JVR-RPO-OFH-01/02`, `NIU-RBV-RPO-OFH-01`, `br.ce.fla.mtb.gp.04`
**Características:** churn > 50%, inadimplência > 70%, rede com incidentes frequentes ou longos
**Diagnóstico:** expansão agressiva em área nova com infraestrutura imatura e público vulnerável
**Ação sugerida:** congelamento de novas vendas na área até estabilização da rede; análise de viabilidade econômica do cluster

---

## 7. Hipóteses para Investigação Continuada

| # | Hipótese | Evidência atual | Próximo passo |
|---|---|---|---|
| H1 | Canal PAP Indireto origina clientes com maior churn e inadimplência | Churn 34% vs. 14% (Indique Amigo); inadimplência alta nas OLTs captadas por PAP | Rastrear canal de venda até OLT e calcular LTV por canal |
| H2 | Incidentes de rede não causam churn diretamente | Correlação OLT-level churn × incidentes = -0,08 | Análise em nível de cliente com janela temporal (incidente → churn nos 30 dias seguintes) |
| H3 | A inadimplência de Caucaia é carteira morta, não flutuação | Atraso mediano de 445 dias + fatura < R$ 75 | Análise de vintage: quando esses clientes foram captados e quando pararam de pagar |
| H4 | Clientes de planos 300–450 Mbps têm menor retenção estrutural | Churn 40% vs. 21% para 1024 Mbps | Análise de sobrevivência (Kaplan-Meier) por plano de velocidade |
| H5 | OLTs com > 20 tickets/cliente têm problema crônico, não pontual | Pct. tickets abertos > 65% nessas OLTs | Série temporal de tickets por OLT para identificar início do problema |

---

## 8. Lacunas de Dados Identificadas

| Lacuna | Impacto |
|---|---|
| `produtos` 100% nulo | Não é possível analisar cross-sell / bundle |
| `otts` 94% nulo | Impossível avaliar impacto de serviços OTT na retenção |
| `marca_modem` 45% nulo | Não é possível correlacionar hardware com qualidade de serviço |
| `flag_mudanca_plano` constante | Mudanças de plano não estão sendo registradas |
| `mes_referencia` (B5) com mês 00 | Impossível análise temporal confiável de inadimplência |
| Ausência de velocidade real medida | Não há como comparar velocidade contratada vs. entregue |
| Ausência de dados de atendimento presencial | Jornada do cliente incompleta |
| Bloco 3 ausente | Há salto de Bloco 2 para Bloco 4 — qual dado está faltando? |

---

## 9. Variáveis Prioritárias para Modelagem de Churn

| Variável | Fonte | Tipo | Relevância |
|---|---|---|---|
| `tipo_cancelamento` | B1 | Target | Derivar flag binária de churn |
| `canal_venda` | B1 | Categórica | Alta discriminação entre perfis de cliente |
| `velocidade_internet` | B1 | Numérica | Forte correlação com taxa de retenção |
| `preco_banda_larga` | B1 | Numérica | Proxy de produto e perfil socioeconômico |
| `estado` | B1 | Categórica | Grandes variações regionais de churn |
| `olt_id` | B1 | Categórica | Captura efeitos de rede e região simultaneamente |
| `tempo_na_base` (derivada) | B1 | Numérica | `mes_referencia - data_entrada_base` |
| `duracao_total_incidentes_olt` (derivada) | B1+B2 | Numérica | Qualidade de rede na OLT do cliente |
| `n_incidentes_olt_30d` (derivada) | B1+B2 | Numérica | Incidentes recentes como sinal de instabilidade |
| `n_tickets_30d` (derivada) | B1+B4 | Numérica | Demanda de atendimento como sinal de insatisfação |
| `pct_faturas_em_atraso` (derivada) | B1+B5 | Numérica | Histórico de pagamento como preditor de churn involuntário |
| `dias_atraso_max` (derivada) | B5 | Numérica | Profundidade da inadimplência |
| `qtd_chamados_financeiros_90d` | B4 | Numérica | Pressão financeira do cliente |

---

*Relatório gerado com base em análise exploratória completa dos 4 blocos de dados. Scripts em `scripts/eda_full.py` e `scripts/eda_por_olt.py`. Execução via `uv run`.*
