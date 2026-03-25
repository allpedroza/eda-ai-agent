## Resumo de Schemas

### Bloco_1_2026-03-23_18_31_06_2026_03_23.csv (50,000 linhas)
| Coluna | Tipo | Papel sugerido | Cobertura | Valores únicos | Exemplos | Observações |
| --- | --- | --- | --- | --- | --- | --- |
| codigo_cliente | int64 | numérica | 100.0% | 2624 | 1790403, 2976680, 2976629 |  |
| codigo_contrato_air | int64 | numérica | 100.0% | 2607 | 5666608, 5666559, 5666500 |  |
| mes_referencia | object | categórica | 100.0% | 14 | 2026-02, 2026-01, 2025-09 |  |
| data_entrada_base | object | categórica | 100.0% | 119 | 2024-06-04, 2024-06-03, 2024-06-05 |  |
| data_entrada_oferta | object | categórica | 100.0% | 408 | 2025-02-25, 2024-06-04, 2025-05-25 |  |
| data_cancelamento | object | desconhecido | 24.6% | 221 | 2026-03-14, 2025-06-30, 2026-01-31 |  |
| tipo_cancelamento | object | desconhecido | 24.6% | 3 | INVOLUNTARIO, VOLUNTARIO |  |
| produtos | object | desconhecido | 0.0% | 2 | Globoplay Premium |  |
| produto_banda_larga | object | categórica | 100.0% | 270 | 700Mega + Conta Outra Vez + Skeelo Ebook + Bebanca Revistas + SuperComics + Qualifica, 450Mega + Conta Outra Vez + Skeelo Minibook + Bebanca Revistas + SuperComics + Fitanywhere (89,99), 300Mega + Conta Outra Vez + Skeelo Minibook + Bebanca Revistas + SuperComics + Fitanywhere (89,99) |  |
| preco_banda_larga | float64 | numérica | 100.0% | 89 | 114.9, 109.99, 89.99 |  |
| velocidade_internet | float64 | id | 100.0% | 18 | 700.0, 450.0, 300.0 | possível identificador |
| otts | object | desconhecido | 5.7% | 47 | Globoplay , Globoplay , Globoplay, Globoplay , Globoplay, Globoplay , Globoplay, Globoplay  |  |
| estado | object | categórica | 100.0% | 12 | SP, PI, RJ |  |
| cidade | object | id | 100.0% | 207 | GUARUJA, GUARULHOS, TERESINA | possível identificador |
| bairro | object | categórica | 100.0% | 1459 | BALNEARIO PRAIA DO PEREQUE, JARDIM VIRGINIA, CIDADE PARQUE BRASILIA |  |
| regiao_comercial | object | desconhecido | 100.0% | 16 | R4, R3, R13 |  |
| olt_id | object | id | 95.2% | 564 | NIU-PRQ-GRJ-OHW-01, VIP-GRU-1-SPO-ONK-03, br.pi.tsa.cto.gp.01 | possível identificador |
| serial_olt | object | desconhecido | 95.2% | 2718 | 48575443A75064B4, ALCLFC107985, 48575443A5E71BAC |  |
| marca_modem | object | desconhecido | 58.9% | 5 | HUAWEI, FIBERHOME, ZTE |  |
| modelo_modem | object | desconhecido | 67.8% | 28 | EG8145V5-V2, G1425GA, HG6145F |  |
| canal_venda | object | desconhecido | 99.9% | 14 | LOJA, PAP INDIRETO, AGENTE DIGITAL |  |
| email_vendedor | object | desconhecido | 99.9% | 565 | waleria.santos@alloha.com, isabella.s.rs9@alloha.com, thiago.junior@mundiale.com.br |  |
| id_tecnico_instalacao | float64 | id | 99.0% | 987 | 3229.0, 3106.0, 4170.0 | possível identificador |

### Bloco_2_2026-03-23_18_31_06_2026_03_23.csv (50,000 linhas)
| Coluna | Tipo | Papel sugerido | Cobertura | Valores únicos | Exemplos | Observações |
| --- | --- | --- | --- | --- | --- | --- |
| olt_equipamento | object | categórica | 100.0% | 1064 | OLT-NK-SJM-01, OLT-FH-ARBU02-01, br.ce.fla.atn.gp.02 |  |
| motivo_abertura | object | categórica | 100.0% | 4 | Incidente de Rede, Manutenção Emergencial, Manutenção Programada |  |
| inicio_evento | object | categórica | 100.0% | 49983 | 2026-02-08, 17:22:20, 2026-03-18, 14:51:10, 2026-03-19, 01:33:19 | alta cardinalidade |
| fim_evento | object | desconhecido | 99.9% | 49666 | 2025-01-01, 17:09:49, 2025-01-01, 00:30:19, 2025-01-01, 00:34:09 |  |

### Bloco_4_2026-03-23_18_31_06_2026_03_23.csv (50,000 linhas)
| Coluna | Tipo | Papel sugerido | Cobertura | Valores únicos | Exemplos | Observações |
| --- | --- | --- | --- | --- | --- | --- |
| id_cliente | int64 | id | 100.0% | 30538 | 3067804, 3134482, 2021216 | possível identificador |
| id_ticket | int64 | id | 100.0% | 48830 | 39426094, 45068260, 47606303 | possível identificador |
| data_abertura | object | categórica | 100.0% | 48550 | 2025-03-13 10:51:55, 2025-06-30 14:44:23, 2025-08-21 13:47:57 | alta cardinalidade |
| data_conclusao | object | desconhecido | 87.0% | 39138 | 2025-03-14 04:09:22, 2025-07-01 03:06:16, 2025-08-21 13:52:27 |  |
| motivo | object | categórica | 100.0% | 7 | SUPORTE TÉCNICO, ANÁLISE INTERNA, CHD_ST |  |
| canal_atendimento | object | categórica | 100.0% | 6 | Humano, App, Ativo |  |
| status_ticket | object | categórica | 100.0% | 2 | Fechado, Aberto |  |
| status_contrato | object | categórica | 100.0% | 4 | ST_CONT_CANCELADO, ST_CONT_SUSP_DEBITO, ST_CONT_SUSP_SOLICITACAO |  |
| prazo_resolucao | float64 | numérica | 87.0% | 109 | 1.0, 0.0, 6.0 |  |
| qtd_chamados_financeiros_90d | int64 | numérica | 100.0% | 21 | 0, 2, 4 |  |
| flag_rechamada_voz_24h | int64 | numérica | 100.0% | 2 | 0, 1 | binária |
| flag_rechamada_voz_7d | int64 | numérica | 100.0% | 2 | 0, 1 | binária |
| total_interacoes_voz | int64 | numérica | 100.0% | 44 | 0, 1, 3 |  |

### Bloco_5_2026-03-23_18_31_06_2026_03_23.csv (50,000 linhas)
| Coluna | Tipo | Papel sugerido | Cobertura | Valores únicos | Exemplos | Observações |
| --- | --- | --- | --- | --- | --- | --- |
| id_cliente | int64 | id | 100.0% | 13346 | 221594, 290580, 173526 | possível identificador |
| mes_referencia | object | categórica | 100.0% | 14 | 2019-00, 2020-00, 2021-00 |  |
| valor_fatura | float64 | numérica | 99.0% | 9739 | 147.3, 89.9, 150.82 |  |
| data_vencimento | object | categórica | 100.0% | 1560 | 2019-10-27, 2019-12-05, 2020-02-10 |  |
| data_pagamento | object | desconhecido | 80.3% | 1338 | 2021-03-25, 2021-05-27, 2022-02-14 |  |
| dias_atraso | int64 | numérica | 100.0% | 1390 | 2339, 2300, 2233 |  |
| status_pagamento | object | categórica | 100.0% | 3 | não pago/inadimplente, pago em atraso, pago |  |
| flag_mudanca_plano | object | categórica | 100.0% | 1 | não |  |

### Variáveis candidatas para modelagem

| Variável | Papel | Cobertura | Valores únicos |
| --- | --- | --- | --- |

| Bloco_1_2026-03-23_18_31_06_2026_03_23.csv::bairro | categórica | 100.0% | 1459 |

| Bloco_1_2026-03-23_18_31_06_2026_03_23.csv::codigo_cliente | numérica | 100.0% | 2624 |

| Bloco_1_2026-03-23_18_31_06_2026_03_23.csv::codigo_contrato_air | numérica | 100.0% | 2607 |

| Bloco_1_2026-03-23_18_31_06_2026_03_23.csv::data_entrada_base | categórica | 100.0% | 119 |

| Bloco_1_2026-03-23_18_31_06_2026_03_23.csv::data_entrada_oferta | categórica | 100.0% | 408 |

| Bloco_1_2026-03-23_18_31_06_2026_03_23.csv::estado | categórica | 100.0% | 12 |

| Bloco_1_2026-03-23_18_31_06_2026_03_23.csv::mes_referencia | categórica | 100.0% | 14 |

| Bloco_1_2026-03-23_18_31_06_2026_03_23.csv::preco_banda_larga | numérica | 100.0% | 89 |

| Bloco_1_2026-03-23_18_31_06_2026_03_23.csv::produto_banda_larga | categórica | 100.0% | 270 |

| Bloco_2_2026-03-23_18_31_06_2026_03_23.csv::inicio_evento | categórica | 100.0% | 49983 |

| Bloco_2_2026-03-23_18_31_06_2026_03_23.csv::motivo_abertura | categórica | 100.0% | 4 |

| Bloco_2_2026-03-23_18_31_06_2026_03_23.csv::olt_equipamento | categórica | 100.0% | 1064 |

| Bloco_4_2026-03-23_18_31_06_2026_03_23.csv::canal_atendimento | categórica | 100.0% | 6 |

| Bloco_4_2026-03-23_18_31_06_2026_03_23.csv::data_abertura | categórica | 100.0% | 48550 |

| Bloco_4_2026-03-23_18_31_06_2026_03_23.csv::flag_rechamada_voz_24h | numérica | 100.0% | 2 |

| Bloco_4_2026-03-23_18_31_06_2026_03_23.csv::flag_rechamada_voz_7d | numérica | 100.0% | 2 |

| Bloco_4_2026-03-23_18_31_06_2026_03_23.csv::motivo | categórica | 100.0% | 7 |

| Bloco_4_2026-03-23_18_31_06_2026_03_23.csv::qtd_chamados_financeiros_90d | numérica | 100.0% | 21 |

| Bloco_4_2026-03-23_18_31_06_2026_03_23.csv::status_contrato | categórica | 100.0% | 4 |

| Bloco_4_2026-03-23_18_31_06_2026_03_23.csv::status_ticket | categórica | 100.0% | 2 |

| Bloco_4_2026-03-23_18_31_06_2026_03_23.csv::total_interacoes_voz | numérica | 100.0% | 44 |

| Bloco_5_2026-03-23_18_31_06_2026_03_23.csv::data_vencimento | categórica | 100.0% | 1560 |

| Bloco_5_2026-03-23_18_31_06_2026_03_23.csv::dias_atraso | numérica | 100.0% | 1390 |

| Bloco_5_2026-03-23_18_31_06_2026_03_23.csv::flag_mudanca_plano | categórica | 100.0% | 1 |

| Bloco_5_2026-03-23_18_31_06_2026_03_23.csv::mes_referencia | categórica | 100.0% | 14 |

| Bloco_5_2026-03-23_18_31_06_2026_03_23.csv::status_pagamento | categórica | 100.0% | 3 |

| Bloco_5_2026-03-23_18_31_06_2026_03_23.csv::valor_fatura | numérica | 99.0% | 9739 |

| Bloco_4_2026-03-23_18_31_06_2026_03_23.csv::prazo_resolucao | numérica | 87.0% | 109 |
