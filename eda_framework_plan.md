# Plano de EDA orientado ao framework proposto

Este documento descreve um roteiro detalhado para conduzir uma análise exploratória dos dados (EDA) alinhada ao framework fornecido, com foco em gerar hipóteses acionáveis para o time de Facebook (FB) e um ranking de variáveis prioritárias para o time de Data Science (DS). O plano foi desenhado para ser executado em Python, mas pode ser adaptado a outras linguagens.

## 1. Preparação do ambiente e inventário de dados
1. Identificar todas as fontes de dados relevantes (inscrições, eventos de captação, atributos escolares, indicações sociais, etc.).
2. Consolidar os dados em um repositório acessível (por exemplo, arquivos CSV ou um banco relacional) e documentar o dicionário de dados.
3. Rodar o script `python scripts/schema_inspector.py --data-dir data --output schema_summary.md` para mapear automaticamente os schemas disponíveis em `data/` e antecipar potenciais variáveis de modelagem.
4. Configurar um ambiente analítico (notebook Jupyter ou script Python) com as dependências essenciais:
   - `pandas`, `numpy`
   - Ferramenta de EDA automatizada (ex.: `ydata-profiling`, `sweetviz` ou `pandas-profiling`)
   - Bibliotecas de visualização (`matplotlib`, `seaborn`, `plotly`)
   - Opcional: `pm4py` para mineração de processos se houver logs de eventos.

## 2. Carregamento dos dados
1. Importar os datasets prioritários (inscritos, leads, campanhas, atributos de escolas, redes sociais, histórico de matérias/disciplinas, etc.).
2. Aplicar verificações de integridade (contagem de linhas, colunas, tipos de dados, datas válidas) e salvar amostras iniciais para conferência.

## 3. Geração automática de relatórios de EDA
1. Executar uma ferramenta de EDA automática (ex.: `ydata-profiling.ProfileReport`) para cada tabela principal e também para merges importantes (ex.: inscritos + campanhas + atributos da escola).
2. Armazenar os relatórios em HTML para fácil compartilhamento com stakeholders.
3. Revisar os alertas automáticos (valores constantes, ausências, distribuições enviesadas) e anotá-los em um log de achados preliminares.

## 4. Limpeza e padronização inicial
1. Tratar valores ausentes seguindo recomendações do relatório (por exemplo, imputar medianas em variáveis contínuas com baixa taxa de ausência, criar categoria "Desconhecido" para campos categóricos, descartar colunas com mais de 95% ausentes).
2. Padronizar categorias (ex.: normalizar nomes de escolas, consolidar variações em campos como "Gênero" ou "Tipo de Campanha").
3. Detectar outliers em variáveis críticas (ex.: investimento em campanhas, notas de disciplinas) utilizando IQR ou z-score, avaliando se devem ser capados, transformados ou apenas sinalizados.

## 5. Síntese automática das descobertas
1. Utilizar um assistente LLM (como GPT) para ler os relatórios HTML gerados e produzir um resumo em linguagem natural com os principais insights:
   - Variáveis com maior variabilidade e correlação com a meta (ex.: conversão, retenção).
   - Segmentos que se destacam (ex.: escolas do tipo XYZ, redes sociais específicas, turmas com indicações ABC).
   - Anomalias ou clusters sugeridos pela distribuição dos dados.
2. Registrar esses resumos em um documento compartilhado com FB e DS.

## 6. Exploração visual direcionada
1. Criar um notebook com templates de visualização reutilizáveis (histogramas, boxplots, scatterplots, heatmaps, séries temporais).
2. Gerar gráficos orientados às hipóteses de negócio:
   - **Hipótese FB 1:** Escolas do tipo XYZ precisam de campanhas presenciais → analisar taxa de conversão por tipo de escola e por proximidade geográfica das campanhas.
   - **Hipótese FB 2:** Inscritos influenciados pelo círculo social ABC → avaliar correlação entre canais de indicação e matérias/disciplinas escolhidas.
   - **Hipótese FB 3:** Matéria JJJJ como diferencial → comparar taxas de retenção/inscrição para alunos que escolhem JJJJ versus outros.
3. Documentar observações relevantes e possíveis ações (ex.: "Campanhas físicas aumentam a taxa de inscrição em escolas XYZ em +15%").

## 7. Priorização de variáveis para DS
1. Construir uma tabela agregada com o target de interesse (ex.: conversão, retenção, performance na matéria JJJJ).
2. Calcular métricas de importância preliminar:
   - Correlação (Pearson/Spearman) para variáveis contínuas.
   - Testes de hipótese (t-test, ANOVA, qui-quadrado) para variáveis categóricas.
   - Feature importance usando modelos baseline (ex.: árvore de decisão rasa, Random Forest com validação rápida).
3. Gerar um ranking das variáveis mais relevantes, justificando cada posição com evidências (estatísticas e gráficos) e destacando variáveis que merecem coleta/qualificação adicional.

## 8. Testes de hipóteses e análises específicas
1. Manter uma lista formal de hipóteses (como as fornecidas pelo time FB) em um quadro Kanban ou planilha.
2. Para cada hipótese, detalhar:
   - Dados necessários
   - Métrica/indicador de sucesso
   - Abordagem estatística sugerida (teste A/B, análise de variância, regressão, etc.)
   - Resultado preliminar (significância, tamanho de efeito, recomendações)
3. Automatizar, quando possível, a execução desses testes via scripts reutilizáveis.

## 9. Mineração de processos (opcional)
Se houver logs de eventos (ex.: jornada do inscrito, interações com campanha):
1. Utilizar `pm4py` para extrair métricas-chave (tempo de ciclo, gargalos, variantes de processo).
2. Correlacionar insights de processo com métricas de conversão/retenção para identificar estágios críticos.

## 10. Governança e documentação
1. Versionar notebooks, scripts e relatórios em repositório Git.
2. Registrar decisões de limpeza e transformações aplicadas.
3. Preparar um sumário executivo destacando:
   - Principais insights confirmados
   - Hipóteses com evidências para ações imediatas
   - Top variáveis para o time DS continuar a modelagem
   - Recomendações para coleta de dados complementar

## 11. Próximos passos
1. Validar os achados com stakeholders (FB, DS, marketing, equipe acadêmica).
2. Definir experimentos ou campanhas-piloto baseados nas hipóteses prioritárias.
3. Iniciar a etapa de modelagem ou aprofundamento analítico com base no ranking de variáveis.

---
Este roteiro garante que a EDA seja completa, colaborativa e orientada a hipóteses, acelerando a geração de conhecimento e facilitando a transição para as próximas fases do projeto.
