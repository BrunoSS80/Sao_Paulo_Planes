---
name: "Sao Paulo Planes Medallion Architecture"
description: "Use when working on the Sao_Paulo_Planes data pipeline, OpenSky, Kafka, PySpark, Parquet, notebooks, or the bronze, silver, and gold layers. Explains the project flow and routes layer-specific work to the equivalent skill."
---

# Sao Paulo Planes: arquitetura medalhao

## Visao geral

Este projeto coleta estados de aeronaves na regiao de Sao Paulo usando a API OpenSky, publica os eventos em Kafka e processa os dados com PySpark em uma arquitetura medalhao:

```text
OpenSky API -> producer.py -> Kafka (plane_data) -> bronze.py -> data/bronze/output
                                                    -> silver.py -> data/silver/output
                                                    -> gold (a implementar) -> data/gold/<mart>
```

O `docker-compose.yml` fornece Zookeeper, Kafka, Kafdrop e um ambiente Jupyter com Spark. Dentro da rede Docker, os clientes Kafka usam `kafka:29092`; fora dela, o broker e anunciado em `localhost:9092`. O produtor usa as credenciais `ClientId` e `ClientSecret` carregadas do `.env`.

## Componentes e responsabilidades

- `src/producer.py` consulta a OpenSky a cada 30 segundos para a caixa geografica configurada, serializa o timestamp e a lista `states`, e envia o payload ao topico Kafka `plane_data`. Nao coloque regras de limpeza ou metricas neste componente.
- `src/bronze.py` consome `plane_data` via Structured Streaming, interpreta o JSON com schema explicito, achata cada item de `states` e grava Parquet em `data/bronze/output` no modo `append`, com checkpoint em `data/bronze/checkpoint` e trigger de 35 segundos.
- `src/silver.py` le o Parquet Bronze em streaming usando o schema obtido de uma leitura estatica, remove duplicatas por `icao24` e `last_contact`, remove registros sem `callsign`, converte `time` de epoch para `timestamp` e cria `year`, `month` e `day`. Grava Parquet particionado por essas colunas em `data/silver/output`, com checkpoint em `data/silver/checkpoint`.
- `notebooks/bronze.ipynb` e `notebooks/silver.ipynb` sao superficies exploratorias e de validacao do mesmo fluxo. Ao corrigir o pipeline, prefira os scripts em `src/` e atualize notebooks apenas quando a documentacao executavel precisar acompanhar a mudanca.
- A camada Gold ainda nao possui scripts em `src/gold/`. Quando for criada, deve consumir Silver em batch e produzir marts Parquet independentes em `data/gold/`, conforme o contrato da skill Gold.

## Contratos entre camadas

- Bronze recebe o payload da fonte e e a zona de landing. O dado deve permanecer o mais proximo possivel da origem, com persistencia append-only, checkpoint e metadados de ingestao.
- Silver e responsavel por limpeza, qualidade, conversao temporal, enriquecimento temporal e particionamento. Nao mova essas regras para Bronze nem repita-as na Gold.
- Gold e responsavel por metricas e tabelas de consumo analitico. Deve validar o schema Silver antes de agregar e nao pode tratar snapshots de aeronave como voos, companhias aereas ou aeroportos sem dimensao/evento que sustente esse significado.
- O contrato atual da Silver contem campos achatados como `icao24`, `callsign`, `origin_country`, `time_position`, `last_contact`, altitude, velocidade, estado no solo e campos de classificacao, alem de `time` como timestamp e das colunas temporais.

## Estado atual e divergencias conhecidas

- O `src/bronze.py` atualmente explode e achata `states` antes de gravar. Isso e o comportamento existente que sustenta o contrato da Silver, mas diverge da regra da skill Bronze de preservar o payload original. Qualquer mudanca nessa fronteira deve decidir explicitamente se o Bronze passara a manter o JSON bruto, se havera uma etapa intermediaria de flattening ou se o contrato atual sera mantido; depois, atualize a Silver e os notebooks afetados.
- O `src/silver.py` ainda usa caminhos fixos relativos e nao expoe todos os parametros previstos pela skill Silver. Melhorias de parametrizacao devem preservar os defaults de `data/bronze` e `data/silver` e o checkpoint existente.
- Nao ha implementacao Gold nem suite de testes automatizados visivel no repositorio atual. Novas camadas ou correc oes relevantes devem incluir validacoes pequenas e deterministicas antes de serem consideradas concluidas.

## Qual skill usar

Sempre carregue a skill equivalente antes de implementar, revisar ou alterar uma camada:

- Use a skill `bronze` para produtor/consumidor Kafka, ingestao OpenSky, landing/raw, schema de entrada, persistencia Parquet bruta, streaming, checkpoint e observabilidade da ingestao. Ela esta em `.github/skills/bronze/SKILL.md`.
- Use a skill `silver` para leitura streaming do Parquet Bronze, limpeza, deduplicacao, tratamento de nulos, conversao de epoch, watermark, enriquecimento temporal, particionamento e checkpoint da Silver. Ela esta em `.github/skills/silver/SKILL.md`.
- Use a skill `gold` para criar ou revisar marts PySpark derivados da Silver, agregacoes, granularidade, metricas de aeronaves, altitude/velocidade, duracao e aeroportos. Ela esta em `.github/skills/gold/SKILL.md`.
- Se uma tarefa atravessar camadas, use as skills em ordem Bronze -> Silver -> Gold e valide o contrato produzido por cada etapa antes de alterar a seguinte.
- Para Docker, Kafka, credenciais, notebooks ou documentacao sem alterar a logica de uma camada, esta instrucao fornece o contexto; carregue uma skill de camada somente se a mudanca tambem afetar aquela camada.

## Regras de implementacao

- Preserve os nomes e tipos do contrato existente, salvo quando a skill da camada justificar uma mudanca explicita e a camada seguinte for atualizada.
- Mantenha caminhos de dados, checkpoints e codigo separados. Nao versionar dados gerados, checkpoints, credenciais ou tokens.
- Prefira caminhos derivados de `Path(__file__)` em scripts novos para que a execucao nao dependa do diretorio corrente.
- Mantenha configuracoes como origem, destino, trigger, checkpoint e periodo de backfill parametrizaveis quando o script precisar ser reutilizado.
- Nao invente colunas ausentes na fonte. Em especial, `origin_country` nao e companhia aerea; `icao24` nao e `flight_id`; e `time_position`/`last_contact` nao representam entrada e saida de voo.
- Ao alterar uma transformacao, valide schema, particionamento, contagens antes/depois e comportamento de reprocessamento. Se nao houver testes automatizados, registre a verificacao executada com Spark local ou notebook.

## Roteamento rapido

Antes de editar, identifique a pergunta principal:

1. O dado esta sendo coletado ou salvo cru? Use `bronze`.
2. O dado esta sendo limpo, convertido ou preparado temporalmente? Use `silver`.
3. O dado esta sendo agregado em uma tabela ou indicador para consumo? Use `gold`.
4. A mudanca cruza mais de uma resposta? Comece pela skill da camada onde o contrato muda e depois valide as camadas dependentes.