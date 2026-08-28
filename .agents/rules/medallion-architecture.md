# Sao Paulo Planes: Arquitetura Medalhão

## Visão Geral

Este projeto coleta estados de aeronaves na região de São Paulo usando a API OpenSky, publica os eventos no Kafka e processa os dados com PySpark em uma arquitetura medalhão:

```text
OpenSky API -> producer.py -> Kafka (plane_data) -> bronze.py -> data/bronze/output
                                                    -> silver.py -> data/silver/output
                                                    -> gold (a implementar) -> data/gold/<mart>
```

O `docker-compose.yml` fornece Zookeeper, Kafka, Kafdrop e um ambiente Jupyter com Spark. Dentro da rede Docker, os clientes Kafka usam `kafka:29092`; fora dela, o broker é anunciado em `localhost:9092`. O produtor usa as credenciais `ClientId` e `ClientSecret` carregadas do `.env`.

## Componentes e Responsabilidades

- `src/producer.py`: consulta a OpenSky a cada 30 segundos para a caixa geográfica configurada, serializa o timestamp e a lista `states`, e envia o payload ao tópico Kafka `plane_data`. Não coloque regras de limpeza ou métricas neste componente.
- `src/bronze.py`: consome `plane_data` via Structured Streaming, interpreta o JSON com schema explícito, achata cada item de `states` e grava Parquet em `data/bronze/output` no modo `append`, com checkpoint em `data/bronze/checkpoint` e trigger de 35 segundos.
- `src/silver.py`: lê o Parquet Bronze em streaming usando o schema obtido de uma leitura estática, remove duplicatas por `icao24` e `last_contact`, remove registros sem `callsign`, converte `time` de epoch para `timestamp` e cria `year`, `month` e `day`. Grava Parquet particionado por essas colunas em `data/silver/output`, com checkpoint em `data/silver/checkpoint`.
- `notebooks/bronze.ipynb` e `notebooks/silver.ipynb`: são superfícies exploratórias e de validação do mesmo fluxo. Ao corrigir o pipeline, prefira os scripts em `src/` e atualize notebooks apenas quando a documentação executável precisar acompanhar a mudança.
- Camada Gold: deve consumir Silver em batch e produzir marts Parquet independentes em `data/gold/`, conforme o contrato da skill Gold.

## Contratos entre Camadas

- **Bronze**: recebe o payload da fonte e é a zona de landing/raw. O dado deve permanecer o mais próximo possível da origem, com persistência append-only, checkpoint e metadados de ingestão.
- **Silver**: responsável por limpeza, qualidade, conversão temporal, enriquecimento temporal e particionamento. Não mova essas regras para Bronze nem repita-as na Gold.
- **Gold**: responsável por métricas e tabelas de consumo analítico. Deve validar o schema Silver antes de agregar e não pode tratar snapshots de aeronave como voos, companhias aéreas ou aeroportos sem dimensão/evento que sustente esse significado.

## Regras de Implementação

- Preserve os nomes e tipos do contrato existente, salvo quando a skill da camada justificar uma mudança explícita e a camada seguinte for atualizada.
- Mantenha caminhos de dados, checkpoints e código separados. Não versionar dados gerados, checkpoints, credenciais ou tokens.
- Prefira caminhos derivados de `Path(__file__)` em scripts novos para que a execução não dependa do diretório corrente.
- Mantenha configurações como origem, destino, trigger, checkpoint e período de backfill parametrizáveis quando o script precisar ser reutilizado.
- Não invente colunas ausentes na fonte. Em especial, `origin_country` não é companhia aérea; `icao24` não é `flight_id`; e `time_position`/`last_contact` não representam entrada e saída de voo.

