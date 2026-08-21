---
name: gold
description: "Use when designing, implementing, or reviewing the gold layer of this medalion architecture: PySpark data marts from Silver Parquet for aircraft activity, airline counts, average altitude and speed, or monitored-area duration."
---

# Skill: Gold Layer

## Objetivo

Esta skill orienta a construção da camada *gold* da arquitetura medalhão. Ela consome dados já tratados da camada `silver`, prepara tabelas analíticas e grava cada data mart em Parquet para consumo por dashboards, consultas e relatórios.

A camada gold deve conter métricas prontas para análise, com nomes de colunas estáveis, granularidade documentada e resultados reproduzíveis. Não deve repetir regras de limpeza que pertencem à silver.

## Entrada e contrato Silver

- Fonte padrão: `../data/silver/output`.
- Formato: Parquet, normalmente particionado por `year`, `month` e `day`.
- Leitura: batch com `spark.read.parquet(source_path)`, preservando o pruning das partições quando houver filtro de data.
- Campos disponíveis no contrato atual: `time`, `icao24`, `callsign`, `origin_country`, `time_position`, `last_contact`, `longitude`, `latitude`, `baro_altitude`, `on_ground`, `velocity`, `true_track`, `vertical_rate`, `geo_altitude`, `spi`, `position_source`, `category`, além das colunas de particionamento temporal.
- Antes de transformar, validar o schema real da Silver e falhar com mensagem clara se faltar uma coluna obrigatória.
- `time`, `time_position` e `last_contact` devem estar em `timestamp` na Silver. Se a implementação precisar aceitar epoch, converter explicitamente e documentar a unidade; não inferir silenciosamente.
- Não usar `states` aninhado na gold: a Silver atual já o explode e achata.

## Organização do projeto

Separar código e dados:

```text
src/gold/
  common.py
  aircraft_activity.py
  airline_counts.py
  altitude_speed.py
  monitored_area_duration.py

data/gold/
  aircraft_activity/
  airline_counts/
  altitude_speed/
  monitored_area_duration/
```

- Cada mart deve ter seu próprio arquivo PySpark executável.
- `common.py` deve concentrar apenas leituras, validações, colunas temporais, normalizações e tabelas intermediárias compartilhadas por dois ou mais marts.
- Não duplicar a mesma tabela derivada em vários scripts.
- Cada mart deve gravar em seu próprio diretório, para evitar colisões de schema e permitir atualização independente.
- Usar `Path(__file__).resolve().parents[2]` ou configuração equivalente para que os caminhos não dependam do diretório de execução.

## Marts obrigatórios

### 1. Quantidade de aeronaves ativas por horário

Arquivo: `src/gold/aircraft_activity.py`.

- Granularidade: uma linha por `date` e hora (`hour`), podendo incluir `year`, `month` e `day` para particionamento.
- Métrica: `active_aircraft_count = countDistinct(icao24)`.
- Definir o horário a partir de `time` e considerar uma aeronave ativa quando houver registro válido nesse horário. Não contar `icao24` nulo.
- Se a análise precisar distinguir horário local de UTC, expor/configurar o timezone e registrar a escolha; o padrão deve ser UTC.

### 2. Quantidade de aeronaves por companhia aérea

Arquivo: `src/gold/airline_counts.py`.

- Granularidade: uma linha por companhia e período (`date` ou janela configurada).
- Métrica: `aircraft_count = countDistinct(icao24)`.
- O dataset atual não possui um campo explícito de companhia aérea. Não chamar `origin_country` de companhia. A implementação deve exigir uma coluna configurável, como `airline`, ou uma dimensão de referência `icao24 -> airline`.
- Quando não houver essa dimensão, o mart deve ser bloqueado com erro descritivo ou marcado como pendente, nunca produzir uma contagem com significado incorreto.

### 3. Altitude e velocidade médias

Arquivo: `src/gold/altitude_speed.py`.

- Granularidade padrão: por `date` e hora; permitir dimensão adicional como companhia apenas se existir no contrato.
- Métricas: `avg_altitude` usando `baro_altitude` e `avg_velocity` usando `velocity`.
- Ignorar valores nulos; documentar unidade de altitude e velocidade conforme a origem OpenSky.
- Se houver filtro operacional, excluir registros em solo (`on_ground = true`) somente quando isso fizer parte do requisito e deixar o filtro explícito.

### 4. Tempo médio entre entrada e saída da área monitorada

Arquivo: `src/gold/monitored_area_duration.py`.

- A fonte atual é um snapshot de posições e não contém eventos explícitos de entrada e saída da área monitorada.
- Definir entrada como o primeiro registro válido de uma sessão e saída como o último registro válido antes do encerramento da sessão, usando uma regra de sessão configurável e documentada, como um intervalo máximo de inatividade.
- Não calcular duração usando a diferença entre `time_position` e `last_contact`: isso mede latência do registro, não permanência na área monitorada.
- Para habilitar este mart, exigir uma regra de sessão aprovada ou uma tabela de eventos com `icao24` (ou outro identificador de aeronave), `entry_ts` e `exit_ts`.
- Após validar a fonte, calcular `duration_seconds = unix_timestamp(exit_ts) - unix_timestamp(entry_ts)`, descartar intervalos nulos ou negativos, e agregar `avg_area_duration_seconds` na granularidade documentada.
- Registrar a quantidade de sessões usadas, descartadas e ainda sem saída.

## Procedimento de implementação

1. Ler a configuração de `source_path`, `output_path`, `run_date` opcional e colunas de dimensão.
2. Criar uma SparkSession com nome do mart e configuração de shuffle apropriada ao ambiente.
3. Ler o Parquet Silver e validar schema, tipos, timezone e partições.
4. Em `common.py`, preparar somente as colunas compartilhadas: chaves não nulas, timestamp, `date`, `hour`, `year`, `month`, `day` e dimensões de referência.
5. Implementar o mart em função testável, por exemplo `build_aircraft_activity(df) -> DataFrame`, mantendo a escrita fora da transformação.
6. Aplicar `countDistinct` nas métricas de entidades e `avg` apenas sobre medidas numéricas válidas.
7. Escrever Parquet em modo `overwrite` para uma data/janela materializada ou em modo definido pela estratégia de backfill. Nunca misturar `append` e `overwrite` sem uma política explícita de reprocessamento.
8. Particionar por `year`, `month` e `day` quando a tabela tiver dimensão diária; não particionar por colunas de alta cardinalidade como `icao24`.
9. Exibir schema, período processado, número de linhas de saída e métricas de descarte.
10. Encerrar a SparkSession em `finally` e retornar código de erro não zero quando contrato ou validação falhar.

## Parâmetros mínimos

Todo script deve aceitar, por argparse ou configuração equivalente:

- `--source_path` (padrão `../data/silver/output`)
- `--output_path` (padrão correspondente ao mart em `../data/gold/`)
- `--run_date` opcional para backfill/reprocessamento
- `--timezone` (padrão `UTC`)
- `--shuffle_partitions` (padrão compatível com o ambiente local)

O mart de duração na área monitorada deve aceitar também a configuração da regra de sessão ou o caminho dos eventos de entrada e saída, por exemplo `--session_gap` ou `--area_events_path`.

## Qualidade e validação

- Testar cada função `build_*` com DataFrames pequenos e determinísticos.
- Cobrir duplicidade de snapshots, `icao24` nulo, medidas nulas, timestamps fora de ordem e mudanças de dia/hora.
- Verificar que `countDistinct(icao24)` não é apresentado como quantidade de voos e que a duração representa sessões na área monitorada.
- Verificar reconciliação: a soma por hora/dimensão deve corresponder à mesma definição de entidade usada no mart.
- Validar que nenhum mart dependente de coluna ausente seja gravado silenciosamente.
- Executar um teste de integração local que leia Parquet de amostra, grave em diretório temporário e confira schema, partições e contagens.
- Para backfill, confirmar que reprocessar a mesma data produz o mesmo resultado e não duplica linhas.

## Checklist de aceite

- [ ] Existe um arquivo PySpark por cada um dos quatro marts obrigatórios.
- [ ] Lógica comum está centralizada e não duplicada.
- [ ] Código e dados estão separados em `src/gold/` e `data/gold/`.
- [ ] Cada mart tem granularidade, chave e unidade documentadas.
- [ ] Companhia aérea e eventos ou regras de entrada/saída da área monitorada são validados antes do uso.
- [ ] Os quatro indicadores não confundem aeronaves, snapshots, sessões na área monitorada e voos.
- [ ] Escrita, particionamento, backfill e idempotência estão definidos.
- [ ] Testes unitários e de integração verificam agregações e contrato.
- [ ] Logs e falhas de schema são observáveis.

## Exemplo de uso

```bash
python src/gold/aircraft_activity.py \
  --source_path ../data/silver/output \
  --output_path ../data/gold/aircraft_activity \
  --run_date 2026-08-18
```

Para `airline_counts`, não executar a materialização até que a coluna ou dimensão de companhia aérea esteja disponível e validada. Para `monitored_area_duration`, não materializar até que a regra de sessão ou os eventos de entrada e saída estejam disponíveis e validados.
