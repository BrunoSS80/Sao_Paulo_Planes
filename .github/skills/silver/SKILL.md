# Skill: Silver Layer

Descrição
- Esta skill implementa a especificação operacional para a camada *silver* do pipeline. O agente que executar esta skill deve consumir arquivos Parquet em modo streaming (origem: camada bronze), aplicar regras de limpeza e enriquecimento temporal em tempo real e gravar a saída em Parquet particionado por data na camada silver.

Entrada esperada
- Fonte: diretório contendo arquivos Parquet gerados pela camada `bronze` (ex.: `../data/bronze/output`).
- Schema: o agente deve obter o `schema` a partir de uma amostra estática do Bronze (por exemplo, leitura estática de `../data/bronze/output`) e forçar esse schema na leitura streaming.
- Streaming: leitura via `spark.readStream.schema(schema).parquet(source_path)`.

Regras de transformação (obrigatórias)
1. Remover duplicatas: `dropDuplicates(["icao24", "last_contact"])`.
2. Remover registros sem `callsign`: `dropna(subset=["callsign"])`.
3. Converter `time` (epoch/segundos) para tipo `timestamp`: `withColumn("time", from_unixtime(col("time")).cast("timestamp"))`.
4. Criar colunas de particionamento temporal: `year`, `month`, `day` derivadas de `time`.
5. Adicionar coluna de ingestão opcional: `ingest_ts = current_timestamp()`.
6. (Opcional) Se política de dados tardios for necessária, aplicar `withWatermark("time","<X seconds/minutes>")` antes de agregações/joins.

Escrita e operacional
- Formato de saída: Parquet.
- Modo: `append`.
- Particionamento: `partitionBy("year","month","day")`.
- Checkpoint: obrigatório — usar `checkpointLocation` para garantir recuperação e idempotência.
- Trigger: parametrizável (ex.: `processingTime="35 seconds"`).

Parâmetros que o agente deve suportar
- `source_path`: caminho para input streaming (padrão `../data/bronze/output`).
- `schema_source`: caminho para leitura do schema (padrão `../data/bronze/output`).
- `output_path`: caminho da camada silver (padrão `../data/silver/output`).
- `checkpoint_path`: caminho do checkpoint (padrão `../data/silver/checkpoint`).
- `trigger_interval`: intervalo do trigger (ex.: `35 seconds`).
- `watermark`: valor opcional para watermark (ex.: `60 seconds`).
- `run_seconds`: modo de teste — roda por N segundos e finaliza.

Saída esperada
- Arquivos Parquet gravados em `output_path` com particionamento em pastas `year=.../month=.../day=...`.
- Schema de saída: todas as colunas originais do Bronze (limpas) + `time` em `timestamp` + `year`,`month`,`day` + `ingest_ts` (opcional).
- Checkpoint criado em `checkpoint_path`.

Observabilidade e verificação
- Logs: start, stop, erros e contagens básicas (linhas lidas, escritas, descartadas).
- Verificação pós-run: contagem de linhas antes/depois (leitura estática de Bronze vs Silver) e checar redução por regras aplicadas.
- Testes: criar testes unitários para funções de transformação (p.ex. conversão de `time`, remoção de nulos e duplicatas) e um teste de integração que execute o job em modo `--run_seconds` com pequenos arquivos Parquet.

Recuperação e idempotência
- Checkpoint é obrigatório para recomeçar sem duplicação.
- Job relançado deve usar o mesmo `checkpointLocation` para evitar `duplicate writes`.

Como usar (exemplos)
- Execução normal (ex. em desenvolvimento local com Spark já configurado):

```bash
python src/silver.py --source_path ../data/bronze/output \
    --schema_source ../data/bronze/output \
    --output_path ../data/silver/output \
    --checkpoint_path ../data/silver/checkpoint \
    --trigger_interval "35 seconds"
```

- Execução em modo teste (roda por 60 segundos e finaliza):

```bash
python src/silver.py --run_seconds 60
```

Boas práticas operacionais
- Configurar `spark.sql.shuffle.partitions` conforme ambiente.
- Definir `watermark` se houver dados tardios.
- Monitorar o tamanho das partições e compactar/compactor quando necessário.
- Versionar e documentar mudanças no schema de entrada: em caso de alteração do schema do Bronze, a skill deve ser executada com `schema_source` atualizado.

Checklist de aceite
- [ ] A skill lê Parquet em streaming usando um schema fixo obtido do Bronze.
- [ ] As regras de limpeza obrigatórias são aplicadas e testadas.
- [ ] Os dados são gravados em Parquet particionado por `year/month/day`.
- [ ] Existe checkpoint funcional que permite recuperação idempotente.
- [ ] Documentação de uso e verificação incluída.

Observação
- Esta skill assume que o input streaming é um diretório Parquet (micro-batches). Se a origem for Kafka/JSON/Outros, criar adaptador separado.
