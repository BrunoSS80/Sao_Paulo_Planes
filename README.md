# Sao_Paulo_Planes

## Executar o pipeline

Com Kafka e os demais servicos iniciados pelo Docker Compose, execute na raiz do
projeto:

```bash
docker compose up -d
python run_pipeline.py
```

O comando inicia somente `producer` e `bronze` simultaneamente.

Ao pressionar
`Ctrl+C`, ou se um dos processos terminar com erro, os outros tambem sao
encerrados. Para executar o launcher em outro ambiente Kafka, informe o broker:

```bash
python run_pipeline.py --kafka-bootstrap-servers host:9092
```

## Dashboard

O dashboard Streamlit consome os marts Parquet ja materializados em
`data/gold`. Execute a geracao da Gold separadamente e, depois, inicie o
relatorio pela raiz do projeto:

```bash
pip install -r dashboard/requirements.txt
streamlit run dashboard/app.py
```

O app usa UTC, permite filtrar o periodo e apresenta atividade horaria,
altitude, velocidade, companhias observadas e duracao media das sessoes na
area monitorada. Essas metricas representam observacoes e sessoes, nao voos.

Para construir a imagem, instalar as bibliotecas e subir o dashboard pelo
Docker:

```bash
docker compose up -d --build dashboard
```

Acesse `http://localhost:8501`. O servico monta `data/` como somente leitura;
os marts Gold precisam existir antes de abrir o relatorio.

## Airflow

O Compose tambem prepara um Airflow com PostgreSQL, scheduler e executor local.
Suba os servicos e aguarde a inicializacao do banco:

```bash
docker compose up -d airflow-init
docker compose up -d airflow-webserver airflow-scheduler
```

Acesse `http://localhost:8080` com `airflow` / `airflow`. Os DAGs locais ficam em
`dags/`, e os logs em `logs/`.