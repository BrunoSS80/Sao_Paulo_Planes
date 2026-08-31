# ✈️ São Paulo Planes - Real-Time Air Traffic Lakehouse

Bem-vindo ao repositório do **São Paulo Planes**!

Este projeto demonstra uma solução completa de Engenharia de Dados para coleta em tempo real, ingestão em streaming, tratamento, enriquecimento dimensional, armazenamento e análise de dados de tráfego aéreo sobrevoando a região metropolitana de São Paulo (TMA São Paulo), utilizando a API do **OpenSky Network**, **Apache Kafka**, **Apache Spark (PySpark & Structured Streaming)**, **Apache Airflow**, **Docker** e visualização interativa com **Streamlit**.

Desenvolvido como projeto de portfólio, ele implementa boas práticas utilizadas em arquiteturas modernas de dados, contemplando processamento contínuo (streaming), transformações distribuídas no modelo **Medallion**, orquestração automatizada com sensores e operadores Docker, além de disponibilização de data marts analíticos para dashboards.

---

# 🏗️ Arquitetura de Dados

A arquitetura deste projeto segue o modelo **Medallion Architecture (Lakehouse)** dividido em três camadas no formato **Apache Parquet**.

```text
               ┌────────────────────────────────────────────────────────┐
               │              OpenSky Network API (REST)                │
               └──────────────────────────┬─────────────────────────────┘
                                          │ Coleta a cada 30s (Bounding Box SP)
                                          ▼
                               ┌─────────────────────┐
                               │   Kafka Producer    │
                               └──────────┬──────────┘
                                          │ Tópico: plane_data
                                          ▼
                               ┌─────────────────────┐
                               │    Apache Kafka     │
                               └──────────┬──────────┘
                                          │
                                          ▼
┌─────────────────────────────────────────────────────────────────────────────────────────┐
│                                MEDALLION ARCHITECTURE                                   │
│                                                                                         │
│  🥉 Bronze (Raw Stream)                                                                 │
│     └── Spark Structured Streaming consome Kafka -> Gravação em Parquet com Checkpoint │
│           │                                                                             │
│           ▼ (Trigger availableNow + Broadcast Join com OpenFlights)                     │
│  🥈 Silver (Trusted)                                                                    │
│     └── Deduplicação, tipagem temporal e enriquecimento com Companhias Aéreas           │
│           │                                                                             │
│           ▼ (Agregações analíticas particionadas por ano/mês/dia)                       │
│  🥇 Gold (Analytics Marts)                                                              │
│     ├── aircraft_activity        (Atividade horária de voos)                            │
│     ├── airline_counts           (Volume por companhia aérea)                           │
│     ├── altitude_speed           (Médias de altitude e velocidade)                      │
│     └── monitored_area_duration  (Tempo de permanência no espaço aéreo)                 │
└─────────────────────────────────────────┬───────────────────────────────────────────────┘
                                          │
                                          ▼
                               ┌─────────────────────┐
                               │ Streamlit Dashboard │
                               │  (Plotly Analytics) │
                               └─────────────────────┘
```

### 🥉 Bronze (Raw)

Armazena os dados brutos de telemetria das aeronaves consumidos do tópico Kafka exatamente como entregues pela API, estruturados e persistidos continuamente via **Spark Structured Streaming** em formato Parquet com tolerância a falhas via checkpointing.

### 🥈 Silver (Trusted)

Responsável pela limpeza, deduplicação por aeronave e timestamp (`icao24`, `last_contact`), tratamento de nulos, tipagem e particionamento temporal (`year`, `month`, `day`). Realiza enriquecimento dimensional via **Broadcast Join** com o dataset de companhias aéreas (*OpenFlights*) a partir do prefixo ICAO do *callsign*.

### 🥇 Gold (Analytics)

Contém data marts analíticos agregados e otimizados em Parquet, particionados por data, projetados para alimentar métricas operacionais e o dashboard:
- **`aircraft_activity`**: Contagem de aeronaves distintas ativas no ar por data e hora.
- **`airline_counts`**: Quantidade de aeronaves observadas por companhia aérea.
- **`altitude_speed`**: Estatísticas agregadas de altitude barométrica, altitude geométrica e velocidade.
- **`monitored_area_duration`**: Duração média das sessões de sobrevoo e tempo de permanência dentro da área monitorada.

---

# 📖 Visão Geral do Projeto

O projeto contempla:

- 📌 Arquitetura Lakehouse Medallion utilizando Bronze, Silver e Gold em Parquet.
- ✈️ Coleta contínua de dados de voos em tempo real da região de São Paulo via OpenSky API.
- 📨 Mensageria distribuída e desacoplamento com **Apache Kafka** e **Zookeeper**.
- ⚡ Ingestão e transformações distribuídas utilizando **Apache Spark (PySpark & Structured Streaming)**.
- 🔄 Enriquecimento dimensional com Broadcast Join para identificação de companhias aéreas.
- ⏱️ Orquestração de pipelines automatizada via **Apache Airflow** com `FileSensor` e `DockerOperator`.
- 📊 Dashboard analítico interativo desenvolvido com **Streamlit** e gráficos em **Plotly**.
- 🐳 Ambiente 100% conteinerizado e reproduzível com **Docker** e **Docker Compose**.

---

## 🧩 Diagrama Estrutural

```text
┌─────────────────┐       ┌─────────────────┐       ┌─────────────────┐
│ OpenSky Network │ ────> │ Kafka Producer  │ ────> │  Apache Kafka   │
│   (TMA SP BBox) │       │   (Python 3)    │       │ (Topic: planes) │
└─────────────────┘       └─────────────────┘       └────────┬────────┘
                                                             │
                                                             ▼
┌─────────────────┐       ┌─────────────────┐       ┌─────────────────┐
│ Streamlit App   │ <──── │    Gold Marts   │ <──── │  Silver Stream  │
│ (:8501 / Docker)│       │  (PySpark Jobs) │       │ (Broadcast Join)│
└─────────────────┘       └────────┬────────┘       └────────┬────────┘
                                   ▲                         ▲
                                   │     Airflow Pipeline    │
                                   └─────────┐       ┌───────┘
                                             │       │
                                      ┌──────┴───────┴──────┐
                                      │   Apache Airflow    │
                                      │ (Sensor & DockerOp) │
                                      └─────────────────────┘
```

---

# 🚀 Orquestração

A orquestração do pipeline de dados é gerenciada pelo **Apache Airflow**, combinando sensores de arquivos e execução isolada em contêineres Docker via `DockerOperator`.

### Pipeline 1 - Ingestão Contínua (Streaming)

```text
OpenSky API
    │
    ▼
Kafka Producer
    │
    ▼
Kafka Broker (plane_data)
    │
    ▼
Spark Bronze (Structured Streaming)
```

Executado continuamente para coletar coordenadas, altitude, velocidade e dados de voo a cada 30 segundos e persistir em `data/bronze/output`.

---

### Pipeline 2 - Processamento Batch / Incremental (Airflow DAG: `planes_pipeline`)

```text
FileSensor (check_bronze)
        │
        ▼
DockerOperator (silver_transformation)
        │
        ▼
DockerOperator (gold_marts)
```

1. **`check_bronze`**: Sensor do Airflow que monitora a chegada de novos arquivos Parquet na camada Bronze.
2. **`silver_transformation`**: Executa o processamento incremental (`availableNow=True`), limpando os dados brutos e integrando as dimensões de companhias aéreas.
3. **`gold_marts`**: Gera e atualiza os 4 data marts analíticos da camada Gold particionados por data.

---

# 📊 Dashboards

O projeto disponibiliza um painel analítico interativo desenvolvido em **Streamlit** e **Plotly**, consumindo diretamente os data marts materializados na camada Gold.

## ✈️ Painel de Monitoramento de Tráfego Aéreo

- **Atividade Horária**: Volume de aeronaves sobrevoando a região hora a hora.
- **Companhias Aéreas**: Distribuição e ranking das companhias com maior presença no espaço aéreo paulista.
- **Perfil de Voo**: Médias de altitude (barométrica e geométrica) e velocidade horizontal.
- **Permanência no Espaço Aéreo**: Análise do tempo de duração de sessões das aeronaves na área delimitada.
- **Filtros Interativos**: Seleção de datas e janelas temporais de análise (UTC).

---

# 🛠️ Tecnologias Utilizadas

- **Apache Spark 3.5.1 (PySpark & Structured Streaming)**
- **Apache Kafka 2.8.1 & Apache Zookeeper**
- **Apache Airflow 2.10.5**
- **Streamlit & Plotly**
- **Docker & Docker Compose**
- **PostgreSQL 16** (Metadados do Airflow)
- **Kafdrop** (Interface Web para monitoramento do Kafka)
- **Python 3**
- **OpenSky Network API** (`opensky-api`)
- **Apache Parquet**

---

# ▶️ Como Executar

## 1️⃣ Clone o repositório

```bash
git clone https://github.com/seu-usuario/sao-paulo-planes.git
cd sao-paulo-planes
```

---

## 2️⃣ Configure as variáveis de ambiente

Crie um arquivo `.env` na raiz do projeto (opcional para credenciais autenticadas da [OpenSky API](https://opensky-network.org/data/api)):

```env
ClientId=seu_client_id
ClientSecret=seu_client_secret
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
```

---

## 3️⃣ Suba a infraestrutura de serviços

Inicie todos os contêineres necessários (Kafka, Zookeeper, Spark Master/Worker, Airflow, Postgres, Kafdrop e Dashboard):

```bash
# Inicializa o banco de dados do Airflow
docker compose up -d airflow-init

# Sobe os demais serviços
docker compose up -d
```

---

## 4️⃣ Inicie a ingestão em tempo real

Execute o script unificado que inicia o coletor (producer) e o streaming de ingestão da camada Bronze:

```bash
python run_pipeline.py
```

> 💡 *Para interromper, basta pressionar `Ctrl+C`. O script gerencia o encerramento gracioso de todos os subprocessos.*

---

## 5️⃣ Execute a orquestração no Airflow

Acesse o Apache Airflow em [http://localhost:8080](http://localhost:8080) (usuário: `airflow` / senha: `airflow`) e ative a DAG **`planes_pipeline`**.

Caso deseje gerar a camada Gold manualmente via linha de comando:

```bash
# Execução da Silver
docker exec -u root -it spark-master python3 /opt/spark/src/silver.py

# Execução da Gold
docker exec -u root -it spark-master python3 /opt/spark/src/gold/common.py
```

---

## 6️⃣ Acesse os Dashboards e Interfaces

- **Streamlit Dashboard**: [http://localhost:8501](http://localhost:8501)
- **Apache Airflow**: [http://localhost:8080](http://localhost:8080)
- **Kafdrop (Kafka UI)**: [http://localhost:9000](http://localhost:9000)
- **Spark Master UI**: [http://localhost:8081](http://localhost:8081)
- **Jupyter Notebook**: [http://localhost:8888](http://localhost:8888)

---

# 🎯 Objetivo

Desenvolver uma plataforma escalável de Engenharia de Dados em tempo real para monitoramento e análise do espaço aéreo da Região Metropolitana de São Paulo, aplicando streaming de eventos com Kafka, processamento distribuído com Apache Spark na arquitetura Medallion, orquestração robusta com Apache Airflow e disponibilização de métricas em um painel interativo.

---

# 📌 Especificações

| Item | Descrição |
|------|-----------|
| **Fonte de Dados** | OpenSky Network API (Bounding Box SP) & OpenFlights |
| **Linguagem** | Python 3 |
| **Streaming & Ingestão** | Apache Kafka & Spark Structured Streaming |
| **Processamento** | Apache Spark 3.5.1 (PySpark) |
| **Arquitetura** | Medallion (Lakehouse) |
| **Armazenamento** | Apache Parquet (Particionado por ano/mês/dia) |
| **Camadas** | Bronze (Raw), Silver (Trusted) e Gold (Analytics Marts) |
| **Orquestração** | Apache Airflow (FileSensor + DockerOperator) |
| **Visualização** | Streamlit & Plotly |
| **Ambiente** | Docker & Docker Compose |

---

# 📚 Principais Funcionalidades

- Coleta contínua de telemetria aeronáutica em tempo real via OpenSky API.
- Streaming e mensageria de alta vazão com Apache Kafka.
- Ingestão contínua com tolerância a falhas e controle de offsets/checkpoints em Spark.
- Limpeza, deduplicação e enriquecimento dimensional com base de companhias aéreas.
- Agregações analíticas e geração de 4 data marts especializados na camada Gold.
- Orquestração agendada e orientada a sensores de dados via Apache Airflow.
- Dashboard web interativo com gráficos e métricas de tráfego aéreo.
- Ambiente totalmente conteinerizado e pronto para execução local.

---

## 🧑‍💻 Autor

Desenvolvido por **Bruno Severgnini da Silva**

📌 Conecte-se comigo:
- LinkedIn: https://www.linkedin.com/in/bruno-severgnini/
- GitHub: https://github.com/BrunoSS80