# Sao_Paulo_Planes

## Executar o pipeline

Com Kafka e os demais servicos iniciados pelo Docker Compose, execute na raiz do
projeto:

```bash
docker compose up -d
python run_pipeline.py
```

O comando inicia `producer`, `bronze` e `silver` simultaneamente. Ao pressionar
`Ctrl+C`, ou se um dos processos terminar com erro, os outros tambem sao
encerrados. Para executar o launcher em outro ambiente Kafka, informe o broker:

```bash
python run_pipeline.py --kafka-bootstrap-servers host:9092
```

Se a Silver foi executada anteriormente no container Jupyter e agora sera
executada no host, recrie o estado da Silver para descartar caminhos antigos:

```bash
python run_pipeline.py --reset-silver-state
```