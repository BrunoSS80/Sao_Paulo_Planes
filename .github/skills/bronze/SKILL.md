---
name: bronze
description: "Use when working on the bronze layer, raw ingestion, streaming ingestion, Kafka consumers, landing-zone logic, or ingestion scripts for this project. Use this skill to design or review bronze jobs, raw persistence logic, and streaming ingestion without transforming the original source schema."
---

# Skill: Bronze Layer

Use esta skill quando a tarefa estiver relacionada à camada bronze do pipeline de dados. A responsabilidade desta camada é receber o dado o mais próximo possível da fonte original, preservá-lo em formato bruto e garantir confiabilidade, rastreabilidade e ingestão contínua.

## Quando usar esta skill

Use esta skill quando o agente precisar:

- criar ou ajustar jobs de ingestão em streaming;
- implementar consumo de dados brutos de APIs, filas, tópicos ou arquivos;
- persistir dados na camada bronze sem transformação de negócio;
- revisar código para garantir que o schema original foi preservado;
- validar que não houve enriquecimento, joins, regras analíticas ou normalização de domínio;
- avaliar se a lógica está no lugar errado (por exemplo, bronze vs silver).

## Entrada prevista

A skill aceita uma descrição da tarefa, por exemplo:

- "criar consumidor Kafka para a camada bronze";
- "inserir dados brutos em parquet sem transformar o schema";
- "ajustar pipeline de ingestão em streaming para um endpoint externo";
- "revisar se esse script respecta as regras da camada bronze";
- "identificar se a lógica é de bronze ou silver".

A entrada esperada é uma tarefa específica com contexto suficiente para dizer:

- qual fonte está sendo usada;
- se o fluxo é em streaming ou batch;
- qual formato ou destino de persistência;
- se a intenção é apenas coletar e armazenar dados crus.

## Saída esperada

A saída esperada do agente deve incluir:

- análise da tarefa em relação à camada bronze;
- recomendação de implementação ou correção;
- validação de que o schema original foi preservado;
- indicação de eventuais violações de camada;
- observações sobre streaming, idempotência, checkpoint, metadados e monitoramento;
- proposta final de código ou arquitetura quando aplicável.

A resposta deve ser orientada a dados brutos, não a análise de negócio.

## Regras obrigatórias da camada bronze

- Trabalhe com processo em streaming sempre que a fonte suportar esse padrão ou quando o fluxo de dados for contínuo.
- Não realize transformações no schema original da fonte de dados.
- Não realize enriquecimento dos dados.
- Não aplique joins, agregações, deduplicação sem regra explícita de negócio ou normalização de domínio.
- Preserve o dado exatamente como foi recebido, incluindo tipos, formatos, campos, nomes e valores originais.
- Priorize ingestão append-only e incremental, sem sobrescrever registros crus já persistidos.
- Mantenha a camada bronze como zona de landing/raw, não como camada de negócio ou analítica.
- Registre metadados relevantes da ingestão, como origem, data/hora de captura, particionamento, identificador do lote e status da leitura.

## Boas práticas da camada bronze

- Use processamento em streaming para reduzir latência e manter o pipeline próximo ao evento ou à origem.
- Trate a ingestão como uma operação de coleta e persistência, e não como transformação de negócio.
- Valide apenas integridade estrutural básica, como campos obrigatórios, formato de arquivo e conexões de leitura, sem remodelar o dado.
- Preserve a ordem dos eventos e evite reprocessamentos que alterem o histórico bruto salvo.
- Prefira mecanismos de idempotência e checkpoints em fluxos contínuos.
- Mantenha logs claros para falhas de leitura, retries, timeouts, perda de mensagens e rejeição de registros.
- Separe responsabilidades entre coleta, persistência e monitoramento; o código da camada bronze não deve conter regras de negócio.
- Use nomes de tabelas/arquivos que representem o dado bruto, sem padronização de domínio que altere o significado original.
- Em cenários de falha, retorne o dado para a forma mais próxima do original e preserve o histórico da origem.
- Documente a origem, a frequência de ingestão, o formato do payload e os critérios de particionamento.

## Proibições explícitas

- Não renomear campos, converter tipos, aplicar regras de negócio, criar colunas derivadas ou reformatar strings.
- Não realizar enriquecimento com dados externos, joins com referências ou lookup tables.
- Não transformar datas, textos, valores monetários, códigos ou identificadores para fins analíticos.
- Não realizar limpeza de dados, padronização, correção de qualidade ou deduplicação agressiva sem instrução explícita do projeto.
- Não criar modelos analíticos, agregados ou tabelas de apresentação nesta camada.

## Checklist antes de concluir a tarefa

Antes de finalizar, confirme:

- o fluxo funciona em streaming quando apropriado;
- o schema original da fonte foi preservado;
- nenhum enriquecimento foi adicionado;
- o dado bruto foi armazenado sem transformação de negócio;
- metadados e observabilidade foram considerados;
- a solução permanece compatível com reprocessamento e histórico bruto.

## Exemplo de saída esperada

Uma resposta adequada deve seguir este formato:

1. Identificação da camada: bronze.
2. Diagnóstico do objetivo do script.
3. Verificação das regras: streaming, schema, enriquecimento, persistência bruta.
4. Recomendações finais e pontos de atenção.
5. Observações sobre monitoramento, idempotência e rastreabilidade.

## Objetivo final

A skill bronze deve orientar o agente a tratar a camada como zona de landing raw, com foco em coleta, robustez, rastreabilidade e preservação do dado original, sem misturar responsabilidade de negócio ou transformação analítica.
