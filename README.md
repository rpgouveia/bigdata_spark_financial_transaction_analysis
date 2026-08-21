# ⚡ Big Data - Análise de Transações Financeiras com Apache Spark

Projeto de análise de dados de transações financeiras utilizando **Apache Spark**, implementado em duas APIs distintas: **Spark Core (RDDs)** e **Spark SQL (DataFrames)**.
Desenvolvido em dupla como trabalho avaliativo para a disciplina de Big Data (Grupo 9).

> 🔗 Projeto complementar: [Análise das mesmas transações com Hadoop MapReduce](https://github.com/rpgouveia/bigdata_hadoop_financial_transaction_analysis)

---

## 📋 Sobre o Projeto

Este projeto reimplementa em Apache Spark um conjunto de rotinas de análise de transações financeiras, com o objetivo de **comparar paradigmas de processamento distribuído**.

Cada uma das 10 rotinas foi implementada **duas vezes** — uma com RDDs e outra com DataFrames — totalizando **20 implementações**. Isso permite contrastar diretamente:

| Aspecto | Spark Core (RDD) | Spark SQL (DataFrame) |
|---|---|---|
| Nível de abstração | Baixo — controle explícito de transformações | Alto — API declarativa |
| Otimização | Manual, pelo desenvolvedor | Automática (Catalyst Optimizer) |
| Parsing do CSV | `split()` manual por índice de coluna | `inferSchema` + colunas tipadas |
| Estruturas auxiliares | Classes `Serializable` customizadas | Schema implícito do DataFrame |
| Verbosidade | Maior | Menor |

A referência de comparação é o projeto original em Hadoop MapReduce, onde as mesmas análises exigiram pipelines de 2 a 3 *jobs* encadeados.

---

## 🎯 Objetivos

- Processar um dataset de ~500 mil transações financeiras
- Implementar a mesma lógica de negócio em RDDs e DataFrames
- Contrastar Spark com Hadoop MapReduce em expressividade e estrutura
- Aplicar padrões de agregação, ranking e pipelines multi-etapa
- Construir um pipeline de detecção de risco de fraude baseado em perfil comportamental

---

## 📁 Estrutura do Projeto

```
src/main/java/
├── sparkcore/                                  # Implementação com RDDs
│   ├── routines/
│   │   ├── basic/
│   │   │   ├── AmountByCity.java               # Valor total por cidade
│   │   │   ├── AmountByClient.java             # Valor total por cliente
│   │   │   └── ChipUsageCount.java             # Contagem por tipo de transação
│   │   ├── intermediate/
│   │   │   ├── citystatistics/                 # Estatísticas por cidade (+ CityStats)
│   │   │   ├── topcategoriesbycity/            # Top 3 MCC por cidade (+ CategoryCount)
│   │   │   ├── topcategoriesbystate/           # Top 3 MCC por estado (EUA)
│   │   │   └── topcategoriesbycountry/         # Top 3 MCC por país (internacional)
│   │   └── advanced/
│   │       ├── categorybytimeperiod/           # Top 3 MCC por cidade e período
│   │       ├── clientbehaviorchipuse/          # Perfil de risco por cliente e UF
│   │       └── frauddetectionpipeline/         # Pipeline de detecção de fraude
│   └── utils/
│       └── MCCDescriptionMapper.java           # Mapeamento MCC → descrição
│
├── sparksql/                                   # Implementação com DataFrames
│   ├── routines/
│   │   ├── basic/                              # Mesmas 3 rotinas básicas
│   │   ├── intermediate/                       # Mesmas 4 rotinas intermediárias
│   │   └── advanced/                           # Mesmas 3 rotinas avançadas
│   └── utils/
│       └── MCCDescriptionMapper.java
│
└── resources/
    ├── transactions_data.csv                   # Dataset (~500 mil transações)
    └── log4j2.properties                       # Configuração de logging
```

---

## 🚀 Rotinas Implementadas

### 📌 Básicas

| Rotina | Descrição |
|---|---|
| **AmountByCity** | Valor total transacionado por cidade, ordenado decrescente |
| **AmountByClient** | Valor total transacionado por cliente |
| **ChipUsageCount** | Distribuição de transações por tipo (Chip, Swipe, Online) |

**Conceitos:** agregação por chave, precisão monetária com `BigDecimal`, ordenação de resultados.

### ⭐ Intermediárias

| Rotina | Descrição |
|---|---|
| **CityStatistics** | Contagem, valor total e ticket médio por cidade |
| **TopCategoriesByCity** | As 3 categorias (MCC) mais frequentes em cada cidade |
| **TopCategoriesByState** | Top 3 MCC por estado — filtra apenas transações domésticas (50 estados + DC) |
| **TopCategoriesByCountry** | Top 3 MCC por país — filtra apenas transações internacionais |

**Conceitos:** classes auxiliares `Serializable` (equivalentes aos *Custom Writables* do Hadoop), ranking com ordenação, filtragem geográfica, mapeamento de códigos MCC para descrições legíveis.

### 🔥 Avançadas

#### CategoryByTimePeriod
Identifica as 3 categorias mais frequentes por **cidade e período do dia**, usando chave composta (`CityPeriodKey`).

| Período | Faixa horária |
|---|---|
| MORNING | 00:00 – 11:59 |
| AFTERNOON | 12:00 – 17:59 |
| NIGHT | 18:00 – 23:59 |

> No Hadoop, esta análise exigiu 2 jobs MapReduce encadeados com `SequenceFile` intermediário. Em Spark Core, resolve-se em 4 etapas encadeadas sem persistência intermediária.

#### ClientBehaviorChipUse
Perfil de risco em duas fases:
- **Fase 1 — perfil do cliente:** calcula `onlineRate`, `errorRate`, ticket médio e valor máximo; determina a UF predominante; classifica em LOW / MED / HIGH.
- **Fase 2 — agregação por UF:** contagem de clientes por faixa de risco e identificação das **top 5 cidades** com maior concentração de clientes de alto risco, ordenadas por percentual.

#### FraudDetectionPipeline
Pipeline integrado de 3 etapas para detecção de risco de fraude:

1. **Client Profile Builder** — agrega todas as transações por cliente, extraindo 13 métricas comportamentais (cidades únicas, MCCs únicos, cartões distintos, taxa de erro, chargebacks, janela temporal, proporção online/presencial).
2. **Risk Category Classifier** — calcula um *risk score* ponderado a partir de **7 fatores de risco**, registrando quais fatores dispararam para cada cliente.
3. **Final Risk Report Generator** — consolida relatórios por categoria com médias, totais e os 10 clientes de maior risco.

**Fatores de risco avaliados:**

| # | Fator | Peso máximo |
|---|---|---|
| 1 | Mobilidade geográfica (cidades distintas) | 15 |
| 2 | Diversidade de categorias MCC | 12 |
| 3 | Múltiplos cartões | 20 |
| 4 | Taxa de erros nas transações | 25 |
| 5 | Ocorrência de chargebacks | 25 |
| 6 | Ticket médio elevado | 15 |
| 7 | Desbalanceamento entre canais (online × presencial) | 10 |

**Classificação final:**

| Categoria | Score | Interpretação |
|---|---|---|
| `LOW` | 0 – 30 | Comportamento normal |
| `MEDIUM` | 31 – 60 | Alguns sinais de alerta |
| `HIGH` | 61 – 85 | Múltiplos indicadores de risco |
| `CRITICAL` | 86+ | Risco extremo |

O pipeline instrumenta o tempo de execução de cada etapa e imprime um resumo consolidado ao final.

---

## 📊 Dataset

Arquivo CSV com aproximadamente **500 mil transações**, localizado em `src/main/resources/transactions_data.csv`.

```
id,date,client_id,card_id,amount,use_chip,merchant_id,merchant_city,merchant_state,zip,mcc,errors
```

| Campo | Descrição |
|---|---|
| `id` | Identificador único da transação |
| `date` | Data e hora (`yyyy-MM-dd HH:mm:ss`) |
| `client_id` | Identificador do cliente |
| `card_id` | Identificador do cartão |
| `amount` | Valor da transação (formato `$XX.XX`; valores negativos indicam chargeback) |
| `use_chip` | Tipo da transação (Chip, Swipe, Online) |
| `merchant_id` | Identificador do comerciante |
| `merchant_city` | Cidade do comerciante |
| `merchant_state` | Estado (EUA) ou país |
| `zip` | Código postal |
| `mcc` | Merchant Category Code |
| `errors` | Erros e validações associados à transação |

---

## 🛠️ Tecnologias Utilizadas

| Tecnologia | Versão |
|---|---|
| Java (OpenJDK) | 17 |
| Apache Spark (Core + SQL) | 4.0.1 |
| Scala (binário) | 2.13 |
| Hadoop Client | 3.4.2 |
| SLF4J | 2.0.9 |
| JUnit | 4.13.2 |
| Maven | 3.6+ |

Build configurado com **maven-shade-plugin** para geração de JAR executável com todas as dependências.

---

## ⚙️ Como Executar

### Pré-requisitos

```bash
# OpenJDK 17
# Apache Maven 3.6 ou superior
```

> ⚠️ O Spark 4.x exige acesso reflexivo a módulos internos da JVM. Ao executar em JDK 17, adicione as flags:
> `--add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED`

### Compilação

```bash
mvn clean package
```

O JAR com dependências é gerado em `target/spark_fta-1.0-SNAPSHOT.jar`.

### Execução via IDE

Todas as rotinas rodam em modo local (`local[*]`). Configure os argumentos do *Run Configuration*:

```
<input-path> <output-path>
```

Exemplo para `sparkcore.routines.basic.AmountByCity`:

```
src/main/resources/transactions_data.csv output/spark_core/basic/amount_by_city
```

### Execução via spark-submit

```bash
# Spark Core (RDDs)
spark-submit \
  --class sparkcore.routines.advanced.frauddetectionpipeline.FraudDetectionPipeline \
  --master "local[*]" \
  target/spark_fta-1.0-SNAPSHOT.jar \
  src/main/resources/transactions_data.csv \
  output/spark_core/advanced/fraud_detection_pipeline

# Spark SQL (DataFrames)
spark-submit \
  --class sparksql.routines.advanced.FraudDetectionPipeline \
  --master "local[*]" \
  target/spark_fta-1.0-SNAPSHOT.jar \
  src/main/resources/transactions_data.csv \
  output/spark_sql/advanced/fraud_detection_pipeline
```

### Convenção de saída

Os resultados são gravados com `coalesce(1)`, produzindo um único arquivo por rotina:

```
output/
├── spark_core/{basic,intermediate,advanced}/<rotina>/     # arquivo texto
└── spark_sql/{basic,intermediate,advanced}/<rotina>/      # CSV com cabeçalho
```

```bash
# Visualizar resultados
cat output/spark_core/basic/amount_by_city/part-00000
cat output/spark_sql/basic/amount_by_city/part-*.csv
```

---

## 📈 Observações Técnicas

- **Precisão monetária:** valores são arredondados com `BigDecimal` e `RoundingMode.HALF_UP`, evitando erros de ponto flutuante em somatórios financeiros.
- **Particionamento:** as rotinas Spark SQL configuram `spark.sql.shuffle.partitions = 8`, adequado a execução local (o padrão de 200 gera sobrecarga desnecessária nesse volume).
- **Serialização:** as classes auxiliares implementam `Serializable`, requisito para trafegarem entre executores — o equivalente conceitual à interface `Writable` do Hadoop.
- **Tolerância a falhas de parsing:** registros malformados são descartados individualmente sem interromper o processamento.
- **Logging:** nível ajustado para `WARN` a fim de manter a saída legível durante a execução.

---

## 👥 Autores

Projeto desenvolvido em dupla (**Grupo 9**) para a disciplina de Big Data.

| Autor | Contribuição |
|---|---|
| **Renato Gouveia** — [@rpgouveia](https://github.com/rpgouveia) | Idealização da proposta e desenvolvimento das rotinas |
| **Victor Ryuki Tamezava** | Desenvolvimento das rotinas |

Ambos os autores participaram da construção das ferramentas nas duas plataformas — Hadoop MapReduce e Apache Spark.

---

## 👥 Autores

Projeto desenvolvido em dupla (**Grupo 9**), com concepção e implementação conjunta das ferramentas em Hadoop MapReduce e Apache Spark.

| Autor | GitHub |
|---|---|
| Renato Gouveia | [@rpgouveia](https://github.com/rpgouveia) |
| Victor Ryuki Tamezava | [@VicRuk](https://github.com/VicRuk) |

---

## 📄 Licença

Distribuído sob a licença MIT. Consulte o arquivo [LICENSE](LICENSE) para mais detalhes.