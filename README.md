# Financial Exchange Rate Medallion Pipeline (PySpark & Databricks)

Pipeline de Engenharia de Dados em padrão de produção construído sobre a **Arquitetura Medallion (Bronze, Silver e Gold)** no **Databricks**, utilizando **PySpark** e **Delta Lake**. 

O pipeline consome cotações de moedas em tempo real da API REST da AwesomeAPI, aplica limpezas, conversões de tipo, regras de negócio para monitoramento de risco cambial e gera agregações executivas para consumo por ferramentas de BI.

---

## Arquitetura do Pipeline
[ AwesomeAPI ] ──(REST)─> [ Bronze Layer ] ──(PySpark)──> [ Silver Layer ] ──(PySpark)──> [ Gold Layer ]
(Raw Data)                   (Clean & Enriched)             (Aggregated)

<img width="1239" height="588" alt="pipeline_dag" src="https://github.com/user-attachments/assets/54e9bc39-ca35-42dd-b3ab-ca1a8a880f61" />

1. **Bronze Layer (`bronze_cotacoes`):**
   * Ingestão incremental de dados brutos em formato JSON via requisições HTTP REST.
   * Parametrização dinâmica com `dbutils.widgets` para gestão segura de tokens e moedas.
   * Otimização de inferência de schema via conversão intermediária em `pandas.DataFrame`, eliminando gargalos no driver Spark.
   * Persistência em `.mode("append")` no Delta Lake.

2. **Silver Layer (`silver_cotacoes`):**
   * Limpeza, padronização e conversão de tipos (`string` para `double` e `timestamp`).
   * Deduplicação em chave composta: `dropDuplicates(["code", "create_date"])`.
   * **Regra de Negócio de Risco:** Engenharia de atributos com `pyspark.sql.functions.when` e `abs()` para categorizar volatilidade cambial (`ALTO RISCO` vs `NORMAL`).
   * Persistência em `.mode("overwrite")`.

3. **Gold Layer (`gold_resumo_cambio`):**
   * Agregação executiva de métricas por moeda (`code`).
   * Cálculo de média de compra (`avg_bid` arredondado a 4 casas decimais), máximo (`max_high`), mínimo (`min_low`) e volume de eventos (`event_count`).
   * Tabela Delta otimizada para integração direta com Power BI ou Tableau.

---

## Otimização & Performance

* **Redução de Latência:** Otimização na inferência de schema no driver reduzindo o tempo total de execução do Job no Databricks Workflows de **~10 minutos para apenas 39 segundos** (redução de ~93% no consumo de computação/DBUs).
* **Orquestração Automática:** Job configurado no **Databricks Workflows (Jobs)** com dependências encadeadas (`Ingestao_Bronze` ────> `Transformacao_Silver` ────> `Agregacao_Gold`).

---

## Tecnologias Utilizadas

* **Linguagens:** Python, PySpark, SQL
* **Plataforma & Armazenamento:** Databricks Community Edition (AWS), Delta Lake
* **Orquestração:** Databricks Workflows (Jobs)
* **Bibliotecas:** `requests`, `pandas`, `pyspark.sql.functions`

---

## Estrutura do Repositório

```text
├── notebooks/
│   ├── 01_ingestao_bronze.py
│   ├── 02_transformacao_silver.py
│   └── 03_camada_gold.py
├── docs/
│   └── pipeline_dag.png
└── README.md
```
---

## 👤 Autor

**Bernardo Martins**  
*Estudante de Ciência da Computação @ UFRRJ | Data Engineering*  
[LinkedIn](https://www.linkedin.com/in/bernardo-martins-da-silva) | [GitHub](https://github.com/bernardomartins21)
