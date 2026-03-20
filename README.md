**Delta Lake · PySpark · dbt · DuckDB · Streamlit**  
Arquitetura Lakehouse com ACID transactions, Time Travel e Schema Evolution — 100% local

---

## Por que este projeto existe

> Delta Lake é a base do Databricks — a plataforma dominante no mercado enterprise de dados. Este projeto demonstra os três diferenciais que entrevistadores sempre perguntam e poucos candidatos conseguem mostrar na prática: **Time Travel**, **Schema Evolution** e **ACID transactions**.

---

## Arquitetura

```
ENEM (dados.gov.br)
    │
    ▼ ingestion/download.py
data/raw/enem_{ano}.csv
    │
    ▼ spark_jobs/bronze.py  ──  PySpark · schema enforcement · partitionBy(ano)
Delta Lake Bronze  (data/delta/bronze/enem/)
    │
    ▼ spark_jobs/silver.py  ──  limpeza · ACID MERGE · Schema Evolution
Delta Lake Silver  (data/delta/silver/enem/)
    │                           ↑ _delta_log/ guarda todo o histórico
    ▼ spark_jobs/delta_to_duckdb.py
Parquet  →  DuckDB view silver.enem
    │
    ▼ dbt run
DuckDB Gold  ──  gld_desempenho_uf · gld_desempenho_renda · gld_evolucao_anual
    │
    ▼ Streamlit
Dashboard  (localhost:8501)
```

---

## Início rápido

```bash
# 1. Clonar e criar ambiente
git clone https://github.com/seu-usuario/lakehouse-enem
cd lakehouse-enem
python -m venv .venv
.venv\Scripts\activate        # Windows
pip install -r requirements.txt

# 2. Pipeline completo + dashboard
python run.py

# 3. Opções
python run.py --only-pipeline              # só pipeline
python run.py --only-dashboard             # só dashboard
python run.py --skip-spark                 # pula Spark (Delta já existe)
python run.py --anos 2021 2022             # anos específicos

# 4. Demo de Time Travel isolado
python spark_jobs/time_travel.py
```

---

## Features do Delta Lake demonstradas

| Feature | Arquivo | Como funciona |
|---|---|---|
| **ACID Transactions** | `spark_jobs/silver.py` | MERGE garante consistência — job interrompido não deixa estado parcial |
| **Time Travel (VERSION)** | `spark_jobs/time_travel.py` | `.option("versionAsOf", 0)` lê qualquer versão anterior |
| **Time Travel (TIMESTAMP)** | `spark_jobs/time_travel.py` | `.option("timestampAsOf", "2024-01-01")` lê por data |
| **Schema Evolution** | `spark_jobs/silver.py` | `.option("mergeSchema", "true")` adiciona `percentil_nacional` sem recriar |
| **History / Auditoria** | `spark_jobs/time_travel.py` | `DeltaTable.history()` — quem escreveu, quando, qual operação |
| **ROLLBACK** | `spark_jobs/time_travel.py` | `RESTORE TABLE ... TO VERSION AS OF N` |
| **Particionamento** | `spark_jobs/bronze.py` | `partitionBy("ano")` — leitura seletiva sem scan total |

---

## Stack

| Componente | Tecnologia | Papel |
|---|---|---|
| **Armazenamento** | Delta Lake 3.2 | Formato ACID sobre Parquet com _delta_log |
| **Processamento** | PySpark 3.5 | Bronze, Silver, MERGE, Schema Evolution |
| **Transformação** | dbt-duckdb 1.8 | Gold layer: SQL analítico + testes de qualidade |
| **Query Engine** | DuckDB 0.10 | Lê Parquet nativamente, serve o dbt e o dashboard |
| **Dashboard** | Streamlit + Plotly | 5 páginas analíticas sobre o gold layer |
| **Container** | Docker Compose | Spark standalone sem instalar Java localmente |
| **CI/CD** | GitHub Actions | dbt parse → compile → run → test a cada push |

---

## Estrutura do projeto

```
lakehouse_enem/
├── run.py                          # Entry point único
├── requirements.txt
│
├── ingestion/
│   └── download.py                 # Gera microdados ENEM simulados (estrutura INEP real)
│
├── spark_jobs/
│   ├── bronze.py                   # CSV → Delta Lake Bronze (schema enforcement)
│   ├── silver.py                   # Bronze → Silver (ACID MERGE + Schema Evolution)
│   ├── delta_to_duckdb.py          # Silver Delta → Parquet → DuckDB view
│   └── time_travel.py              # Demo completo de Time Travel
│
├── dbt_project/
│   ├── dbt_project.yml
│   ├── profiles.yml                # DuckDB apontando para lakehouse.duckdb
│   ├── packages.yml
│   └── models/
│       ├── silver/
│       │   └── sources.yml         # Declara silver.enem como source
│       └── gold/
│           ├── schema.yml          # Testes de qualidade
│           ├── gld_desempenho_uf.sql
│           ├── gld_desempenho_renda.sql
│           └── gld_evolucao_anual.sql
│
├── dashboard/
│   └── app.py                      # Streamlit dark theme · 6 páginas
│
├── docker/
│   └── docker-compose.yml          # Spark standalone
│
└── data/
    ├── raw/                         # CSVs gerados
    ├── delta/                       # Delta Lake (bronze/ + silver/)
    └── warehouse/
        └── lakehouse.duckdb        # DuckDB warehouse
```

---

## dbt — Gold Layer

O dbt lê o Silver exportado para Parquet via DuckDB e constrói três modelos gold:

```
silver.enem (view DuckDB sobre Parquet)
    ├── gld_desempenho_uf       — média por UF + rankings + % escola pública
    ├── gld_desempenho_renda    — desigualdade por renda e tipo de escola
    └── gld_evolucao_anual      — YoY por região com window functions
```

```bash
cd dbt_project
dbt deps
dbt run   --profiles-dir . --project-dir .
dbt test  --profiles-dir . --project-dir .
dbt docs generate && dbt docs serve
```

---

## Extensões sugeridas

- Substituir dados simulados pelos **microdados reais do INEP** (download direto do dados.gov.br)
- Adicionar **dbt snapshots** para SCD Type 2 nas dimensões
- Configurar **Spark no cluster** substituindo `local[*]` pelo endereço do master
- Adicionar **Great Expectations** para validações estatísticas na camada Silver
- Conectar **Metabase** ao DuckDB gold para BI self-service
