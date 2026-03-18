"""
spark_jobs/silver.py
Transforma a camada Bronze em Silver no Delta Lake.

Demonstra:
- Leitura do Delta Lake
- Transformações com PySpark
- ACID Transactions: escrita atômica com merge
- Schema Evolution: adiciona coluna nova sem recriar a tabela
- Particionamento por ano + região
"""

import os
import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# ─── Windows: configura HADOOP_HOME antes de importar PySpark ────
import sys as _sys, os as _os
_sys.path.insert(0, _os.path.dirname(_os.path.abspath(__file__)))
from _winutils import setup_hadoop_home
setup_hadoop_home()
# ──────────────────────────────────────────────────────────────────

from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

ROOT        = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DELTA_DIR   = os.path.join(ROOT, "data", "delta")
BRONZE_PATH = os.path.join(DELTA_DIR, "bronze", "enem")
SILVER_PATH = os.path.join(DELTA_DIR, "silver", "enem")


def get_spark() -> SparkSession:
    builder = (
        SparkSession.builder
        .appName("lakehouse_enem_silver")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.shuffle.partitions", "4")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


def transform_silver(df: DataFrame) -> DataFrame:
    """Aplica transformações bronze → silver. NÃO inclui percentil_nacional aqui."""

    # 1. Remove ausentes e treineiros
    df = df.filter(
        (F.col("tp_presenca") == "Presente") &
        (F.col("in_treineiro") == 0)
    )

    # 2. Média das notas objetivas
    notas_obj = ["nu_nota_cn", "nu_nota_ch", "nu_nota_lc", "nu_nota_mt"]
    df = df.withColumn(
        "media_objetivas",
        F.round((sum(F.col(c) for c in notas_obj)) / 4, 2)
    )

    # 3. Média geral
    df = df.withColumn(
        "media_geral",
        F.round(
            (F.col("media_objetivas") * 4 + F.col("nu_nota_redacao")) / 5, 2
        )
    )

    # 4. Faixa de desempenho
    df = df.withColumn(
        "faixa_desempenho",
        F.when(F.col("media_geral") >= 700, "Excelente")
         .when(F.col("media_geral") >= 600, "Bom")
         .when(F.col("media_geral") >= 500, "Regular")
         .when(F.col("media_geral") >= 400, "Abaixo da Média")
         .otherwise("Insuficiente")
    )

    # 5. Flag escola pública
    df = df.withColumn(
        "fl_escola_publica",
        F.when(F.col("tp_escola").contains("Pública"), 1).otherwise(0)
    )

    # 6. Grupo de renda simplificado
    df = df.withColumn(
        "grupo_renda",
        F.when(F.col("renda_familiar").isin(["Nenhuma", "Até 1 SM"]), "Baixa")
         .when(F.col("renda_familiar").isin(["1-2 SM", "2-5 SM"]), "Média")
         .otherwise("Alta")
    )

    # 7. Auditoria
    df = df.withColumn("_silver_updated_at", F.current_timestamp())

    return df.drop("_bronze_loaded_at", "_source_file")


def add_percentil(spark: SparkSession) -> None:
    """
    Schema Evolution: adiciona percentil_nacional à tabela Silver já existente.
    Feito APÓS o MERGE para que o source e target tenham o mesmo schema.
    """
    logger.info("Schema Evolution: calculando percentil_nacional...")
    df = spark.read.format("delta").load(SILVER_PATH)

    window = Window.partitionBy("ano").orderBy("media_geral")
    df_com_percentil = df.withColumn(
        "percentil_nacional",
        F.round(F.percent_rank().over(window) * 100, 1)
    )

    (
        df_com_percentil.write
        .format("delta")
        .mode("overwrite")
        .option("mergeSchema", "true")   # ← schema evolution aqui
        .partitionBy("ano", "regiao")
        .save(SILVER_PATH)
    )
    logger.info("  → percentil_nacional adicionado via schema evolution")


def run_silver() -> None:
    spark = get_spark()
    spark.sparkContext.setLogLevel("WARN")

    logger.info("Lendo Bronze layer...")
    bronze_df = spark.read.format("delta").load(BRONZE_PATH)
    logger.info(f"  → {bronze_df.count():,} registros no Bronze")

    logger.info("Transformando para Silver...")
    silver_df = transform_silver(bronze_df)
    n = silver_df.count()
    logger.info(f"  → {n:,} registros após filtros")

    # ── STEP 1: Escreve/atualiza os dados base (sem percentil) ───
    if DeltaTable.isDeltaTable(spark, SILVER_PATH):
        logger.info("Silver já existe — executando MERGE (ACID transaction)...")

        # Garante que o source tenha o mesmo schema que o target
        # removendo colunas extras que possam existir no target (ex: percentil_nacional)
        target_cols = set(DeltaTable.forPath(spark, SILVER_PATH).toDF().columns)
        source_cols = set(silver_df.columns)
        # Adiciona colunas faltantes no source como NULL para compatibilidade
        for col in target_cols - source_cols:
            silver_df = silver_df.withColumn(col, F.lit(None).cast("double"))

        silver_table = DeltaTable.forPath(spark, SILVER_PATH)
        (
            silver_table.alias("target")
            .merge(
                silver_df.alias("source"),
                "target.nu_inscricao = source.nu_inscricao AND target.ano = source.ano"
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
        logger.info("  → MERGE concluído")
    else:
        logger.info("Criando Silver pela primeira vez...")
        (
            silver_df.write
            .format("delta")
            .mode("overwrite")
            .option("mergeSchema", "true")
            .partitionBy("ano", "regiao")
            .save(SILVER_PATH)
        )
        logger.info(f"  → Silver criado: {SILVER_PATH}")

    # ── STEP 2: Schema Evolution — adiciona percentil APÓS o merge ─
    add_percentil(spark)

    spark.stop()
    logger.info("Silver concluído.")


if __name__ == "__main__":
    run_silver()