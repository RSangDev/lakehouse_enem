"""
spark_jobs/silver.py
Transforma a camada Bronze em Silver no Delta Lake.

Demonstra:
- Leitura do Delta Lake
- Transformações com PySpark
- Schema Evolution: adiciona coluna nova sem recriar a tabela
- ACID Transactions: escrita atômica com merge
- Particionamento por ano + região
"""

import os
import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

ROOT      = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DELTA_DIR = os.path.join(ROOT, "data", "delta")

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
    """Aplica todas as transformações bronze → silver."""

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

    # 3. Média geral (objetivas + redação)
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

    # 7. Auditorias
    df = df.withColumn("_silver_updated_at", F.current_timestamp())

    return df.drop("_bronze_loaded_at", "_source_file")


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

    # ── ACID Transaction via MERGE ────────────────────────────────
    # Se a tabela Silver já existe, faz UPSERT (update or insert)
    # Isso é uma ACID transaction — garante consistência mesmo se o job falhar
    if DeltaTable.isDeltaTable(spark, SILVER_PATH):
        logger.info("Silver já existe — executando MERGE (ACID transaction)...")
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

    # ── Schema Evolution: adiciona coluna nova sem recriar ────────
    # Simula uma evolução do schema: adicionamos percentil nacional
    logger.info("Demonstrando Schema Evolution...")
    from pyspark.sql.window import Window
    window = Window.partitionBy("ano").orderBy("media_geral")
    silver_com_percentil = (
        spark.read.format("delta").load(SILVER_PATH)
        .withColumn(
            "percentil_nacional",
            F.round(F.percent_rank().over(window) * 100, 1)
        )
    )
    (
        silver_com_percentil.write
        .format("delta")
        .mode("overwrite")
        .option("mergeSchema", "true")   # ← schema evolution aqui
        .partitionBy("ano", "regiao")
        .save(SILVER_PATH)
    )
    logger.info("  → Coluna 'percentil_nacional' adicionada via schema evolution")

    spark.stop()
    logger.info("Silver concluído.")


if __name__ == "__main__":
    run_silver()
