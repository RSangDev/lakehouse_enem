"""
spark_jobs/delta_to_duckdb.py
Exporta a camada Silver do Delta Lake para Parquet,
que o DuckDB lê nativamente para o dbt construir o Gold layer.

Fluxo:
  Delta Lake (Silver) → Parquet → DuckDB (leitura) → dbt (Gold)

Isso é a ponte entre o mundo Spark/Delta e o mundo dbt/DuckDB.
"""

import os
import logging
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
import duckdb

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

ROOT         = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DELTA_DIR    = os.path.join(ROOT, "data", "delta")
SILVER_PATH  = os.path.join(DELTA_DIR, "silver", "enem")
PARQUET_PATH = os.path.join(DELTA_DIR, "parquet", "silver_enem")
DW_PATH      = os.path.join(ROOT, "data", "warehouse", "lakehouse.duckdb")


def get_spark() -> SparkSession:
    builder = (
        SparkSession.builder
        .appName("delta_to_duckdb")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.shuffle.partitions", "4")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


def export_silver_to_parquet() -> None:
    """Converte Delta → Parquet para leitura pelo DuckDB."""
    spark = get_spark()
    spark.sparkContext.setLogLevel("WARN")

    logger.info("Lendo Silver Delta Lake...")
    df = spark.read.format("delta").load(SILVER_PATH)
    n  = df.count()
    logger.info(f"  → {n:,} registros")

    logger.info(f"Exportando para Parquet: {PARQUET_PATH}")
    os.makedirs(os.path.dirname(PARQUET_PATH), exist_ok=True)
    (
        df.write
        .mode("overwrite")
        .parquet(PARQUET_PATH)
    )
    logger.info("  → Parquet escrito")
    spark.stop()


def register_in_duckdb() -> None:
    """Registra o Parquet como view no DuckDB para o dbt consumir."""
    os.makedirs(os.path.dirname(DW_PATH), exist_ok=True)
    conn = duckdb.connect(DW_PATH)

    conn.execute("CREATE SCHEMA IF NOT EXISTS silver")

    # View que lê os arquivos Parquet diretamente
    parquet_glob = PARQUET_PATH.replace("\\", "/") + "/**/*.parquet"
    conn.execute(f"""
        CREATE OR REPLACE VIEW silver.enem AS
        SELECT * FROM read_parquet('{parquet_glob}', hive_partitioning=true)
    """)

    n = conn.execute("SELECT COUNT(*) FROM silver.enem").fetchone()[0]
    logger.info(f"  → DuckDB view silver.enem: {n:,} registros")

    conn.execute("CREATE SCHEMA IF NOT EXISTS gold")
    conn.close()
    logger.info(f"DuckDB registrado: {DW_PATH}")


def run_bridge() -> None:
    export_silver_to_parquet()
    register_in_duckdb()
    logger.info("Bridge Delta → DuckDB concluída.")


if __name__ == "__main__":
    run_bridge()
