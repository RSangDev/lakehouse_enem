"""
spark_jobs/bronze.py
Ingere os CSVs brutos do ENEM no Delta Lake (camada Bronze).

Demonstra:
- Escrita em formato Delta com PySpark
- Schema enforcement (rejeita dados fora do schema)
- Particionamento por ano
- Append idempotente com mergeSchema
"""

import os
import sys
import logging
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, DoubleType, TimestampType,
)
from delta import configure_spark_with_delta_pip

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

ROOT      = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW_DIR   = os.path.join(ROOT, "data", "raw")
DELTA_DIR = os.path.join(ROOT, "data", "delta")

# ─── Schema explícito — schema enforcement ────────────────────────
BRONZE_SCHEMA = StructType([
    StructField("nu_inscricao",      StringType(),  False),
    StructField("ano",               IntegerType(), False),
    StructField("sg_uf_residencia",  StringType(),  True),
    StructField("regiao",            StringType(),  True),
    StructField("tp_sexo",           StringType(),  True),
    StructField("nu_idade",          IntegerType(), True),
    StructField("tp_escola",         StringType(),  True),
    StructField("renda_familiar",    StringType(),  True),
    StructField("in_treineiro",      IntegerType(), True),
    StructField("tp_presenca",       StringType(),  True),
    StructField("nu_nota_cn",        DoubleType(),  True),
    StructField("nu_nota_ch",        DoubleType(),  True),
    StructField("nu_nota_lc",        DoubleType(),  True),
    StructField("nu_nota_mt",        DoubleType(),  True),
    StructField("nu_nota_redacao",   DoubleType(),  True),
    StructField("dt_carga",          StringType(),  True),
])


def get_spark() -> SparkSession:
    builder = (
        SparkSession.builder
        .appName("lakehouse_enem_bronze")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.shuffle.partitions", "4")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


def ingest_bronze(anos: list[int] = None) -> None:
    spark = get_spark()
    spark.sparkContext.setLogLevel("WARN")

    delta_path = os.path.join(DELTA_DIR, "bronze", "enem")

    if anos is None:
        anos = [
            int(f.replace("enem_", "").replace(".csv", ""))
            for f in os.listdir(RAW_DIR)
            if f.startswith("enem_") and f.endswith(".csv")
        ]

    for ano in sorted(anos):
        csv_path = os.path.join(RAW_DIR, f"enem_{ano}.csv")
        if not os.path.exists(csv_path):
            logger.warning(f"Arquivo não encontrado: {csv_path}")
            continue

        logger.info(f"Ingerindo bronze: ENEM {ano}...")

        df = (
            spark.read
            .option("header", "true")
            .option("inferSchema", "false")
            .schema(BRONZE_SCHEMA)
            .csv(csv_path)
        )

        # Adiciona metadados de auditoria
        df = df.withColumn("_bronze_loaded_at", F.current_timestamp())
        df = df.withColumn("_source_file", F.lit(f"enem_{ano}.csv"))

        n = df.count()
        logger.info(f"  → {n:,} registros lidos")

        # Escreve em Delta — particionado por ano
        (
            df.write
            .format("delta")
            .mode("overwrite")  # idempotente: reprocessa se necessário
            .option("replaceWhere", f"ano = {ano}")
            .option("mergeSchema", "true")
            .partitionBy("ano")
            .save(delta_path)
        )
        logger.info(f"  → Delta bronze escrito: {delta_path}/ano={ano}/")

    spark.stop()
    logger.info("Bronze concluído.")


if __name__ == "__main__":
    anos = [int(a) for a in sys.argv[1:]] if len(sys.argv) > 1 else None
    ingest_bronze(anos)
