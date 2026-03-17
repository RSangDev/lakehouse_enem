"""
spark_jobs/time_travel.py
Demonstra o poder do Time Travel no Delta Lake, 
que é a capacidade deconsultar e restaurar versões anteriores dos dados sem esforço.

Time Travel permite consultar como os dados estavam em qualquer
versão anterior ou timestamp — sem backups manuais, com custo zero.

Demonstra:
- Consulta por versão (VERSION AS OF)
- Consulta por timestamp (TIMESTAMP AS OF)
- Comparação entre versões
- ROLLBACK: restaurar versão anterior
- HISTORY: auditoria completa de todas as operações
"""

import os
import logging
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

ROOT       = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DELTA_DIR  = os.path.join(ROOT, "data", "delta")
SILVER_PATH = os.path.join(DELTA_DIR, "silver", "enem")


def get_spark() -> SparkSession:
    builder = (
        SparkSession.builder
        .appName("lakehouse_enem_time_travel")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.shuffle.partitions", "4")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


def demo_time_travel() -> dict:
    """
    Executa todas as demos de Time Travel e retorna resultados
    para exibição no dashboard.
    """
    spark = get_spark()
    spark.sparkContext.setLogLevel("WARN")
    results = {}

    # ── 1. History: todas as operações na tabela ──────────────────
    logger.info("=" * 55)
    logger.info("DEMO 1: Delta Lake History (auditoria completa)")
    logger.info("=" * 55)
    delta_table = DeltaTable.forPath(spark, SILVER_PATH)
    history_df  = delta_table.history()
    history     = history_df.select(
        "version", "timestamp", "operation", "operationParameters"
    ).orderBy("version")
    history.show(truncate=False)
    results["history"] = history.toPandas()

    # ── 2. Lê versão atual ────────────────────────────────────────
    logger.info("=" * 55)
    logger.info("DEMO 2: Lendo versão atual")
    logger.info("=" * 55)
    current = spark.read.format("delta").load(SILVER_PATH)
    n_current = current.count()
    media_atual = current.agg(F.round(F.avg("media_geral"), 2)).collect()[0][0]
    logger.info(f"  Versão atual: {n_current:,} registros | média geral: {media_atual}")
    results["n_atual"] = n_current
    results["media_atual"] = media_atual

    # ── 3. Time Travel por versão ─────────────────────────────────
    logger.info("=" * 55)
    logger.info("DEMO 3: Time Travel — VERSION AS OF 0")
    logger.info("=" * 55)
    try:
        v0 = (
            spark.read
            .format("delta")
            .option("versionAsOf", 0)        # ← versão inicial
            .load(SILVER_PATH)
        )
        n_v0 = v0.count()
        has_percentil = "percentil_nacional" in v0.columns
        logger.info(f"  Versão 0: {n_v0:,} registros | tem percentil_nacional: {has_percentil}")
        results["n_v0"] = n_v0
        results["v0_tem_percentil"] = has_percentil
    except Exception as e:
        logger.warning(f"  Versão 0 indisponível: {e}")
        results["n_v0"] = None

    # ── 4. Time Travel por timestamp ──────────────────────────────
    logger.info("=" * 55)
    logger.info("DEMO 4: Time Travel — TIMESTAMP AS OF (1 hora atrás)")
    logger.info("=" * 55)
    # Pega o timestamp da versão 0 do histórico
    try:
        ts_v0 = history_df.filter("version = 0").select("timestamp").collect()[0][0]
        ts_str = ts_v0.strftime("%Y-%m-%d %H:%M:%S")
        df_timestamp = (
            spark.read
            .format("delta")
            .option("timestampAsOf", ts_str)  # ← timestamp exato
            .load(SILVER_PATH)
        )
        n_ts = df_timestamp.count()
        logger.info(f"  Timestamp {ts_str}: {n_ts:,} registros")
        results["timestamp_v0"] = ts_str
        results["n_timestamp"] = n_ts
    except Exception as e:
        logger.warning(f"  Timestamp indisponível: {e}")
        results["n_timestamp"] = None

    # ── 5. Comparação entre versões ───────────────────────────────
    logger.info("=" * 55)
    logger.info("DEMO 5: Comparação entre versões (diff)")
    logger.info("=" * 55)
    try:
        v0 = spark.read.format("delta").option("versionAsOf", 0).load(SILVER_PATH)
        v1 = spark.read.format("delta").option("versionAsOf", 1).load(SILVER_PATH)

        # Registros que existem na v1 mas não na v0 (novos via schema evolution)
        colunas_novas = set(v1.columns) - set(v0.columns)
        logger.info(f"  Colunas adicionadas entre v0 e v1: {colunas_novas}")
        results["colunas_novas"] = list(colunas_novas)
    except Exception as e:
        logger.warning(f"  Comparação indisponível: {e}")
        results["colunas_novas"] = []

    # ── 6. ROLLBACK: restaurar versão anterior ────────────────────
    logger.info("=" * 55)
    logger.info("DEMO 6: Como fazer ROLLBACK (sem executar)")
    logger.info("=" * 55)
    logger.info("""
  Para restaurar a versão 0:
  
    df_v0 = spark.read.format("delta").option("versionAsOf", 0).load(PATH)
    df_v0.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(PATH)
  
  Ou via SQL:
    RESTORE TABLE delta.`/path/to/table` TO VERSION AS OF 0
  """)
    results["rollback_exemplo"] = "RESTORE TABLE delta.`path` TO VERSION AS OF 0"

    spark.stop()
    logger.info("Time Travel demo concluído.")
    return results


if __name__ == "__main__":
    demo_time_travel()
