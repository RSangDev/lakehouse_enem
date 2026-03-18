"""
spark_jobs/_winutils.py
Configura o HADOOP_HOME no Windows automaticamente.

O PySpark no Windows precisa do winutils.exe para operações de filesystem.
Este módulo baixa o binário correto e configura as variáveis de ambiente
antes de iniciar qualquer SparkSession.

Chamado automaticamente por bronze.py, silver.py, delta_to_duckdb.py e time_travel.py.
"""

import os
import sys
import urllib.request
import logging

logger = logging.getLogger(__name__)

# winutils.exe compatível com Hadoop 3.x (usado pelo PySpark 3.5)
WINUTILS_URL = (
    "https://github.com/cdarlint/winutils/raw/master/hadoop-3.3.5/bin/winutils.exe"
)
HADOOP_DLL_URL = (
    "https://github.com/cdarlint/winutils/raw/master/hadoop-3.3.5/bin/hadoop.dll"
)


def setup_hadoop_home() -> None:
    """
    Configura HADOOP_HOME e hadoop.home.dir para o PySpark no Windows.
    No Linux/Mac é no-op.
    """
    if sys.platform != "win32":
        return  # Linux/Mac não precisam de winutils

    # Pasta de destino: projeto/hadoop/bin/
    project_root  = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    hadoop_home   = os.path.join(project_root, "hadoop")
    hadoop_bin    = os.path.join(hadoop_home, "bin")
    winutils_path = os.path.join(hadoop_bin, "winutils.exe")
    hadoop_dll    = os.path.join(hadoop_bin, "hadoop.dll")

    os.makedirs(hadoop_bin, exist_ok=True)

    # Baixa winutils.exe se ainda não existe
    if not os.path.exists(winutils_path):
        logger.info("Baixando winutils.exe (necessário para PySpark no Windows)...")
        try:
            urllib.request.urlretrieve(WINUTILS_URL, winutils_path)
            logger.info(f"  → winutils.exe salvo em: {winutils_path}")
        except Exception as e:
            logger.error(
                f"Falha ao baixar winutils.exe: {e}\n"
                f"Baixe manualmente de: {WINUTILS_URL}\n"
                f"E salve em: {winutils_path}"
            )
            raise

    # Baixa hadoop.dll se ainda não existe
    if not os.path.exists(hadoop_dll):
        logger.info("Baixando hadoop.dll...")
        try:
            urllib.request.urlretrieve(HADOOP_DLL_URL, hadoop_dll)
            logger.info(f"  → hadoop.dll salvo em: {hadoop_dll}")
        except Exception as e:
            logger.warning(f"hadoop.dll não pôde ser baixado: {e} (pode funcionar sem ele)")

    # Define as variáveis de ambiente ANTES de importar pyspark
    os.environ["HADOOP_HOME"]        = hadoop_home
    os.environ["hadoop.home.dir"]    = hadoop_home

    # Adiciona hadoop/bin ao PATH para o Spark encontrar os binários
    if hadoop_bin not in os.environ.get("PATH", ""):
        os.environ["PATH"] = hadoop_bin + os.pathsep + os.environ.get("PATH", "")

    logger.info(f"HADOOP_HOME configurado: {hadoop_home}")
