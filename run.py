"""
run.py — Entry point do projeto Lakehouse ENEM
Executa o pipeline completo ou etapas individuais.

Uso:
  python run.py                    # pipeline completo + dashboard
  python run.py --only-pipeline    # só pipeline (sem dashboard)
  python run.py --only-dashboard   # só dashboard
  python run.py --skip-spark       # pula o Spark (DuckDB já populado)
  python run.py --anos 2021 2022   # anos específicos
"""

import argparse
import os
import sys
import subprocess
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

ROOT    = os.path.dirname(os.path.abspath(__file__))
PYTHON  = sys.executable
IS_WIN  = sys.platform == "win32"

def _bin(name: str) -> str:
    """Resolve executável do venv atual."""
    exe  = name + (".exe" if IS_WIN else "")
    full = os.path.join(os.path.dirname(PYTHON), exe)
    return full if os.path.exists(full) else name

DBT        = _bin("dbt")
STREAMLIT  = _bin("streamlit")
DBT_DIR    = os.path.join(ROOT, "dbt_project")


def run_cmd(cmd: list, cwd: str = ROOT, label: str = "") -> bool:
    label = label or " ".join(cmd[:2])
    logger.info(f"▶ {label}")
    r = subprocess.run(cmd, cwd=cwd)
    if r.returncode != 0:
        logger.error(f"✗ Falhou: {label} (código {r.returncode})")
        return False
    logger.info(f"✓ {label}")
    return True


def pipeline(anos: list[int], skip_spark: bool = False) -> bool:
    logger.info("=" * 55)
    logger.info("STEP 1/5 — Download / geração dos dados")
    logger.info("=" * 55)
    sys.path.insert(0, ROOT)
    from ingestion.download import run_download
    run_download(anos=anos)

    if not skip_spark:
        logger.info("=" * 55)
        logger.info("STEP 2/5 — Bronze (PySpark → Delta Lake)")
        logger.info("=" * 55)
        if not run_cmd([PYTHON, "spark_jobs/bronze.py"] + [str(a) for a in anos], label="bronze.py"):
            return False

        logger.info("=" * 55)
        logger.info("STEP 3/5 — Silver (Delta Lake · ACID · Schema Evolution)")
        logger.info("=" * 55)
        if not run_cmd([PYTHON, "spark_jobs/silver.py"], label="silver.py"):
            return False

        logger.info("=" * 55)
        logger.info("STEP 4/5 — Bridge Delta → DuckDB")
        logger.info("=" * 55)
        if not run_cmd([PYTHON, "spark_jobs/delta_to_duckdb.py"], label="delta_to_duckdb.py"):
            return False
    else:
        logger.info("⏭ Spark ignorado (--skip-spark)")

    logger.info("=" * 55)
    logger.info("STEP 5/5 — dbt run + dbt test (Gold layer)")
    logger.info("=" * 55)
    os.makedirs(os.path.join(ROOT, "data", "warehouse"), exist_ok=True)
    if not run_cmd([DBT, "deps"], cwd=DBT_DIR, label="dbt deps"):
        return False
    if not run_cmd([DBT, "run",  "--profiles-dir", ".", "--project-dir", "."], cwd=DBT_DIR, label="dbt run"):
        return False
    if not run_cmd([DBT, "test", "--profiles-dir", ".", "--project-dir", "."], cwd=DBT_DIR, label="dbt test"):
        logger.warning("dbt test reportou falhas — verifique os logs")

    logger.info("Pipeline concluído.")
    return True


def dashboard() -> None:
    logger.info("Iniciando dashboard em http://localhost:8501")
    subprocess.run([STREAMLIT, "run", "dashboard/app.py"], cwd=ROOT)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--only-pipeline",  action="store_true")
    p.add_argument("--only-dashboard", action="store_true")
    p.add_argument("--skip-spark",     action="store_true", help="Pula Spark (Bronze+Silver já existem)")
    p.add_argument("--anos",           nargs="+", type=int, default=[2020, 2021, 2022])
    args = p.parse_args()

    if args.only_dashboard:
        dashboard()
    elif args.only_pipeline:
        ok = pipeline(anos=args.anos, skip_spark=args.skip_spark)
        sys.exit(0 if ok else 1)
    else:
        ok = pipeline(anos=args.anos, skip_spark=args.skip_spark)
        if ok:
            dashboard()
