"""
ingestion/download.py
Simula o download dos microdados do ENEM (dados.gov.br).
Em produção: substituir pela URL real do INEP/dados.gov.br.
Os dados simulados reproduzem a estrutura real dos microdados ENEM 2022.
"""

import os
import numpy as np
import pandas as pd
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

RAW_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "raw")

# ─── Referências ────────────────────────────────────────────────
ESTADOS = {
    "SP": {"regiao": "Sudeste", "pop": 46_000_000},
    "RJ": {"regiao": "Sudeste", "pop": 17_000_000},
    "MG": {"regiao": "Sudeste", "pop": 21_000_000},
    "RS": {"regiao": "Sul",     "pop": 11_000_000},
    "PR": {"regiao": "Sul",     "pop": 11_000_000},
    "SC": {"regiao": "Sul",     "pop":  7_000_000},
    "BA": {"regiao": "Nordeste","pop": 15_000_000},
    "CE": {"regiao": "Nordeste","pop":  9_000_000},
    "PE": {"regiao": "Nordeste","pop":  9_000_000},
    "MA": {"regiao": "Nordeste","pop":  7_000_000},
    "PA": {"regiao": "Norte",   "pop":  8_000_000},
    "AM": {"regiao": "Norte",   "pop":  4_000_000},
    "GO": {"regiao": "Centro-Oeste", "pop": 7_000_000},
    "DF": {"regiao": "Centro-Oeste", "pop": 3_000_000},
    "MT": {"regiao": "Centro-Oeste", "pop": 3_000_000},
}

TIPOS_ESCOLA = ["Pública Federal", "Pública Estadual", "Pública Municipal", "Privada"]
AREAS = ["CN", "CH", "LC", "MT"]  # Ciências Naturais, Humanas, Linguagens, Matemática


def gerar_enem(n: int = 50_000, ano: int = 2022, seed: int = 42) -> pd.DataFrame:
    np.random.seed(seed)
    logger.info(f"Gerando {n:,} registros ENEM {ano}...")

    ufs       = list(ESTADOS.keys())
    pesos_uf  = [ESTADOS[uf]["pop"] for uf in ufs]
    pesos_uf  = [p / sum(pesos_uf) for p in pesos_uf]

    uf_col    = np.random.choice(ufs, size=n, p=pesos_uf)
    escola    = np.random.choice(TIPOS_ESCOLA, size=n, p=[0.05, 0.55, 0.05, 0.35])
    sexo      = np.random.choice(["M", "F"], size=n)
    idade     = np.clip(np.random.normal(18, 3, n).astype(int), 14, 65)
    renda_fam = np.random.choice(
        ["Nenhuma", "Até 1 SM", "1-2 SM", "2-5 SM", "5-10 SM", "Acima 10 SM"],
        size=n, p=[0.03, 0.25, 0.30, 0.25, 0.12, 0.05],
    )
    treineiro  = np.random.choice([0, 1], size=n, p=[0.92, 0.08])
    presenca   = np.random.choice(["Presente", "Ausente", "Eliminado"], size=n, p=[0.80, 0.18, 0.02])

    # Nota base influenciada por tipo de escola e renda
    bonus_escola = {"Privada": 60, "Pública Federal": 30,
                    "Pública Estadual": 0, "Pública Municipal": -20}
    bonus_renda  = {"Nenhuma": -40, "Até 1 SM": -20, "1-2 SM": 0,
                    "2-5 SM": 20,   "5-10 SM": 50,   "Acima 10 SM": 80}

    nota_base = np.array([
        500
        + bonus_escola[escola[i]]
        + bonus_renda[renda_fam[i]]
        + np.random.normal(0, 60)
        for i in range(n)
    ])

    rows = {
        "nu_inscricao":     [f"{ano}{i+1000000:08d}" for i in range(n)],
        "ano":              ano,
        "sg_uf_residencia": uf_col,
        "regiao":           [ESTADOS[uf]["regiao"] for uf in uf_col],
        "tp_sexo":          sexo,
        "nu_idade":         idade,
        "tp_escola":        escola,
        "renda_familiar":   renda_fam,
        "in_treineiro":     treineiro,
        "tp_presenca":      presenca,
    }

    # Notas por área — NaN se ausente
    for area in AREAS:
        variacao = np.random.normal(0, 40, n)
        notas = np.clip(nota_base + variacao, 0, 1000).round(1)
        notas = np.where(presenca == "Presente", notas, np.nan)
        rows[f"nu_nota_{area.lower()}"] = notas

    # Nota redação
    red = np.clip(nota_base * 0.9 + np.random.normal(0, 80, n), 0, 1000)
    red = np.round(red / 40) * 40  # redação é múltiplo de 40
    rows["nu_nota_redacao"] = np.where(presenca == "Presente", red, np.nan)

    rows["dt_carga"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    df = pd.DataFrame(rows)
    logger.info(f"  → {len(df):,} registros gerados")
    return df


def run_download(anos: list[int] = [2022], n_por_ano: int = 50_000) -> dict:
    os.makedirs(RAW_DIR, exist_ok=True)
    result = {}
    for ano in anos:
        df = gerar_enem(n=n_por_ano, ano=ano, seed=ano)
        path = os.path.join(RAW_DIR, f"enem_{ano}.csv")
        df.to_csv(path, index=False)
        logger.info(f"Salvo: {path}")
        result[ano] = df
    return result


if __name__ == "__main__":
    run_download(anos=[2020, 2021, 2022], n_por_ano=50_000)
