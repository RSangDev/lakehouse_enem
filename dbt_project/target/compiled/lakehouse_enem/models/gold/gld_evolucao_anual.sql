-- models/gold/gld_evolucao_anual.sql
-- Evolução anual do desempenho — mostra tendências entre anos
-- Usa window function para calcular variação YoY diretamente no dbt

WITH base AS (
    SELECT
        ano,
        regiao,
        ROUND(AVG(media_geral), 2)      AS media_geral,
        ROUND(AVG(nu_nota_redacao), 2)  AS media_redacao,
        ROUND(AVG(nu_nota_mt), 2)       AS media_matematica,
        COUNT(*)                         AS n_candidatos
    FROM "lakehouse"."silver"."enem"
    WHERE media_geral IS NOT NULL
    GROUP BY ano, regiao
)

SELECT
    ano,
    regiao,
    n_candidatos,
    media_geral,
    media_redacao,
    media_matematica,
    LAG(media_geral) OVER (PARTITION BY regiao ORDER BY ano)  AS media_geral_ano_anterior,
    ROUND(
        media_geral - LAG(media_geral) OVER (PARTITION BY regiao ORDER BY ano), 2
    )                                                          AS variacao_yoy,
    ROUND(
        100.0 * (media_geral - LAG(media_geral) OVER (PARTITION BY regiao ORDER BY ano))
        / NULLIF(LAG(media_geral) OVER (PARTITION BY regiao ORDER BY ano), 0), 1
    )                                                          AS variacao_yoy_pct

FROM base
ORDER BY regiao, ano