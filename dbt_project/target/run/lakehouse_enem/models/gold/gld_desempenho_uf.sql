
  
  create view "lakehouse"."main"."gld_desempenho_uf__dbt_tmp" as (
    -- models/gold/gld_desempenho_uf.sql
-- Agrega desempenho médio por UF e ano
-- Demonstra que o dbt consome o Silver diretamente do DuckDB (que lê o Parquet do Delta Lake)

SELECT
    sg_uf_residencia                        AS uf,
    regiao,
    ano,
    COUNT(*)                                AS total_candidatos,
    ROUND(AVG(media_geral), 2)              AS media_geral,
    ROUND(AVG(nu_nota_redacao), 2)          AS media_redacao,
    ROUND(AVG(nu_nota_mt), 2)               AS media_matematica,
    ROUND(AVG(nu_nota_cn), 2)               AS media_ciencias,
    ROUND(AVG(nu_nota_ch), 2)               AS media_humanas,
    ROUND(AVG(nu_nota_lc), 2)               AS media_linguagens,
    ROUND(AVG(percentil_nacional), 2)       AS percentil_medio,
    SUM(fl_escola_publica)                  AS n_escola_publica,
    ROUND(
        100.0 * SUM(fl_escola_publica) / COUNT(*), 1
    )                                       AS pct_escola_publica,
    ROUND(
        100.0 * SUM(CASE WHEN faixa_desempenho = 'Excelente' THEN 1 ELSE 0 END) / COUNT(*), 1
    )                                       AS pct_excelente

FROM "lakehouse"."silver"."enem"
WHERE media_geral IS NOT NULL
GROUP BY 1, 2, 3
ORDER BY ano, media_geral DESC
  );
