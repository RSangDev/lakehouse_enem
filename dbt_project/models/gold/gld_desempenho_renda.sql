-- models/gold/gld_desempenho_renda.sql
-- Desempenho por faixa de renda e tipo de escola
-- Revela desigualdade educacional — narrativa forte para portfolio

SELECT
    ano,
    grupo_renda,
    tp_escola,
    fl_escola_publica,
    COUNT(*)                                AS total_candidatos,
    ROUND(AVG(media_geral), 2)              AS media_geral,
    ROUND(AVG(nu_nota_redacao), 2)          AS media_redacao,
    ROUND(AVG(nu_nota_mt), 2)               AS media_matematica,
    ROUND(AVG(percentil_nacional), 2)       AS percentil_medio,
    ROUND(
        100.0 * SUM(CASE WHEN faixa_desempenho IN ('Excelente','Bom') THEN 1 ELSE 0 END) / COUNT(*), 1
    )                                       AS pct_bom_ou_excelente

FROM {{ source('silver', 'enem') }}
WHERE media_geral IS NOT NULL
GROUP BY 1, 2, 3, 4
ORDER BY ano, grupo_renda, media_geral DESC
