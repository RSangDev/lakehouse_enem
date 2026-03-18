
    
    

with all_values as (

    select
        grupo_renda as value_field,
        count(*) as n_records

    from "lakehouse"."main"."gld_desempenho_renda"
    group by grupo_renda

)

select *
from all_values
where value_field not in (
    'Baixa','Média','Alta'
)


