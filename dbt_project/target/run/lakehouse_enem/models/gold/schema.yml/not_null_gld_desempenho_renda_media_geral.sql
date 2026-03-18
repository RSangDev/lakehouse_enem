
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select media_geral
from "lakehouse"."main"."gld_desempenho_renda"
where media_geral is null



  
  
      
    ) dbt_internal_test