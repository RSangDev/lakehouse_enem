
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select pct_escola_publica
from "lakehouse"."main"."gld_desempenho_uf"
where pct_escola_publica is null



  
  
      
    ) dbt_internal_test