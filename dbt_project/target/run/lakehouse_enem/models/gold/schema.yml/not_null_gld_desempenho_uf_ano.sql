
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select ano
from "lakehouse"."main"."gld_desempenho_uf"
where ano is null



  
  
      
    ) dbt_internal_test