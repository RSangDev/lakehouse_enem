
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select total_candidatos
from "lakehouse"."main"."gld_desempenho_uf"
where total_candidatos is null



  
  
      
    ) dbt_internal_test