
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select regiao
from "lakehouse"."main"."gld_evolucao_anual"
where regiao is null



  
  
      
    ) dbt_internal_test