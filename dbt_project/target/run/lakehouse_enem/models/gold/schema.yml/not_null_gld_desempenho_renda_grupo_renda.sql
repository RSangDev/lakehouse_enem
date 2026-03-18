
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select grupo_renda
from "lakehouse"."main"."gld_desempenho_renda"
where grupo_renda is null



  
  
      
    ) dbt_internal_test