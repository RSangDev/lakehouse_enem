
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select uf
from "lakehouse"."main"."gld_desempenho_uf"
where uf is null



  
  
      
    ) dbt_internal_test