select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select dia_hora
from "postgres"."public"."canal_brasil_silver"
where dia_hora is null



      
    ) dbt_internal_test