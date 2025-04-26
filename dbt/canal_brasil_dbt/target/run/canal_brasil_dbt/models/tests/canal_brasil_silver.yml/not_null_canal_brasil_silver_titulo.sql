select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select titulo
from "postgres"."public"."canal_brasil_silver"
where titulo is null



      
    ) dbt_internal_test