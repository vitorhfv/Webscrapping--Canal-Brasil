select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      -- tests/no_duplicate_titulo_hora.sql
SELECT
  titulo,
  dia_hora,
  COUNT(*) as qtd
FROM canal_brasil_silver
GROUP BY titulo, dia_hora
HAVING COUNT(*) > 1
      
    ) dbt_internal_test