{{
    config(
        materialized='table'
    )
}}

WITH base AS (
  SELECT
    titulo,
    (data || ' ' || horario)::timestamp as dia_hora,
    split_part(genero_ano, ' / ', 1) AS genero,
    sinopse,
    NULLIF(split_part(genero_ano, ' / ', 2), '') AS ano,
    NULLIF(split_part(genero_ano, ' / ', 3), '') AS nota_imdb
  FROM canal_brasil
),
final AS (
  SELECT 
    titulo,
    ano,
    dia_hora,
    LEAD(dia_hora) OVER (ORDER BY dia_hora) - dia_hora AS duracao,
    genero,
    sinopse,
    nota_imdb
  FROM
    base
  ORDER BY 
    dia_hora
  )
  SELECT * FROM final
--   WHERE dia_hora >= current_date
ORDER BY dia_hora