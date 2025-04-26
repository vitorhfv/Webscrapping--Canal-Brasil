
  
    

  create  table "postgres"."public"."canal_brasil_silver__dbt_tmp"
  
  
    as
  
  (
    

WITH ajustado AS ( -- no script o calendario do dia vai até as 6h do dia seguinte; aqui se corrige isso
  SELECT
    *,
    CASE
      WHEN horario::time < '05:00:00' THEN data::date + INTERVAL '1 day'
      ELSE data::date
    END AS data_ajustada
  FROM canal_brasil
),
com_lag AS (
  SELECT
    *,
    LAG(titulo) OVER (ORDER BY data_ajustada, horario) AS titulo_ant,
    LAG(horario) OVER (ORDER BY data_ajustada, horario) AS horario_ant,
    LAG(data_ajustada) OVER (ORDER BY data_ajustada, horario) AS data_ant
  FROM ajustado
),
-- filtra duplicatas (programas consecutivos por dia e horário)
sem_duplicatas AS (
  SELECT *
  FROM com_lag
  WHERE NOT (
    titulo = titulo_ant AND
    horario = horario_ant AND
    data_ajustada = data_ant
  )
),
df_estrutura AS (
  SELECT
    titulo,
    data_ajustada AS data,
    horario,
    genero_ano,
    sinopse
  FROM sem_duplicatas
  ORDER BY data, horario
),
base AS (
  SELECT
    titulo,
    (data::date || ' ' || horario)::timestamp AS dia_hora,
    split_part(genero_ano, ' / ', 1) AS genero,
    sinopse,
    NULLIF(split_part(genero_ano, ' / ', 2), '') AS ano,
    NULLIF(split_part(genero_ano, ' / ', 3), '') AS nota_imdb
  FROM df_estrutura
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
  FROM base
  ORDER BY dia_hora
)
SELECT *
FROM final
WHERE dia_hora >= current_date
  );
  