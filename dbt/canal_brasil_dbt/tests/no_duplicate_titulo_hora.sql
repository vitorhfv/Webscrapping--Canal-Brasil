-- tests/no_duplicate_titulo_hora.sql
SELECT
  titulo,
  dia_hora,
  COUNT(*) as qtd
FROM {{ ref('canal_brasil_silver') }}
GROUP BY titulo, dia_hora
HAVING COUNT(*) > 1
