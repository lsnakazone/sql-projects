/* Criando um total em execução usando funções de janela */
SELECT standard_amt_usd,
       SUM(standard_amt_usd) OVER (ORDER BY occurred_at) AS running_total
FROM orders

/* Criando um total em execução particionado usando funções de janela */
SELECT standard_amt_usd,
       DATE_TRUNC('year', occurred_at) as year,
       SUM(standard_amt_usd) OVER (PARTITION BY DATE_TRUNC('year', occurred_at) ORDER BY occurred_at) AS running_total
FROM orders

/* Papel total em execução por conta */
SELECT id,
       account_id,
       total,
       RANK() OVER (PARTITION BY account_id ORDER BY total DESC) AS total_rank
FROM orders

/* Aliases para múltiplas funções de janela */
SELECT id,
       account_id,
       DATE_TRUNC('year',occurred_at) AS year,
       DENSE_RANK() OVER account_year_window AS dense_rank,
       total_amt_usd,
       SUM(total_amt_usd) OVER account_year_window AS sum_total_amt_usd,
       COUNT(total_amt_usd) OVER account_year_window AS count_total_amt_usd,
       AVG(total_amt_usd) OVER account_year_window AS avg_total_amt_usd,
       MIN(total_amt_usd) OVER account_year_window AS min_total_amt_usd,
       MAX(total_amt_usd) OVER account_year_window AS max_total_amt_usd
FROM orders 
WINDOW account_year_window AS (PARTITION BY account_id ORDER BY DATE_TRUNC('year',occurred_at))

/* Comparando uma linha à linha anterior */
SELECT occurred_at,
       total_amt_usd,
       LEAD(total_amt_usd) OVER (ORDER BY occurred_at) AS lead,
       LEAD(total_amt_usd) OVER (ORDER BY occurred_at) - total_amt_usd AS lead_difference
FROM (
SELECT occurred_at,
       SUM(total_amt_usd) AS total_amt_usd
  FROM orders 
 GROUP BY 1
) sub

/* Use a funcionalidade NTILE para dividir as contas em 4 níveis em termos de quantidade de standard_qty 
em seus pedidos. Sua tabela resultante deve ter a account_id, o tempo occurred_at para cada pedido, a 
quantidade total de papel standard_qty comprado e um dos quatro níveis em uma coluna standard_quartile. */
SELECT id,
       account_id,
       occurred_at,
       standard_qty,
       NTILE(4) OVER (PARTITION BY account_id ORDER BY standard_qty) AS standard_quartile,
  FROM orders 
 ORDER BY account_id DESC


/* Use a funcionalidade NTILE para dividir as contas em dois níveis em termos de quantidade de gloss_qty 
em seus pedidos. Sua tabela resultante deve ter a account_id, o tempo occurred_at para cada pedido, a 
quantidade total de papel gloss_qty comprado e um dos dois níveis em uma coluna gloss_half. */
SELECT id,
       account_id,
       occurred_at,
       gloss_qty,
       NTILE(2) OVER (PARTITION BY account_id ORDER BY gloss_qty) AS gloss_half,
  FROM orders 
 ORDER BY account_id DESC


/* Use a funcionalidade NTILE para dividir as contas em 100 níveis em termos de quantidade de total_amt_usd 
em seus pedidos. Sua tabela resultante deve ter a account_id, o tempo occurred_at para cada pedido, a quantidade 
total de papel total_amt_usd comprado e um dos 100 níveis em uma coluna total_percentile`. */
SELECT id,
       account_id,
       occurred_at,
       total_amt_usd,
       NTILE(100) OVER (PARTITION BY account_id ORDER BY total_amt_usd) AS total_percentile
  FROM orders 
 ORDER BY account_id DESC

