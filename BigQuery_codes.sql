--Cryptocurrencies with the Highest 24h Price Change
SELECT DISTINCT
    id , symbol, name, price_change_24h
FROM `airflow-data-pipeline-448107.crypto_db.crypto_tb`
ORDER BY ABS(price_change_24h) DESC
LIMIT 10;


--Average Price and Supply for Each Cryptocurrency
SELECT 
    symbol, 
    name, 
    AVG(current_price) AS avg_price, 
    AVG(total_supply) AS avg_total_supply
FROM `airflow-data-pipeline-448107.crypto_db.crypto_tb`
GROUP BY symbol, name
ORDER BY avg_price DESC;


--Latest Price for Each Cryptocurrency

WITH LatestPrices AS (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY last_updated DESC) AS rn
    FROM `airflow-data-pipeline-448107.crypto_db.crypto_tb`
)
SELECT 
    id, symbol, name, current_price, last_updated
FROM LatestPrices
WHERE rn = 1
ORDER BY current_price DESC;


