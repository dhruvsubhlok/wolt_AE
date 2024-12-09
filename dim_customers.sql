CREATE TABLE IF NOT EXISTS dim_customer (
   customer_id,
   total_purchases FLOAT,
   first_purchase_date TIMESTAMP,
   last_purchase_date TIMESTAMP
);


INSERT INTO dim_customer
SELECT DISTINCT
   CUSTOMER_KEY,
   COUNT(DISTINCT PURCHASE_KEY) AS total_purchases,
   MIN(TIME_ORDER_RECEIVED_UTC) AS first_purchase_date,
   MAX(TIME_ORDER_RECEIVED_UTC) AS last_purchase_date
FROM raw_orders
GROUP BY CUSTOMER_KEY;
