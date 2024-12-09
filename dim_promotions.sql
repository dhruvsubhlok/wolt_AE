CREATE TABLE IF NOT EXISTS dim_promotions (
   item_id VARCHAR,
   promo_type VARCHAR,
   percent_discount FLOAT,
   promotion_start_date TIMESTAMP,
   promotion_end_date TIMESTAMP
);


INSERT INTO dim_promotions
SELECT
   ITEM_KEY,
   PROMO_TYPE,
   DISCOUNT_IN_PERCENTAGE,
   PROMO_START_DATE,
   PROMO_END_DATE
FROM raw_promotions;