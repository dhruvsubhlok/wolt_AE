SELECT 
  CAST(tfo.order_timestamp as date) as order_date,
  extract(year from cast(tfo.order_timestamp as date)) as order_year,
  extract(month from cast(tfo.order_timestamp as date)) as order_month,
  tfo.item_category,
  COUNT(DISTINCT tfo.order_id) as order_count,
  COUNT(tfo.item_id) as item_count,
  COUNT(case when promo_type is not null then tfo.item_id else null end) as promo_item_count,
  COUNT(DISTINCT customer_id) as cust_count,
  round(avg(revenue),2) as avg_item_revenue,
  round(avg(delivery_distance),2) as avg_distance,
  round(COUNT(tfo.item_id)/COUNT(DISTINCT tfo.customer_id),2) as average_item_rate,
  round(COUNT(case when promo_type is not null then tfo.item_id else null end)/COUNT(tfo.item_id),2) as promo_item_percentage


FROM fact_orders tfo

GROUP BY 1,2,3,4
