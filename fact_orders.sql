WITH raw_data AS (
   SELECT
       PURCHASE_KEY as order_id,
       CUSTOMER_KEY as customer_id,
       TIME_ORDER_RECEIVED_UTC as order_timestamp,
       DELIVERY_DISTANCE_LINE_METERS as delivery_distance,
       WOLT_SERVICE_FEE as wolt_service_fee,
       COURIER_BASE_FEE as courier_base_fee,
       TOTAL_BASKET_VALUE as total_basket_value,
       item_id,
       item_count as quantity
   FROM raw_orders
),



SELECT
   p.order_id,
   p.customer_id,
   p.order_date,
   p.delivery_distance,
   p.wolt_service_fee,
   p.courier_base_fee,
   p.total_basket_value,
   p.item_id,
   i.item_category,
   SUM(p.quantity * i.price) AS revenue


FROM raw_data p

LEFT JOIN dim_items i ON p.item_id = i.item_id
   and p.order_date between i.start_date and i.end_date

GROUP BY p.order_id, i.item_id, i.item_category;