# ITEMS DIMENSION
MERGE INTO dim_items AS d
USING raw_items AS r
ON d.item_id = r.ITEM_KEY


-- Step 1: Deactivate the existing record by updating end_date and is_active flag
WHEN MATCHED AND (
   d.is_active = TRUE
   AND (
       d.brand_name <> r.brand_name OR
       d.item_category <> r.item_category OR
       d.price <> r.price OR
       d.vat_rate <> r.vat_rate
   )
)
THEN
   UPDATE SET
       d.is_active = FALSE,
       d.end_date = r.start_date


-- Step 2: Insert a new record for the updated data
WHEN MATCHED AND (
   d.is_active = TRUE
   AND (
       d.brand_name <> r.brand_name OR
       d.item_category <> r.item_category OR
       d.price <> r.price OR
       d.vat_rate <> r.vat_rate
   )
)
THEN
   INSERT (item_id, item_name, brand_name, item_category, price, vat_rate, start_date, end_date, is_active)
   VALUES (r.ITEM_KEY, r.item_name, r.brand_name, r.item_category, r.price, r.vat_rate, r.start_date, NULL, TRUE)


-- Step 3: Insert new records if they don't already exist in the target
WHEN NOT MATCHED BY TARGET
THEN
   INSERT (item_id, item_name, brand_name, item_category, price, vat_rate, start_date, end_date, is_active)
   VALUES (r.ITEM_KEY, r.item_name, r.brand_name, r.item_category, r.price, r.vat_rate, r.start_date, NULL, TRUE);
