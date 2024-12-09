
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, DoubleType


# Initialize Spark session
spark = SparkSession.builder.appName("Parse CSV with JSON").getOrCreate()


# CREATE RAW_ITEMS TABLE
data_items = spark.read.option("header", "true") \
                .option("inferSchema", "true") \
                .option("multiline", "true") \
                .option("quote", '"') \
                .option("escape", '"') \
               .csv('s3://Wolt_snack_store_item_logs.csv')


# One of the fields has JSON format so we define schema for atomicity in our dataset
payload_schema = StructType([
   StructField("brand_name", StringType(), True),
   StructField("item_category", StringType(), True),
   StructField("item_key", StringType(), True),
   StructField("name", ArrayType(
       StructType([
           StructField("lang", StringType(), True),
           StructField("value", StringType(), True)
       ])
   ), True),
   StructField("number_of_units", DoubleType(), True),
   StructField("price_attributes", ArrayType(
       StructType([
           StructField("currency", StringType(), True),
           StructField("product_base_price", DoubleType(), True),
           StructField("vat_rate_in_percent", DoubleType(), True)
       ])
   ), True),
   StructField("time_item_created_in_source_utc", StringType(), True),
   StructField("weight_in_grams", DoubleType(), True)
])


# Parse PAYLOAD column
data_items = data_items.withColumn("parsed_payload", from_json(col("PAYLOAD"), payload_schema))


# Extract required fields
raw_items = data_items.withColumn("brand_name", col("parsed_payload.brand_name")) \
.withColumn("item_category", col("parsed_payload.item_category")) \
.withColumn("product_base_price", col("parsed_payload.price_attributes")[0]["product_base_price"]) \
.withColumn("vat_rate", col("parsed_payload.price_attributes")[0]["vat_rate_in_percent"]) \
.withColumn("weight_in_grams", col("parsed_payload.weight_in_grams")) \
          .drop("PAYLOAD", "parsed_payload")




# Filter records where base price is negative.
raw_items = raw_items.filter(col("product_base_price")>0).orderBy("ITEM_KEY", "TIME_LOG_CREATED_UTC")











# CREATE RAW_ORDERS_TABLE
data_orders = spark.read.option("header", "true") \
                .option("inferSchema", "true") \
                .option("multiline", "true") \
                .option("quote", '"') \
                .option("escape", '"') \
                .csv('s3://Wolt_snack_store_purchase_logs.csv')




# Define schema for ITEM_BASKET_DESCRIPTION
item_basket_schema = ArrayType(
   StructType([
       StructField("item_count", DoubleType(), True),
       StructField("item_key", StringType(), True)
   ])
)


# # Parse and Explode ITEM_BASKET_DESCRIPTION column
data_orders = data_orders.withColumn(
   "parsed_item_basket",
   from_json(col("item_basket_description"), item_basket_schema)
)


data_orders_exploded = data_orders.withColumn("exploded_item", explode(col("parsed_item_basket")))


raw_orders = data_orders_exploded.withColumn("item_key", col("exploded_item.item_key")) \
.withColumn("item_count", col("exploded_item.item_count")) \
.drop("item_basket_description","parsed_item_basket", "exploded_item")










# CREATE RAW_PROMOTIONS TABLE
data_promotions = spark.read.option("header", "true") \
                .option("inferSchema", "true") \
                .option("multiline", "true") \
                .option("quote", '"') \
                .option("escape", '"') \
                .csv('s3://Wolt_snack_store_promos.csv')


raw_promotions = data_promotions             


