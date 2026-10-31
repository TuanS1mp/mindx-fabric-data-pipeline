#!/usr/bin/env python
# coding: utf-8

# ## nb_silver_gold_processing
# 
# null

# In[3]:


from pyspark.sql.functions import (
    col, from_json, explode, year, month, to_date, 
    when, count, countDistinct, sum as _sum, round as _round
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType, ArrayType
)

print("=== BẮT ĐẦU XỬ LÝ TẦNG SILVER & GOLD ===")

# -------------------------------------------------------------------------
# STEP 1: ĐỌC DỮ LIỆU TỪ TẦNG BRONZE (PARQUET)
# -------------------------------------------------------------------------
df_sales_bronze = spark.read.parquet("abfss://DED_Final_Project_Tuan@onelake.dfs.fabric.microsoft.com/mindx_retail_lakehouse.Lakehouse/Files/Bronze/mindx_raw_sales_data.parque")
df_exchange_bronze = spark.read.parquet("abfss://DED_Final_Project_Tuan@onelake.dfs.fabric.microsoft.com/mindx_retail_lakehouse.Lakehouse/Files/Bronze/exchange_rate_2425.parquet")

# -------------------------------------------------------------------------
# STEP 2: TẦNG SILVER - LỌC QUARANTINE & BÓC TÁCH JSON
# -------------------------------------------------------------------------

# 1. Tách dữ liệu rác/lỗi (Quarantine): Failed HOẶC Feedback score ngoài [1, 5]
quarantine_condition = (col("order_status") == "Failed") | (col("feedback_score").isNotNull() & (~col("feedback_score").between(1, 5)))

df_sales_quarantine = df_sales_bronze.filter(quarantine_condition)
df_sales_clean_raw = df_sales_bronze.filter(~quarantine_condition)

# Ghi bảng Quarantine và Exchange Rate vào Delta Table
df_sales_quarantine.write.mode("overwrite").format("delta").saveAsTable("silver_sales_quarantine")
df_exchange_bronze.write.mode("overwrite").format("delta").saveAsTable("silver_exchange_rate")

# 2. Bóc tách mảng JSON ở cột items cho dữ liệu sạch
item_schema = ArrayType(StructType([
    StructField("product_id", StringType(), True),
    StructField("category", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("quantity", IntegerType(), True)
]))

df_sales_parsed = df_sales_clean_raw \
    .withColumn("order_date_dt", to_date(col("order_date"))) \
    .withColumn("year", year(col("order_date_dt"))) \
    .withColumn("month", month(col("order_date_dt"))) \
    .withColumn("parsed_items", from_json(col("items"), item_schema))

# Explode mảng items thành từng dòng sản phẩm chi tiết
df_silver_sales = df_sales_parsed.select(
    col("order_id"),
    col("order_date_dt").alias("order_date"),
    col("year"),
    col("month"),
    col("location"),
    col("discount_code"),
    col("order_status"),
    col("feedback_score"),
    explode(col("parsed_items")).alias("item")
).select(
    col("order_id"),
    col("order_date"),
    col("year"),
    col("month"),
    col("location"),
    col("discount_code"),
    col("order_status"),
    col("feedback_score"),
    col("item.product_id").alias("product_id"),
    col("item.category").alias("category"),
    col("item.price").alias("price_usd"),
    col("item.quantity").alias("quantity"),
    (col("item.price") * col("item.quantity")).alias("revenue_usd")
)

# Ghi dữ liệu sạch vào Delta Table silver_sales
df_silver_sales.write.mode("overwrite").format("delta").saveAsTable("silver_sales")
print("✅ Hoàn thành Tầng Silver: Đã lưu silver_sales & silver_sales_quarantine")

# -------------------------------------------------------------------------
# STEP 3: TẦNG GOLD - TÍNH TOÁN BÁO CÁO KINH DOANH
# -------------------------------------------------------------------------

# Join silver_sales với silver_exchange_rate
# Join silver_sales với silver_exchange_rate
df_gold_joined = df_silver_sales.join(
    df_exchange_bronze,
    ["year", "month"],
    "left"
)

# Metric 1: Doanh thu VNĐ hàng tháng theo Category
gold_monthly_revenue_vnd = df_gold_joined.groupBy("year", "month", "category") \
    .agg(
        _round(_sum("revenue_usd"), 2).alias("total_revenue_usd"),
        _round(_sum(col("revenue_usd") * col("exchange_rate")), 0).alias("total_revenue_vnd")
    ) \
    .orderBy("year", "month", "category")

gold_monthly_revenue_vnd.write.mode("overwrite").format("delta").saveAsTable("gold_monthly_revenue_vnd")

# Metric 2: Tỷ lệ % số đơn hàng có sử dụng mã giảm giá theo Khu vực
gold_discount_by_location = df_sales_clean_raw.groupBy("location") \
    .agg(
        countDistinct("order_id").alias("total_orders"),
        countDistinct(when(col("discount_code").isNotNull() & (col("discount_code") != ""), col("order_id"))).alias("discount_orders")
    ) \
    .withColumn("discount_rate_pct", _round((col("discount_orders") / col("total_orders")) * 100, 2)) \
    .orderBy(col("discount_rate_pct").desc())

gold_discount_by_location.write.mode("overwrite").format("delta").saveAsTable("gold_discount_by_location")

print("✅ Hoàn thành Tầng Gold: Đã lưu gold_monthly_revenue_vnd & gold_discount_by_location")

