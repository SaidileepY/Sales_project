# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## Overview
# MAGIC
# MAGIC This notebook will show you how to create and query a table or DataFrame that you uploaded to DBFS. [DBFS](https://docs.databricks.com/user-guide/dbfs-databricks-file-system.html) is a Databricks File System that allows you to store data for querying inside of Databricks. This notebook assumes that you have a file already inside of DBFS that you would like to read from.
# MAGIC
# MAGIC This notebook is written in **Python** so the default cell type is Python. However, you can use different languages by using the `%LANGUAGE` syntax. Python, Scala, SQL, and R are all supported.

# COMMAND ----------









# File location and type
file_location = "/FileStore/tables/supermarket_sales___Sheet1-1.csv"
file_type = "csv"



# The applied options are for CSV files. For other file types, these will be ignored.
df = spark.read.format(file_type) \
  .option("inferSchema", True) \
  .option("header", True) \
  .option("sep", delimiter) \
  .load(file_location)

display(df)

# COMMAND ----------

from pyspark.sql.functions import col, to_date, hour, to_timestamp

# Convert Date to proper Date format
df = df.withColumn("Date", to_date(col("Date"), "M/d/yyyy"))

# Convert Time to Timestamp and extract Hour
df = df.withColumn("Time", to_timestamp(col("Time"), "HH:mm"))
df = df.withColumn("Hour", hour(col("Time")))

display(df)

# COMMAND ----------

from pyspark.sql.functions import sum, desc

df_branch_sales = df.groupBy("Branch").agg(sum("Total").alias("Total_Sales"))

df_branch_order=df_branch_sales.orderBy(desc("Total_Sales"))
df_branch_order.show()


# COMMAND ----------

df_peak_hours = df.groupBy("Hour").agg(sum("Total").alias("Total_Sales")).orderBy(col("Total_Sales").desc())
df_peak_hours.show()


# COMMAND ----------

df_payment_method = df.groupBy("Payment").agg(sum("Total").alias("Total_Spent")).orderBy(col("Total_Spent").desc())
df_payment_method.show()


# COMMAND ----------

from pyspark.sql.functions import dayofweek

df = df.withColumn("Day_of_Week", dayofweek(col("Date")))

df_busy_days = df.groupBy("Day_of_Week").agg(sum("Total").alias("Total_Sales")).orderBy(col("Total_Sales").desc())
df_busy_days.show()


# COMMAND ----------

#we can use display command and get a pictorial representation or can perfrom some aggregrations in the visualization asell, which will be easy to understand for the clients.
# we can also register the temp table as permenant and then do much more transformations on the new permenant table aswell.

# COMMAND ----------

from pyspark.sql import SparkSession
df.registerTempTable("temp_table") #Registering Temporary Table

df_1=spark.sql("select * from temp_table where rating>9.0")#we are trying to get the products which has rating of more than 9 so we can do more production of those items which can increase the sales
display(df_1)

# COMMAND ----------

from pyspark.sql.functions import round

grouped_df=df.groupBy("Product line").agg(sum("gross income").alias("Total_gross"))   #checking to see what type of products yeilds more income
df_round = grouped_df.withColumn("rounded_Total_sales", round(grouped_df["Total_gross"], 2))
display(df_round)

# COMMAND ----------

gender_grouped_df=df.groupby("Gender").sum("gross income")#checking to see whose spending is more
display(gender_grouped_df)

# COMMAND ----------

# entire code in single place
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, hour, to_timestamp, sum, avg, dayofweek

# Initialize Spark Session (if not already created in Databricks)
spark = SparkSession.builder.appName("SupermarketSalesETL").getOrCreate()

# Load the dataset from DBFS
file_path = "/FileStore/tables/supermarket_sales___Sheet1-1.csv"
df = spark.read.option("header", True).option("inferSchema", True).csv(file_path)

# 1️⃣ Data Cleaning
df = df.withColumn("Date", to_date(col("Date"), "M/d/yyyy"))
df = df.withColumn("Time", to_timestamp(col("Time"), "HH:mm"))
df = df.withColumn("Hour", hour(col("Time")))
df = df.withColumn("Day_of_Week", dayofweek(col("Date")))

# 2️⃣ Feature Engineering & Aggregations

# Total Sales Per Branch
df_branch_sales = df.groupBy("Branch").agg(sum("Total").alias("Total_Sales"))

# Average Basket Size (Avg Total per Transaction)
df_avg_basket = df.agg(avg("Total").alias("Avg_Basket_Size"))

# Total Spending Per Customer Type & Gender
df_customer_spend = df.groupBy("Customer type", "Gender").agg(sum("Total").alias("Total_Spending"))

# Preferred Payment Methods
df_payment_method = df.groupBy("Payment").agg(sum("Total").alias("Total_Spent"))

# Peak Sales Hours
df_peak_hours = df.groupBy("Hour").agg(sum("Total").alias("Total_Sales")).orderBy(col("Total_Sales").desc())

# Busiest Days of the Week
df_busy_days = df.groupBy("Day_of_Week").agg(sum("Total").alias("Total_Sales")).orderBy(col("Total_Sales").desc())

# Total Revenue & Profit per Branch
df_branch_financials = df.groupBy("Branch").agg(
    sum("Total").alias("Total_Revenue"),
    sum("gross income").alias("Total_Profit")
)

# Average Rating Per Product Line
df_avg_rating = df.groupBy("Product line").agg(avg("Rating").alias("Avg_Rating"))

# Most Frequently Purchased Products
df_top_products = df.groupBy("Product line").agg(sum("Quantity").alias("Total_Quantity")).orderBy(col("Total_Quantity").desc())

# Showing Results, We can do the same with display for visualization
df_branch_sales.show()
df_avg_basket.show()
df_customer_spend.show()
df_payment_method.show()
df_peak_hours.show()
df_busy_days.show()
df_branch_financials.show()
df_avg_rating.show()
df_top_products.show()

