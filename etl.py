# Databricks Notebook: ETL Example for DAB Release
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ETL Job").getOrCreate()

df = spark.read.csv("/mnt/raw/customers.csv", header=True, inferSchema=True)
clean_df = df.dropna(subset=["customer_id"]).dropDuplicates(["customer_id"])

clean_df.write.mode("overwrite").parquet("/mnt/bronze/customers_clean")
print(" ETL completed successfully.")