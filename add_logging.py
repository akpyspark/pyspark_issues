from pyspark.sql import SparkSession
import logging
spark = SparkSession.builder.master("local[1]").appName("SparkByExamples.com").getOrCreate()
df = spark.read.csv("gdp_per_capita.csv", header=True)
df_count = df.count()
logging.basicConfig()
logging.root.setLevel("INFO")
logging.info(f"Total rows count {df_count}")
