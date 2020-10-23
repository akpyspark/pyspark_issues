from pyspark.sql import SparkSession
import pyspark.sql.functions as fx
spark = SparkSession.builder.master("local[1]").appName("SparkByExamples.com").getOrCreate()
df = spark.read.csv("gdp_per_capita.csv", header=True)
header = [x.upper() for x in df.columns]
for header_col in header:
    data = df.filter((fx.col(header_col) != header_col) | (fx.col(header_col).isNull()))
    data.show(300, truncate=False)