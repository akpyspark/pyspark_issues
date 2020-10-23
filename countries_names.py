from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local[1]").appName("SparkByExamples.com").getOrCreate()
df = spark.read.csv("gdp_per_capita.csv", header=True)
df = df.filter(df["2019"] >= 1000)
df.select("Country Name").show(300, truncate=False)
