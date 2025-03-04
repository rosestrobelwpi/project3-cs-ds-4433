# Query 1. Filter out (drop) the Purchases from file P with a total purchase amount (TransTotal)
# above $100. Store the result as file T1.

from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

# Reading the Purchases.csv
purchasesCSV = spark.read.csv("Purchases.csv", header=False, inferSchema=True)

# Creating a local temporary view so that we can use SparkSQL to query the data
purchasesCSV.createOrReplaceTempView("purchases")

# Selecting the rows where the total purchase amount is less than or equal to 100 so that we can
# filter out any purchases about $100
filterTotalAmount = spark.sql("SELECT * FROM purchases WHERE _c2 <= 100")

# Saving as a new file
filterTotalAmount.write.csv("task2query1.csv", header=False)
