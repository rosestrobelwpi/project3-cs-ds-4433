# Group the Purchases in T1 by the number of items purchased (TransNumItems). For
# each group with the same number of items, calculate the median, min and max of total amount
# spent for purchases in that group. Report the result back to the client side.

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, median, min, max
spark = SparkSession.builder.getOrCreate()

# Reading Purchases.csv
purchasesCSV = spark.read.csv("Purchases.csv", header=False, inferSchema=True)

# Grouping all of the purchases with the same number of items
groupSameNum = purchasesCSV.groupBy("_c3")

# Calculating the median, min, and max of the total amount spent for purchases in each group
results = groupSameNum.agg(median("_c2").alias("Median"),
                           min("_c2").alias("Min"),
                           max("_c2").alias("Max"))

# Reporting results back to client side
results.show()