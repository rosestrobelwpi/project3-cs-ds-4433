from pyspark.sql import SparkSession
from pyspark.sql.functions import col, rand, expr, min, max, count, sum, avg, percentile_approx
import random
import string

spark = SparkSession.builder.appName("PurchaseTransactions").getOrCreate()

def random_string(length):
    return ''.join(random.choices(string.ascii_letters, k=length))

def generate_customers(num_customers=50000):
    customers = [(i, random_string(random.randint(10, 20)), random.randint(18, 100), 
                  random_string(15), round(random.uniform(1000, 10000), 2))
                 for i in range(1, num_customers + 1)]
    return customers

customers_df = spark.createDataFrame(generate_customers(), ["CustID", "Name", "Age", "Address", "Salary"])
customers_df.write.csv("customers.csv", header=True, mode="overwrite")

def generate_purchases(num_purchases=5000000, num_customers=50000):
    purchases = [(i, random.randint(1, num_customers), round(random.uniform(10, 2000), 2), 
                  random.randint(1, 15), random_string(random.randint(20, 50)))
                 for i in range(1, num_purchases + 1)]
    return purchases

purchases_df = spark.createDataFrame(generate_purchases(), ["TransID", "CustID", "TransTotal", "TransNumItems", "TransDesc"])
purchases_df.write.csv("purchases.csv", header=True, mode="overwrite")

T2 = purchases_df.groupBy("TransNumItems").agg(
    percentile_approx("TransTotal", 0.5).alias("MedianTransTotal"),
    min("TransTotal").alias("MinTransTotal"),
    max("TransTotal").alias("MaxTransTotal")
)
T2.show()

genz_customers_df = customers_df.filter((col("Age") >= 18) & (col("Age") <= 21))
T3 = purchases_df.join(genz_customers_df, "CustID")
T3 = T3.groupBy("CustID", "Age").agg(
    count("TransNumItems").alias("TotalNumItems"),
    sum("TransTotal").alias("TotalTransTotal")
)
T3.show()
T3.write.csv("T3.csv", header=True, mode="overwrite")
