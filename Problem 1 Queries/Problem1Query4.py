# Assume you are given the file Mega-Event. Return for each table table-i, the number of people pi
# that were sitting on this same table and a Boolean flag indicating if the table housed all healthy
# people as flag=healthy, otherwise return flag=concern. If no one was sitting on a table, do not
# return that table. 


from pyspark import SparkContext
sc = SparkContext.getOrCreate()

metaEvent = sc.textFile("Meta-Event.txt").map(lambda col: col.split(" "))

# Map (Table, (Count=1, IsHealthy=True/False))
table = metaEvent.map(lambda cols: (cols[2], (1, cols[3] == "not-sick")))

# Reduce by table to count people and check if all are healthy
def compare(a, b):
    return (a[0] + b[0], a[1] and b[1])  # Sum count, check if all are healthy

groupedTables = table.reduceByKey(compare)

# Format final result as (table-i, count(people), flag["healthy", "concern"])
result = groupedTables.map(lambda col: (col[0], col[1][0], "healthy" if col[1][1] else "concern"))

# Print results
for r in result.collect():
    print(f"table {r[0]}  count {r[1]}  {r[2]}")
