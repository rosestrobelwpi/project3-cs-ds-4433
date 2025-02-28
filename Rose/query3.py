# Assume you are given the file Mega-Event. Return for each table table-i, the number of people pi
# that were sitting on this same table and a Boolean flag indicating if the table housed all healthy
# people as flag=healthy, otherwise return flag=concern. If no one was sitting on a table, do not
# return that table. 

from pyspark import SparkContext
sc = SparkContext.getOrCreate()

metaEvent = sc.textFile("Meta-Event.txt") # Reading file
metaEventSplit = metaEvent.map(lambda col: col.split(" ")) # Splitting columns by space

def get_table_data(cols): # Getting data from table
    person_id = cols[0]
    health = cols[2]
    is_healthy = True if health == "not-sick" else False
    return(person_id, (1, is_healthy)) # Making a tuple, then count, then boolean

table = metaEventSplit.map(get_table_data) # Mapping table data

def compare(a, b): # Comparing two people
    return (a[0] + b[0], a[1] and b[1]) # Returning count and boolean

group = table.reduceByKey(compare) # Groups by table id and then compares

def get_result(col): # Writing for output
    person_id = col[0] # Getting table id
    count = col[1][0] # Getting tuple then first element
    all_healthy = col[1][1] # Getting tuple then second element
    flag = "healthy" if all_healthy else "concern" 
    return(person_id, count, flag)

result = group.map(get_result)

for r in result.collect():
    print(r)