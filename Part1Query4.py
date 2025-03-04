# Query4:
# Assume you are given the file Mega-Event. Return for each table table-i, the number of people pi
# that were sitting on this same table and a Boolean flag indicating if the table housed all healthy
# people as flag=healthy, otherwise return flag=concern. If no one was sitting on a table, do not
# return that table. (4 Points)

from pyspark import SparkContext

sc = SparkContext("local", "MetaEvent")
metaEvent = sc.textFile("Meta-Events.txt").map(lambda line: line.split(" "))

tablePeople = metaEvent.map(lambda person: (person[1], (person[2], person[0])))

tableCounts = tablePeople.map(lambda person: (person[0], 1)).reduceByKey(lambda a, b: a + b)


sickTable = tablePeople.filter(lambda person: person[1][0] == "sick").map(lambda person: (person[0], 1))

tableStatus = tableCounts.leftOuterJoin(sickTable).map(lambda person: (person[0], person[1][0], "concern" if person[1][1] is not None else "healthy"))

tableStatus.saveAsTextFile("TableStatus")
tableStatus.collect()


