# Assume you are given the file Mega-Event, return all healthy people pi in the Mega-Event file that
# were sitting on the same table with at least one sick person pj, so that they can be notified to take
# appropriate precaution. Do not return a healthy person twice. (3 Points)

from pyspark import SparkContext

sc = SparkContext("local", "MetaEvent")
metaEvent = sc.textFile("Meta-Event.txt").map(lambda x: x.split(" "))

tablePeople = metaEvent.map(lambda person: (person[1], (person[0], person[2])))

sickTable = tablePeople.filter(lambda x: x[1][1] == "sick").map(lambda x: (x[0], x[1][0])).distinct()

healthyPeople = tablePeople.filter(lambda x: x[1][1] == "not-sick").map(lambda x: (x[0], x[1][0])).distinct()

atRisk = healthyPeople.join(sickTable).map(lambda x: x[1][0]).distinct()

atRisk.saveAsTextFile("atRisk")
atRisk.collect()