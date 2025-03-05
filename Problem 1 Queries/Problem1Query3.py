# Assume you are given the file Mega-Event, return all healthy people pi in the Mega-Event file that
# were sitting on the same table with at least one sick person pj, so that they can be notified to take
# appropriate precaution. Do not return a healthy person twice. (3 Points)

from pyspark import SparkContext

sc = SparkContext.getOrCreate()

# Load Mega-Event file (ID, Name, Table, Sickness)
metaEvent = sc.textFile("Meta-Event.txt").map(lambda x: x.split(" "))

# Extract (Table, (ID, Name, Sickness))
tablePeople = metaEvent.map(lambda person: (person[2], (person[0], person[1], person[2], person[3])))  # (Table, (ID, Name, Table, Sickness))

# Identify tables with sick people
sickTable = tablePeople.filter(lambda x: x[1][3] == "sick").mapValues(lambda x: x[0])  # (Table, SickPersonID)

# Identify healthy people at each table, keeping all relevant information
healthyPeople = tablePeople.filter(lambda x: x[1][3] == "not-sick").mapValues(lambda x: (x[0], x[1], x[2], "not-sick"))  # (Table, (ID, Name, Table, "not-sick"))

# Find healthy people at tables where there is at least one sick person
atRisk = healthyPeople.join(sickTable).values().map(lambda x: x[0]).distinct()

# Output results
print("\n\nAll healthy people pi in the Mega-Event file that were sitting on the same table with at least one sick person:")
for r in atRisk.collect():
    # print(r)  # Prints (ID, Name, Table, "not-sick")
    print(f"ID {r[0]}, Name:{r[1]}, Table {r[2]}, not-sick")
