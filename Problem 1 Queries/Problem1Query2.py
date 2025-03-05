# The organizers learn that attendees that were sick attended the event as indicated in the Reported-Illnesses file. 
# Assume you are given the file Meta-Event-No-Disclosure and the file Reported- Illnesses, return all people pi in the 
# file Meta-Event-No-Disclosure (including their id and their table assignment) that were reported as sick based on the 
# Reported-Illnesses file, i.e., pi.test = sick.

from pyspark import SparkContext

sc = SparkContext.getOrCreate()

# Load Meta-Event-No-Disclosure file
metaEventNoDisclosure = sc.textFile("Meta-Event-No-Disclosure.txt")
metaEventNoDisclosureSplit = metaEventNoDisclosure.map(lambda line: line.split())

# Extract (ID, TableNumber)
eventData = metaEventNoDisclosureSplit.map(lambda cols: (cols[0], cols[2]))  # (ID, TableNumber)

# Load Reported-Illnesses file
reportedIllnesses = sc.textFile("Reported-Illnesses.txt")
reportedIllnessesSplit = reportedIllnesses.map(lambda line: line.split())

# Extract (ID, "sick")
sickIDs = reportedIllnessesSplit.map(lambda cols: (cols[0], "sick"))  # (ID, "sick")

# Join on ID
sickPeople = eventData.join(sickIDs)  # (ID, (TableNumber, "sick"))

# Format output: (ID, TableNumber, "sick")
result = sickPeople.map(lambda col: (col[0], col[1][0], "sick"))

# Display results
print("\n\nAll people in the file Meta-Event-No-Disclosure (id, table, sick) that were reported as sick based on Reported-Illnesses:")
for r in result.collect():
    print(r)
