# Assume you are given the file Mega-Event-No-Disclosure and the file Reported-Illnesses. Return all
# healthy people pi in the file Mega-Event-No-Disclosure that were sitting on the same table with at
# least one sick person as in the Reported-Illnesses, so that they can be notified to take appropriate
# precaution. People are considered healthy if they do not appear in the Reported-Illnesses file 

from pyspark import SparkContext
sc = SparkContext.getOrCreate()

# Load files
noDisclosure = sc.textFile("Meta-Event-No-Disclosure.txt")
reportedIllnesses = sc.textFile("Reported-Illnesses.txt")

# Get sick person IDs and broadcast them
reportedIllnessesSet = set(reportedIllnesses.map(lambda col: col.split()[0]).collect())  
broadcastSick = sc.broadcast(reportedIllnessesSet)  

# Extract relevant data from Mega-Event-No-Disclosure
def getTableData(cols):
    splitCol = cols.split(" ")
    person_id = splitCol[0].strip()
    person_name = splitCol[1].strip()
    table = splitCol[2].strip()
    return (person_id, person_name, table)  # No health status

table = noDisclosure.map(getTableData)  # (person_id, person_name, table)

# Find sick people based on Reported-Illnesses
sickPeople = table.filter(lambda data: data[0] in broadcastSick.value)  # (ID, Name, Table)

# Find tables with sick people
sickTables = sickPeople.map(lambda data: data[2]).distinct().collect()  
broadcastSickTable = sc.broadcast(set(sickTables))  # Convert to broadcast

# Identify healthy people (not in the sick list)
healthyPeople = table.filter(lambda data: data[0] not in broadcastSick.value)  

# Identify healthy people at sick tables
healthyRisk = healthyPeople.filter(lambda data: data[2] in broadcastSickTable.value)  

print("Given the file Meta-Event-No-Disclosure and the file Reported-Illnesses...")
print("All healthy people who sat at the same table with a sick person:")

for r in healthyRisk.collect():
    print(r)


# for person_id, person_name, table in result:
#     print(f"Person: {person_id}, Name: {person_name}, Table: {table}, Health: not-sick")
