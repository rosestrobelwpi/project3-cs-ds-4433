# Assume you are given the file Mega-Event-No-Disclosure and the file Reported-Illnesses. Return all
# healthy people pi in the file Mega-Event-No-Disclosure that were sitting on the same table with at
# least one sick person as in the Reported-Illnesses, so that they can be notified to take appropriate
# precaution. People are considered healthy if they do not appear in the Reported-Illnesses file 

from pyspark import SparkContext
sc = SparkContext.getOrCreate()

noDisclosure = sc.textFile("Meta-Event.txt")
reportedIllnesses = sc.textFile("Reported-Illnesses.txt")

noDisclosureSplit = noDisclosure.map(lambda col: col.split(" "))
reportedIllnessesSplit = reportedIllnesses.map(lambda col: col.split()[0]).collect() # Getting person id and putting into a list
reportedIllnessesSet = set(reportedIllnessesSplit) # Converting list into set; no duplicates
broadcastSick = sc.broadcast(reportedIllnessesSet) # Converting set to broadcast to transfer data more efficiently

def getTableData(cols):
    splitCol = cols.split(" ")
    person_id = splitCol[0].strip()
    table = splitCol[1].strip()
    return(person_id, table)

table = noDisclosure.map(getTableData) # Mapping table data and creates a tuple (person_id, table)

sickPerson = table.filter(lambda data: data[0] in broadcastSick.value) # Filters the table to collect the data where the person is found in the broadcasted sick set

sickTable = sickPerson.map(lambda data: data[1]).distinct().collect() # Gets the table id and returns only unique numbers

sickTableSet = set(sickTable) # Converting to a set

broadcastSickTable = sc.broadcast(sickTableSet) # Converting set to broadcast

healthy = table.filter(lambda data: data[0] not in broadcastSickTable.value) # Determining who is healthy

healthyRisk = healthy.filter(lambda data: data[1] in broadcastSickTable.value) # Determining who is sick

result = healthyRisk.collect() 
print("Healthy people that have sat at the same table as a sick person:")
for person_id, table in result:
    print(f"Person: {person_id}, Table: {table}")
    

