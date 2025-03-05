# Assume you are given the file Meta-Event, return all people pi in the file 
# Meta-Event that were ill, i.e., pi.test = sick. (3 Points)


from pyspark import SparkContext

sc = SparkContext.getOrCreate()

# Load Meta-Event file
metaEvent = sc.textFile("Meta-Event.txt")

# Split columns (assuming space-separated values)
metaEventSplit = metaEvent.map(lambda col: col.split(" "))

# Extract relevant data (ID, Name, Table, Test)
def get_sick_data(cols):
    person_id = cols[0]
    name = cols[1]
    test_result = cols[3]
    is_sick = test_result == "sick"
    return (person_id, (name, is_sick))

table = metaEventSplit.map(get_sick_data)

# Filter only sick people
sickPeople = table.filter(lambda col: col[1][1])  # Keep only those marked as sick

# Format output (ID, Name, Table, "sick")
def format_output(col):
    person_id = col[0]
    name = col[1][0]
    return (person_id, name, "sick")

result = sickPeople.map(format_output)

# Display results
print("\n\nAll people pi in the file Meta-Event that were ill:")
for r in result.collect():
    print(r)
