# In the file Meta-Event, each person pi has several attributes, including a unique identifier
# pi.id, name pi.name, their table assignment pi.table-i, and the result of a recent flu test,
# namely, pi.test = sick or pi.test = not-sick

import random

def create_pi(n):
    #create file
    dataset = open("input/Meta-Event.txt", "w")
    for i in range(n):
        id = str(i)
        name = "pi" + str(i)
        table = random.randint(1, int(n/10)) # Max 10 people per table, randomly assigned
        options = ["sick", "not-sick"]
        test = random.choices(options, weights=[0.3, 0.7], k=1)[0]
        dataset.write(f"{id} {name} {table} {test}\n")
    dataset.close()

# In the file Meta-Event-No-Disclosure, each person pi has the same attributes such as the
# identifier pi.id, name pi.name, and table assignment pi.table-j, except for no health status is
# reported. In other words, you don’t know who in this file was infected with the illness.

def create_pi_no_disclosure():
    # pull from meta-event
    meta_event = open("input/Meta-Event.txt", "r")
    meta_event_lines = meta_event.readlines()
    meta_event.close()
    dataset = open("input/Meta-Event-No-Disclosure.txt", "w")
    # remove health status
    for line in meta_event_lines:
        line = line.split()
        dataset.write(f"{line[0]} {line[1]} {line[2]}\n")
    dataset.close()
    

# In the small file Reported-Illnesses, the health service maintained all people who had
# tested positive, i.e., pi.test=sick, yet attended the event. The file contains the identifier
# pi.id and the field pi.test=sick. People in this file are a (small) subset of the people in the
# huge Meta-Event-No-Disclosure file.

def create_reported_illnesses():
    meta_event = open("input/Meta-Event.txt", "r")
    meta_event_lines = meta_event.readlines()
    meta_event.close()
    dataset = open("input/Reported-Illnesses.txt", "w")
    # pull sick people from meta-event pi.test=sick
    for line in meta_event_lines:
        line = line.split()
        if line[3] == 'sick':
            dataset.write(f"{line[0]} {line[3]}\n")
    dataset.close()


create_pi(500)
print("Created Meta-Event.txt!")
create_pi_no_disclosure()
print("Created Meta-Event-No-Disclosure.txt!")
create_reported_illnesses()
print("Created Reported-Illnesses.txt!")
