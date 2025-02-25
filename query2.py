# The organizers learn that attendees that were sick attended the event as indicated in the Reported-
# Illnesses file. Assume you are given the file Mega-Event-No-Disclosure and the file Reported-
# Illnesses, return all people pi in the file Mega-Event-No-Disclosure (including their id and their table
# assignment) that were reported as sick based on the Reported-Illnesses file, i.e., pi.test = sick.

#using pyspark

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
