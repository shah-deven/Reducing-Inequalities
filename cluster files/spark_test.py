# this file file creates a folder for every year with a number of csv files from the cluster
#import pyspark
from pyspark import SparkContext, SparkConf, SQLContext
from pyspark.sql.functions import lit

#conf = SparkConf().setAppName('mapreduce').setMaster('local')
sc = SparkContext()
sql = SQLContext(sc)

df = (sql.read.format("com.databricks.spark.csv").option("header", "true").load('equalizers/2000_1_reduced_merged.csv'))

cow = (sql.read.format("com.databricks.spark.csv").option("header", "true").option("delimiter", ":").load('equalizers/COW.csv'))
cow_map = cow.collect()

cow_dict = dict()
for tuple in cow_map:
	cow_dict[tuple[0]] = tuple[1]

#print(cow_dict)
#cow_dict = dict(zip(cow['b'], cow['Not Applicable']))

#cow = cow.withColumn(cow_dict.values(), 0)
for value in cow_dict.values():
	cow = cow.withColumn(value, lit(0))


def fill_column_values(x):
	print(x)
	#x[x['Not Applicable']] = 1
	return x

cow_map = cow.rdd.map(lambda x: fill_column_values(x))
print(cow_map.collect())

#for i in range(0, cow.count()):
#	print(cow[i])
