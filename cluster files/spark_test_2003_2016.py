import pyspark
import os
from pyspark.sql import DataFrameWriter
from pyspark.sql.functions import lit

sc = pyspark.SparkContext()
sql = pyspark.SQLContext(sc)
year = 2003
while year <= 2005:
	df = (sql.read.format("com.databricks.spark.csv").option("header","true").load('equalizers/' + str(year) + '.csv'))
	#df1 = (sql.read.format("com.databricks.spark.csv").option("header","true").load('equalizers/' + str(year) + '_2.csv'))
	
	#df = df.union(df1)
	print("count", df.count())
	df2 = df[["SERIALNO","RAC1P","HISP","INDP","COW","SCHL","PINCP","AGEP","SEX"]]

	df3 = df2.withColumn("DATA_FROM_YEAR", lit(year))

	df4 = df3.withColumnRenamed("RAC1P", "RACE").withColumnRenamed("HISP", "HISPANIC").withColumnRenamed("INDP", "INDUSTRY").withColumnRenamed("COW", "CATEGORY_OF_WORK").withColumnRenamed("SCHL", "EDUCATION").withColumnRenamed("AGEP", "AGE").withColumnRenamed("SEX", "GENDER").withColumnRenamed("PINCP", "income")
	print("new: ", len(df4.columns))
	print("count of df1: ", df4.count())

	df4.write.csv('equalizers/' + str(year) + '_1_reduced.csv', 'overwrite', header=True)
	year += 1
