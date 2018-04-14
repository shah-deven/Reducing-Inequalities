# this file reads the csvs from the dataset, and changes selected column names to readable form; it creates a reduced version with only specific columns. We run it once for every year.
#import pyspark
import os
from pyspark.sql import DataFrameWriter
from pyspark.sql.functions import lit
from pyspark import SparkContext, SparkConf, SQLContext

#extract data over all the years 
sc = SparkContext()
sql = SQLContext(sc)
year = 2003
#while(year <= 2016):
df = (sql.read.format("com.databricks.spark.csv").option("header","true").load('equalizers/' + str(year) + '_1.csv'))
df1 = (sql.read.format("com.databricks.spark.csv").option("header","true").load('equalizers/' + str(year) + '_2.csv'))

df = df.union(df1)
print("count", df.count())
df2 = df[["SERIALNO","RAC1P","HISP","INDP","COW","SCHL","PINCP","AGEP","SEX"]]

df3 = df2.withColumn("DATA_FROM_YEAR", lit(year))

df4 = df3.withColumnRenamed("RAC1P", "RACE").withColumnRenamed("HISP", "HISPANIC").withColumnRenamed("INDP", "INDUSTRY").withColumnRenamed("COW", "CATEGORY_OF_WORK").withColumnRenamed("SCHL", "EDUCATION").withColumnRenamed("AGEP", "AGE").withColumnRenamed("SEX", "GENDER").withColumnRenamed("PINCP", "income")
#df4['INDUSTRY'] = df4['INDUSTRY'] * 10
#df4 = df4.withColumn('INDUSTRY', df4.INDUSTRY * 10)
print("new: ", len(df4.columns))
print("count of df1: ", df4.count())

df4.write.csv('equalizers/' + str(year) + '_1_reduced.csv', 'overwrite', header=True)
#year += 1
