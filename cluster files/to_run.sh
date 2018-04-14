# the spark_test file creates a folder for every year with a number of csv files from the cluster, we need to integrate them
hadoop fs -getmerge -nl equalizers/2011_1_reduced.csv ./2016_1_reduced_merged.csv
hadoop fs -getmerge -nl equalizers/2011_1_reduced.csv ./2015_1_reduced_merged.csv
hadoop fs -getmerge -nl equalizers/2011_1_reduced.csv ./2014_1_reduced_merged.csv
hadoop fs -getmerge -nl equalizers/2011_1_reduced.csv ./2013_1_reduced_merged.csv
hadoop fs -getmerge -nl equalizers/2011_1_reduced.csv ./2012_1_reduced_merged.csv
hadoop fs -getmerge -nl equalizers/2011_1_reduced.csv ./2011_1_reduced_merged.csv
hadoop fs -getmerge -nl equalizers/2010_1_reduced.csv ./2010_1_reduced_merged.csv
hadoop fs -getmerge -nl equalizers/2009_1_reduced.csv ./2009_1_reduced_merged.csv
hadoop fs -getmerge -nl equalizers/2008_1_reduced.csv ./2008_1_reduced_merged.csv
hadoop fs -getmerge -nl equalizers/2007_1_reduced.csv ./2007_1_reduced_merged.csv
hadoop fs -getmerge -nl equalizers/2006_1_reduced.csv ./2006_1_reduced_merged.csv

hadoop fs -getmerge -nl equalizers/2005_1_reduced.csv ./2005_1_reduced_merged.csv
hadoop fs -getmerge -nl equalizers/2004_1_reduced.csv ./2004_1_reduced_merged.csv
hadoop fs -getmerge -nl equalizers/2003_1_reduced.csv ./2003_1_reduced_merged.csv

hadoop fs -getmerge -nl equalizers/2000_1_reduced.csv ./2000_1_reduced_merged.csv
hadoop fs -getmerge -nl equalizers/2001_1_reduced.csv ./2001_1_reduced_merged.csv
hadoop fs -getmerge -nl equalizers/2002_1_reduced.csv ./2002_1_reduced_merged.csv

#upload the integrated files on the cluster
hadoop fs -put 2000_1_reduced_merged.csv equalizers/
hadoop fs -put 2001_1_reduced_merged.csv equalizers/
hadoop fs -put 2002_1_reduced_merged.csv equalizers/
hadoop fs -put 2003_1_reduced_merged.csv equalizers/
hadoop fs -put 2004_1_reduced_merged.csv equalizers/
hadoop fs -put 2005_1_reduced_merged.csv equalizers/
hadoop fs -put 2006_1_reduced_merged.csv equalizers/
hadoop fs -put 2007_1_reduced_merged.csv equalizers/
hadoop fs -put 2008_1_reduced_merged.csv equalizers/
hadoop fs -put 2009_1_reduced_merged.csv equalizers/
hadoop fs -put 2010_1_reduced_merged.csv equalizers/
hadoop fs -put 2011_1_reduced_merged.csv equalizers/
hadoop fs -put 2012_1_reduced_merged.csv equalizers/
hadoop fs -put 2015_1_reduced_merged.csv equalizers/
hadoop fs -put 2016_1_reduced_merged.csv equalizers/
