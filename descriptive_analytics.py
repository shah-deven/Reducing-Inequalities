# Code snippet to parse files across years and compute mean incomes for each year
import pandas as pd
import os
exclude_file_list = ['2001','2002','2003','2004']
one_file_list = ['2005']
chunkSize = 100000
folders = os.listdir("C:\\Users\\Manu\\Documents\\Big Data\\Data\\")
output_file = open("C:\\Users\\Manu\\Documents\\Big Data\\national_avgs.csv","a")

for folder in folders:
	if folder not in exclude_file_list:
		if folder in one_file_list:
			digits = folder[-2:]
			df = pd.read_csv("C:\\Users\\Manu\\Documents\\Big Data\\Data\\"+folder+"\\ss"+digits+"pusa.csv")
		else:
			digits = folder[-2:]
			df1 = pd.read_csv("C:\\Users\\Manu\\Documents\\Big Data\\Data\\"+folder+"\\ss"+digits+"pusa.csv")
			df2 = pd.read_csv("C:\\Users\\Manu\\Documents\\Big Data\\Data\\"+folder+"\\ss"+digits+"pusb.csv")
			df = pd.concat([df1,df2])
			del df1, df2
		df.dropna(axis=0, how='all')
		maxValue = max(df['PINCP'])
		quintileMark = (float(maxValue)/5)*2
		bottomOutputFile = open("C:\\Users\\Manu\\Documents\\Big Data\\"+folder+"_bottom_quintile.csv","a")
		for chunk in pd.read_csv("C:\\Users\\Manu\\Documents\\Big Data\\"+folder+"\\ss"+digits+"pusa.csv",chunksize=chunkSize):
			bottom_df1 = chunk[chunk.PINCP < quintileMark]
			bottom_df = bottom_df1[bottom_df1.PINCP > 0]
			bottom_df.to_csv(bottomOutputFile,header=True,index=False)
			del bottom_df1
			del bottom_df
			del chunk
		if folder not in one_file_list:
			for chunk in pd.read_csv("C:\\Users\\Manu\\Documents\\Big Data\\"+folder+"\\ss"+digits+"pusb.csv",chunksize=chunkSize):
			bottom_df1 = chunk[chunk.PINCP < quintileMark]
			bottom_df = bottom_df1[bottom_df1.PINCP > 0]
			bottom_df.to_csv(bottomOutputFile,header=True,index=False)
			del bottom_df1
			del bottom_df
			del chunk

		mean_income = df['PINCP'].mean()
		line = folder+","+str(mean_income)+"\n"
		output_file.write(line)
		del df

output_file.close()
