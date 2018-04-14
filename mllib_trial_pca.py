from pyspark import SparkContext, SQLContext

from pyspark.mllib.regression import LabeledPoint, RidgeRegressionWithSGD
from pyspark.mllib.linalg.distributed import RowMatrix
from pyspark.mllib.linalg import Vector
import numpy as np

def parsePoint(line):
    values = []
#    if line.split(",")[1] != 'income':
            
    for x in line.replace(",", " ").split(" "):
        try:
            values.append(float(x))
        except:
            pass
    if len(values) != 0:
        return LabeledPoint(values[0], values[1:])

'''values = [float(x) for x in line.replace(",", " ").split(" ")]
return LabeledPoint(values[0], values[1:])'''

sc = SparkContext()
sql = SQLContext(sc)
#train_data = sc.textFile("./reduced-inequalities/flattened_with_years.csv")
#data = sc.read.format("csv").option("header", "true").load("./flattened_2002_1000.csv")

train_data = (sql.read.format("csv").option("header","true").load('./flattened_with_years.csv'))

train_data_y = train_data[train_data.columns[0]]
train_data = train_data[train_data.columns[1:]] 
row_data = train_data.rdd
vector_data = row_data.map(lambda x: np.array(x))

#parsed_data_train = train_data.map(parsePoint)
#print(vector_data.collect())
matrix = RowMatrix(vector_data)
#print(matrix.collect())
pc = matrix.computePrincipalComponents(7)
#print(pc)
projected_data_train = matrix.multiply(pc)
#print("here:", projected_data_train)

train_data_y = train_data_y.rdd
projected_data_train = projected_data_train.rdd
train_data = train_data_y.join(projected_data_train)
print(train_data.collect())

df_train = projected_data_train.map(lambda x: LabeledPoint(x[0], x[1:]))
#print(df_train)

model = RidgeRegressionWithSGD.train(df_train)
print(model.weights)


print(parsedData.take(3))
test_data = sc.textFile("./reduced-inequalities/flattened_2016_with_year.csv")
parsed_data_test = test_data.map(parsePoint)

test_matrix = RowMatrix(parsed_data_test)
projected_data_test = test_matrix.multiply(pc)

valuesAndPreds = projected_data_test.map(lambda p: (p[0], model.predict(p[1:])))
print(valuesAndPreds.take(3))

mse = valuesAndPreds.map(lambda v, p: (v - p)**2).reduce(lambda x,y: x + y) / valuesAndPreds.count()

print("Mean Squared Error: ", mse)
