from pyspark import SparkContext, SQLContext

from pyspark.mllib.regression import LabeledPoint, RidgeRegressionWithSGD, LinearRegressionWithSGD

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

def computeSquaredError(x):
    error = x[0] - x[1]
    squared_error = error ** 2
    return (1, squared_error)

sc = SparkContext()
sql = SQLContext(sc)
#train_data = sc.textFile("./reduced-inequalities/new_flattened_with_years_10.csv")
train_data = sc.read.format("csv").option("header", "true").load("./flattened_with_years.csv")
#data = (sql.read.format("csv").option("header","true").load('./flattened_2002_1000.csv'))
parsed_data_train = train_data.map(parsePoint)


model = RidgeRegressionWithSGD.train(parsed_data_train)
#model = LinearRegressionWithSGD.train(parsed_data_train)
#print(parsedData.take(3))
test_data = sc.textFile("./reduced-inequalities/flattened_2016_with_year.csv")
parsed_data_test = test_data.map(parsePoint)
#print(parsed_data_test.take(3))
valuesAndPreds = parsed_data_test.map(lambda p: (p.label, model.predict(p.features)))
squared_errors = valuesAndPreds.map(lambda x : computeSquaredError(x))
squared_errors = squared_errors.reduceByKey(lambda x, y: x+y)
squared_error = squared_errors.collect()
mse = squared_error[0][1] / valuesAndPreds.count()
#print(valuesAndPreds.take(3))
#mse = valuesAndPreds.map(lambda (v, p): (v - p)**2).reduce(lambda x,y: x + y) / valuesAndPreds.count()
print(model.weights)

print("Mean Squared Error: ", str(mse))
sc.stop()
