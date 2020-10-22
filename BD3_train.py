import sys
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import RandomForestClassifier
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession


def toFloat(str):
    try:
        x = float(str)
        return x
    except ValueError:
        return 0.0

def main(input_path, spark_output_path):

    configuration = SparkConf().setAppName("BigDataProj3_Trainer")
    context = SparkContext(conf=configuration)
    context.setLogLevel("ERROR")
    session = SparkSession(context)

    input_data = context.textFile(input_path)
    input_data = input_data.map(lambda x: x.split(","))
    input_data = input_data.filter(lambda x: x[0] != "AccX")
    input_data = input_data.map(lambda x: list(map(lambda y: toFloat(y), x[0:-1])) + [toFloat(x[-1])])

    input_cols = []
    for i in range(22):
        input_cols.append("_" + str(i+1))
    assembler = VectorAssembler(inputCols=input_cols, outputCol='features')
    data_frame = assembler.transform(input_data.toDF())

    rf = RandomForestClassifier(featuresCol='features', labelCol='_23',numTrees=10)
    model = rf.fit(data_frame)
    model.write().overwrite().save(spark_output_path)


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("\nBad command syntax. The correct parameter syntax is:")
        print("input_path spark_output_path\n")
    else:
        good = True
        try:
            input_path = sys.argv[1]
            spark_output_path = sys.argv[2]
        except ValueError:
            print("\nInvalid parameters.\n")
            good = False
        if good:
            main(input_path, spark_output_path)
