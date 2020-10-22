import sys
import numpy as np
from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.ml.classification import RandomForestClassificationModel
from pyspark.ml.feature import VectorAssembler
from pyspark.sql import SparkSession
from sklearn import metrics


def prediction(input_data, model):

    data_split = input_data.map(lambda x: x[1].split(','))
    data_formated = data_split.map(lambda x: list(map(lambda y: float(y), x[0:-1])))
    data_label = data_split.map(lambda x: float(x[-1]))

    count = data_formated.count()
    print("Total number of elements: " + str(count) + ".")

    vector_label = np.array(data_label.collect())
    input_cols = []
    for i in range(22):
        input_cols.append("_" + str(i+1))
    assembler = VectorAssembler(inputCols=input_cols, outputCol='features')
    data_frame = assembler.transform(data_formated.toDF())
    prediction = model.transform(data_frame)
    vector_prediction = np.array(prediction.select("prediction").collect())

    accuracy = metrics.accuracy_score(vector_label, vector_prediction)
    print("Accuracy = " + str(accuracy))


def main(bootstrap_server, topic_name, time_interval, spark_model_path):

    configuration = SparkConf().setAppName("BigDataProj3_Consumer")
    context = SparkContext(conf=configuration)
    context.setLogLevel("ERROR")
    SparkSession(context)

    model = RandomForestClassificationModel.load(spark_model_path)

    streaming_context = StreamingContext(context, time_interval)
    stream = KafkaUtils.createDirectStream(streaming_context, [topic_name], {"metadata.broker.list": bootstrap_server})

    stream.foreachRDD(lambda input_data: prediction(input_data, model))

    streaming_context.start()
    streaming_context.awaitTermination()


if __name__ == "__main__":
    if len(sys.argv) != 5:
        print("\nBad command syntax. The correct parameter syntax is:")
        print(
            "bootstrap_server topic_name time_interval spark_model_path\n")
    else:
        good = True
        try:
            bootstrap_server = sys.argv[1]
            topic_name = sys.argv[2]
            time_interval = float(sys.argv[3])
            spark_model_path = sys.argv[4]
        except ValueError:
            print("\nInvalid parameters.\n")
            good = False
        if good:
            main(bootstrap_server, topic_name, time_interval, spark_model_path)
