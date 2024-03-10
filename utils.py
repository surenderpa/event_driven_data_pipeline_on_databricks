from pyspark.sql import SparkSession
spark = None

def get_spark_session(app_name='data_ingestion'):
    global spark
    if spark is not None:
        return spark
    spark = (SparkSession.builder.appName(app_name).getOrCreate())
             # .config("spark.some.config.option", "some-value")
    return spark


