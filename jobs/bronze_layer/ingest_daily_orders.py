from utils import get_spark_session
from jobs.bronze_layer import dq_checks

def run(input_file_path):
    spark = get_spark_session()
    daily_orders_df = spark.read.option('header', True).csv(input_file_path)
    dq_checks.run()
    daily_orders_df.show(2, truncate=False)
