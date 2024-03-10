from jobs.bronze_layer import ingest_daily_orders

file_path = '/Users/suren/Desktop/pyspark/data_pipeline/resources/sample_daily_orders.csv'


def test_read_csv_file():
    ingest_daily_orders.run(file_path)