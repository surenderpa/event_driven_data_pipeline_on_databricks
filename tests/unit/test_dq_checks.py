from jobs.bronze_layer import dq_checks
import pytest
from utils import get_spark_session

spark = get_spark_session()
df = None

@pytest.fixture(scope='module', autouse=True)
def setup_data():
    file_path = '/Users/suren/Desktop/pyspark/data_pipeline/resources/sample_daily_orders.csv'
    global df
    df = spark.read.option('header', True).csv(file_path).select('payment_key', 'quantity','total_price')
    return df

# Unit tests for No Nulls check
def test_should_not_find_nulls():
    result = dq_checks.nulls_check(df, 'payment_key')
    assert result[0] == 1


def test_should_find_empty_string():
    new_row = [('', 1, 1256),]
    columns = ['payment_key', 'quantity', 'total_price']
    df_with_empty_string_col_value = spark.createDataFrame(new_row, columns)
    empty_string_df = df.union(df_with_empty_string_col_value)
    result = dq_checks.nulls_check(empty_string_df, 'payment_key')
    assert result[0] == 0

def test_should_find_nulls():
    new_row = [(None, 1, 2500),('P120', 1, 2500),]
    columns = ['payment_key', 'quantity', 'total_price']
    nulls_row_df = spark.createDataFrame(new_row, columns)
    nulls_added_df = df.union(nulls_row_df)
    result = dq_checks.nulls_check(nulls_added_df, 'payment_key')
    assert result[0] == 0


# unit test for function which check if valid data quality name is received from audit table
def test_should_return_its_respective_dq_fn_if_dq_is_defined():
    valid_dq_name = dq_checks.get_dq('nulls_check')
    assert valid_dq_name == dq_checks.nulls_check


def test_should_raise_key_error_if_dq_is_not_defined():
    with pytest.raises(ValueError):
        dq_checks.get_dq('undefined_Dq')

