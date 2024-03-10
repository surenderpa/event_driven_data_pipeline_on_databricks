from pyspark.sql.functions import isnan, isnull, coalesce, col, lit, when
import traceback
from utils import get_spark_session
spark = get_spark_session()


def nulls_check(df, column):
    nulls_df = (df.withColumn('is_null', isnull(column))
                .withColumn('is_nan', isnan(column))
                .withColumn('is_empty_string',
                            when(df[column] == '', True).otherwise(False))
                .filter('is_null = True OR is_nan = True OR is_empty_string = True'))
    nulls_count = nulls_df.count()
    if nulls_count == 0:
        return 1,
    else:
        nulls_df = nulls_df.withColumn('dq_failed', lit('nulls_check on column "' + column + '"'))
        return 0, nulls_df


def duplicate_check(df, column):
    return 1,


def column_names_n_order_check(df, column):
    return 1,


def range_check(df, column):
    return 1,


def file_name_check(df, column):
    return 1,


def get_dq(dq_rule):
    mappings = {
            'nulls_check': nulls_check,
            'duplicate_check': duplicate_check,
            'column_names_n_order_check': column_names_n_order_check,
            'range_check': range_check,
            'file_name_check': file_name_check
        }
    dq_function = mappings.get(dq_rule)
    if dq_function is None:
        raise ValueError(f'Data Quality {dq_rule} is invalid and not defined.')
    return dq_function


def get_process_id_n_dqs_from_audit_tbl(process_name):
    dq = spark.sql('''
                           SELECT p.process_id,dq.dq_rule_id,dq.rule,dq.dq_rule_condition 
                           FROM dev.audit.process_definition p
                           INNER JOIN dev.audit.dq_rule_definition dq
                           ON p.process_id = dq.process_id
                           where p.process_name=lower(process_name)
                           ''')
    if dq.count() == 0:
        raise Exception('Process - {process_name} is not found in the process definition tbl')
    if dq.select('process_id').distinct().count() > 1:
        raise Exception(
            'File - {file_name} Ingestion Stopped. Process {process_name} is duplicated in the process definition tbl ')
    return dq


dq_results = {'succeeded': [], 'failed': []}


def run(df):
    dq_list = get_process_id_n_dqs_from_audit_tbl('daily_transactions')
    process_id = dq_list.select('process_id').collect()['process_id']
    dqs_to_run = dq_list.select('dq_rule_id', 'dq_rule', 'dq_rule_condition').collect()
    for dq in dqs_to_run:
        dq_rule_id = dqs_to_run['dq_rule_id']
        dq_rule = dqs_to_run['dq_rule']
        column_name_to_apply_dq = dqs_to_run['dq_rule_condition']
        dq_fn_to_execute = get_dq(dq_rule)

        dq_result = dq_fn_to_execute(column_name_to_apply_dq)
        if dq_result[0]:
            dq_results['succeeded'].append(dq_rule)
        else:
            dq_results['failed'].append((dq_rule, dq_result[1], dq_rule_id, process_id))

