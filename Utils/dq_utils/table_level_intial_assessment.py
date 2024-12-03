from Utils.sparksession.local_spark_utility import *
from Utils.connectors.spark_connectors import *
from Utils.dg_otherUtilities import *
from dqsensors_table import *
import os
import datetime
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pandas as pd
from tqdm import tqdm
import warnings
warnings.filterwarnings("ignore")


completeness_weight = 0.5
uniqueness_weight = 0.2
ref_integrity_weight = 0.2
freshness_weight = 0.1


job_run_timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S %Z')
job_run_timestamp_timezone = str(datetime.datetime.now(datetime.timezone.utc).astimezone().tzinfo)

spark = get_mssql_sparkContext()

os.environ["HADOOP_HOME"] = r"C:\Users\sahil.mate\Downloads\hadoop-3.3.0"

config_file_path = r"C:\Users\sahil.mate\Desktop\dguniversal\src\inHousedqchecks\checksuite_dimCustomer.json"
config_dictionary = load_config(config_file_path)

# List of dictionaries with results of all data quality checks.
result_list = []

# table_name = config_dictionary['table_name']
connection_name = config_dictionary['connection_name']
connection_properties = config_dictionary['connection_properties']

file_type = connection_properties.get("file_type")
file_path = connection_properties.get("file_path")
database_ip = connection_properties.get("database_ip")
database_name = connection_properties.get("database_name")

# metadata_file_path = r"C:\Users\sahil.mate\Downloads\Metadata_mssql_adventureworks.csv"
metadata_file_path = r"C:\Users\sahil.mate\Downloads\Classified_metadata.csv"
metadata_df_mssql = get_table_from_csv(spark=spark, file_path=metadata_file_path)

metadata_df_mssql = calculate_freshness_score(df=metadata_df_mssql, modified_date_column="Last Modified Date")

integrity_result_list = []

print("Calculating referential integrity scores")
for row in tqdm(metadata_df_mssql.filter(col("Key Type") == "FK").collect()):

    table_name = row['Table Name']
    column_name = row['Column Name']
    referenced_table_name = row['Reference Table']
    referenced_column_name = row['Reference Column']

    if table_name != referenced_table_name:
        original_df = get_table_from_mssql(spark=spark, table_name=table_name, db_ip=database_ip, db_name=database_name)
        referenced_df = get_table_from_mssql(spark=spark, table_name=referenced_table_name, db_ip=database_ip, db_name=database_name)
    else:
        original_df = get_table_from_mssql(spark=spark, table_name=table_name, db_ip=database_ip, db_name=database_name)
        referenced_df = original_df
    
    referential_integrity_score = get_referential_integrity_score(original_df=original_df, column_name=column_name, referenced_df=referenced_df, referenced_column_name=referenced_column_name)

    integrity_result_list.append({"table_name" : table_name , "ref_integrity_score" : referential_integrity_score})
    

integrity_metrics_df = pd.DataFrame(integrity_result_list).groupby('table_name').agg(average_ref_integrity_score=('ref_integrity_score', 'mean')).reset_index()


unique_table_names = metadata_df_mssql.select("Table Name").distinct().collect()
dq_scorecard_list = []

print("Calculating other dq metrics")
for table in tqdm(unique_table_names):
    table_name = table["Table Name"]
    df = get_table_from_mssql(spark=spark, table_name=table_name, db_ip=database_ip, db_name=database_name)
    completeness_score, count_of_null_values = get_table_completeness_score(df=df)
    uniqueness_score, count_of_duplicate_rows = get_table_uniqueness_score(df=df)

    filtered_df = integrity_metrics_df[integrity_metrics_df['table_name'] == table_name]
    average_dsm = metadata_df_mssql.filter(col("Table Name") == table_name).select("days_since_modified").collect()[0][0]
    freshness_score = calculate_score(average_dsm)

    if not filtered_df.empty:
        ref_integrity_score = filtered_df['average_ref_integrity_score'].values[0]
    else:
        ref_integrity_score = None


    # integrity_score = integrity_df[integrity_df['table_name'] == table_name]['average_ref_integrity_score'].values[0]
    dq_scorecard_dict = {"table_name" : table_name,
                         "completeness_score" : completeness_score,
                         "uniqueness_score" : uniqueness_score,
                         "ref_integrity_score" : ref_integrity_score,
                         "freshness_score" : freshness_score,
                         "avg_dq_score" : weighted_avg_dq_score(completeness_score=completeness_score, completeness_weight=completeness_weight, uniqueness_score=uniqueness_score, uniqueness_weight=uniqueness_weight, ref_integrity_score=ref_integrity_score, ref_integrity_weight=ref_integrity_weight, freshness_score=freshness_score, freshness_weight=freshness_weight),
                         "total_rows" : total_number_of_rows(df=df),
                         "total_columns" : total_number_of_columns(df=df),
                         "count_of_column_name_containing_whitespace" : count_of_column_name_containing_whitespace(df=df),
                         "null_values" : count_of_null_values,
                         "duplicate_rows" : count_of_duplicate_rows,
                         "job_run_timestamp" : job_run_timestamp,
                         "job_run_timestamp_timezone" : job_run_timestamp_timezone}
    dq_scorecard_list.append(dq_scorecard_dict)

dq_scorecard_df = pd.DataFrame(dq_scorecard_list)
dq_scorecard_df.to_csv("dq_scorecard_2.csv", header=True, index=False, mode='w')


