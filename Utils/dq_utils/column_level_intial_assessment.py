from Utils.sparksession.local_spark_utility import *
from Utils.connectors.spark_connectors import *
from Utils.dg_otherUtilities import *
from dqsensors_column import *
import os
import datetime
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pandas as pd
from tqdm import tqdm
import warnings
warnings.filterwarnings("ignore")

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

profiling_metrics_list = []
table_name_pointer = None
table_df_pointer = None

print("Calculating profiling metrics")
for row in tqdm(metadata_df_mssql.collect()):

    table_name = row['Table Name']
    column_name = row['Column Name']
    tag_value = row['Tag']
    data_type = row['Data Type']
    column_key_type = row['Key Type']

    if table_name != table_name_pointer:
        table_name_pointer = table_name
        original_df = get_table_from_mssql(spark=spark, table_name=table_name_pointer, db_ip=database_ip, db_name=database_name)
        table_df_pointer = original_df

    try:
        row_count = get_total_row_count(df=table_df_pointer)
    except:
        row_count = "Error"

    try:
        count_of_nulls = get_count_of_null_values(df=table_df_pointer, col_name=column_name)
    except:
        count_of_nulls = "Error"
    
    try:
        distinct_count = get_count_of_unique_values(df=table_df_pointer, col_name=column_name)
    except:
        distinct_count = "Error"

    try:
        if data_type != "VARBINARY":
            min_value = get_min_value(df=table_df_pointer, col_name=column_name)
        else:
            min_value = "NA"
    except:
        min_value = "Error"
    
    try:
        if data_type != "VARBINARY":
            max_value = get_max_value(df=table_df_pointer, col_name=column_name)
        else:
            max_value = "NA"
    except:
        max_value = "Error"
    
    try:
        completeness_score = get_percentage_of_notnull_values(df=table_df_pointer, col_name=column_name)
    except:
        completeness_score = "Error"

    try:
        uniqueness_score = get_percentage_of_unique_values(df=table_df_pointer, col_name=column_name)
    except:
        uniqueness_score = "Error"
        
        

    profiling_metrics_list.append({"table_name" : table_name , "column_name" : column_name, "row_count" : row_count, "completeness_score" : completeness_score,
                                   "uniqueness_score" : uniqueness_score, "count_of_nulls" : count_of_nulls, "distinct_count" : distinct_count, 
                                   "min_value" : min_value, "max_value" : max_value, "tags" : tag_value, "column_key_type" : column_key_type})
    

prof_metrics_df = pd.DataFrame(profiling_metrics_list)
prof_metrics_df.to_csv("profiling_metrics_file.csv", header=True, mode='w', index=False)

