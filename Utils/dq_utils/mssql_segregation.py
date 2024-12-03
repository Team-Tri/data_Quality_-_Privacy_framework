from Utils.sparksession.local_spark_utility import *
from Utils.connectors.spark_connectors import *
from Utils.dg_otherUtilities import *
from Utils.dq_utils.dqchecks import *
from Utils.dq_utils.dqsensors_column import get_column_dtype
import os
import datetime
from pyspark.sql.functions import *
import pandas as pd


job_run_timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S %Z')
job_run_timestamp_timezone = str(datetime.datetime.now(datetime.timezone.utc).astimezone().tzinfo)

spark = get_mssql_sparkContext()

os.environ["HADOOP_HOME"] = r"C:\Users\sahil.mate\Downloads\hadoop-3.3.0"

# config_file_path = r"C:\Users\sahil.mate\Desktop\dguniversal\src\inHousedqchecks\checksuite_dimCustomer.json"
config_file_path = r"C:\Users\sahil.mate\Desktop\dguniversal\consolidated_json.json"
config_dictionary = load_config(config_file_path)

# List of dictionaries with results of all data quality checks.
result_list = []

table_name = config_dictionary['table_name']
connection_name = config_dictionary['connection_name']
connection_properties = config_dictionary['connection_properties']

file_type = connection_properties.get("file_type")
file_path = connection_properties.get("file_path")
database_name = connection_properties.get("database_name")
database_ip = connection_properties.get("database_ip")

df = get_table_from_mssql(spark=spark, table_name=table_name, db_ip=database_ip, db_name=database_name)
df = df.select([col(column_name).alias(column_name.upper()) for column_name in df.columns])
print(df.columns)

table_dq_checks = config_dictionary["table_data_quality_checks"]
column_dq_checks = config_dictionary["column_data_quality_checks"]
check_result_folder = config_dictionary['check_result_folder']

dq_checks_module = import_module("src.inHousedqchecks.dqchecks")

checkresult_output_path = check_result_folder + "\\" + f"{connection_name}\\" + f"{table_name}\\" + "dqresults_" + table_name + "_" + datetime.datetime.now().strftime("%Y-%m-%d") + ".csv"
if not os.path.exists(check_result_folder + "\\" + f"{connection_name}"):
    os.mkdir(check_result_folder + "\\" + f"{connection_name}")

if not os.path.exists(check_result_folder + "\\" + f"{connection_name}\\" + f"{table_name}"):
    os.mkdir(check_result_folder + "\\" + f"{connection_name}\\" + f"{table_name}")

if os.path.exists(checkresult_output_path):
    write_mode = 'w'
else:
    write_mode = 'a'


############### RUN TABLE-LEVEL CHECKS ##################

# Iterating through config-file's table_data_quality_checks
for checks in table_dq_checks:
    result_dictionary = {}
    
    result_dictionary["table_name"] = table_name
    result_dictionary["column_name"] = ""

    check_name = checks["check_name"]

    # Collect all json attributes(parameters for check functions and more) which are OPTIONAL
    expected_value = checks.get("expected_value")
    column_list = checks.get("column_list")
    lowerbound = checks.get("lowerbound")
    upperbound = checks.get("upperbound")
    error_margin = checks.get("error_margin")
    check_level = checks.get("check_level")

    # Calling check funtion by passing all optional paramters. Check function will auto-select required paramters.
    check_result_dictionary = eval(f"{check_name}(df=df, column_list=column_list, expected_value=expected_value, lowerbound=lowerbound, upperbound=upperbound, error_margin=error_margin)")

    result_dictionary.update(check_result_dictionary)
    if check_level == None:
        result_dictionary["check_level"] = "Warning"
    else:
        result_dictionary["check_level"] = check_level

    result_dictionary["job_run_timestamp"] = job_run_timestamp
    result_dictionary["job_run_timestamp_timezone"] = job_run_timestamp_timezone

    # Appending result dictionary for each table_dq_check in the result_list.
    result_list.append(result_dictionary)

###################################################################

############### RUN COLUMN-LEVEL CHECKS ##################

# List of dq checks as columnName_checkName
dq_cols_list = []

# Iterating through config-file's column_data_quality_checks
for item in column_dq_checks:
    result_dictionary = {}

    result_dictionary["table_name"] = table_name

    column_name = item["column_name"]
    print(column_name)
    print(get_column_dtype(df, column_name))
    result_dictionary["column_name"] = column_name

    check_name = item["check_name"]
    dq_cols_list.append(column_name + "_" + check_name)

    expected_value = item["expected_value"]   

    # Collect all json attributes(parameters for check functions and more) which are OPTIONAL
    lowerbound = item.get("lowerbound")
    upperbound = item.get("upperbound")
    error_margin = item.get("error_margin")
    value_list = item.get("value_list")
    check_level = item.get('check_level') 

    # Calling check funtion by passing all optional paramters. Check function will auto-select required paramters.
    check_result_dictionary, df_upd = eval(f"{check_name}(df=df, col_name=column_name, expected_value=expected_value, lowerbound=lowerbound, upperbound=upperbound, error_margin=error_margin, value_list=value_list)")

    result_dictionary.update(check_result_dictionary)

    # If check_level is not mentioned in config, setting default check_level to "Warning"
    if check_level == None:
        result_dictionary["check_level"] = "Warning"
    else:
        result_dictionary["check_level"] = check_level


    result_dictionary["job_run_timestamp"] = job_run_timestamp
    result_dictionary["job_run_timestamp_timezone"] = job_run_timestamp_timezone

    # Appending result dictionary for each column_dq_check in the result_list.
    result_list.append(result_dictionary)

    df = df_upd

###################################################################

# Creating Dataframe and writing results to output_path for all data quality checks(table and column) in a single csv file
dq_results_dataframe = pd.DataFrame(result_list)
dq_results_dataframe.to_csv(checkresult_output_path, header=True, mode='w', index=False)

##################### Final DF ########################

df = df.withColumn("row_dq_status", lit(True))
df = df.withColumn("passed_dq_checks", lit(""))
df = df.withColumn("failed_dq_checks", lit(""))


for dqcols in dq_cols_list:
    df = df.withColumn("row_dq_status", col("row_dq_status") & col(dqcols))
    df = df.withColumn("passed_dq_checks", when(col(dqcols) == True, concat(col("passed_dq_checks"),lit(","),lit(dqcols))).otherwise(col("passed_dq_checks")))
    df = df.withColumn("failed_dq_checks", when(col(dqcols) == False, concat(col("failed_dq_checks"),lit(","),lit(dqcols))).otherwise(col("failed_dq_checks")))
    df = df.drop(dqcols)
                 

row_status_passed_final_df = df.filter(col('row_dq_status') == True)
row_status_failed_final_df = df.filter(col('row_dq_status') == False)

good_data_path = check_result_folder + "\\" + f"{connection_name}\\" + f"{table_name}\\" + "goodData"
bad_data_path = check_result_folder + "\\" + f"{connection_name}\\" + f"{table_name}\\" + "badData"

if not os.path.exists(good_data_path):
    os.mkdir(good_data_path)

if not os.path.exists(bad_data_path):
    os.mkdir(bad_data_path)


print('***************************')
print('failed rows')
print(row_status_failed_final_df.show())
row_status_failed_final_df.coalesce(1).write.mode('overwrite').csv(bad_data_path, header=True)
row_status_passed_final_df.coalesce(1).write.mode('overwrite').csv(good_data_path, header=True)


                 

