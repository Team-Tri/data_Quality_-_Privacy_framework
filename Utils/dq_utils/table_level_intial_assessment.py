from Utils.sparksession.local_spark_utility import *
from Utils.connectors.spark_connectors import *
from Utils.HelperFunction.dg_otherUtilities import *
from Utils.dq_utils.dqsensors_table import *
import datetime
from pyspark.sql.functions import *
from pyspark.sql.types import *
from tqdm import tqdm
import warnings
warnings.filterwarnings("ignore")


def get_tablelevel_asessment_stats(spark, table_name, database_name, database_ip, metadata_df):

    job_run_timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S %Z')
    job_run_timestamp_timezone = str(datetime.datetime.now(datetime.timezone.utc).astimezone().tzinfo)

    completeness_weight = 0.5
    uniqueness_weight = 0.2
    ref_integrity_weight = 0.2
    freshness_weight = 0.1
    metadata_df_mssql = metadata_df
    metadata_df_mssql = calculate_freshness_score(df=metadata_df_mssql, modified_date_column="Last Modified Date")

   
    original_df = get_table_from_mssql(spark=spark, table_name=table_name, db_ip=database_ip, db_name=database_name)

    integrity_result_list = []
    # Calculating Referential integrity score
    for row in tqdm(metadata_df_mssql.filter((col("Key Type") == "FK") & (col("Table Name") == table_name)).collect()):
        table_name = row['Table Name']
        column_name = row['Column Name']
        referenced_table_name = row['Reference Table']
        referenced_column_name = row['Reference Column']
        
        if table_name != referenced_table_name:
            referenced_df = get_table_from_mssql(spark=spark, table_name=referenced_table_name, db_ip=database_ip, db_name=database_name)
        else:
            referenced_df = original_df
        
        referential_integrity_score = get_referential_integrity_score(original_df=original_df, column_name=column_name, referenced_df=referenced_df, referenced_column_name=referenced_column_name)

        integrity_result_list.append({"table_name" : table_name , "ref_integrity_score" : referential_integrity_score})

    if not len(integrity_result_list) == 0:
        ref_integrity_sum = 0
        for item in integrity_result_list:
            ref_integrity_sum = ref_integrity_sum + item["ref_integrity_score"]
        ref_integrity_score = ref_integrity_sum / len(integrity_result_list)
    else:
        ref_integrity_score = None 


    completeness_score, count_of_null_values = get_table_completeness_score(df=original_df)
    uniqueness_score, count_of_duplicate_rows = get_table_uniqueness_score(df=original_df)
    # print(table_name)
    # print(metadata_df_mssql.select("Table Name").show())
    # print(metadata_df_mssql.filter(col("Table Name") == table_name).show())
    average_dsm = metadata_df_mssql.filter(col("Table Name") == table_name).select("days_since_modified").collect()[0][0]
    freshness_score = calculate_score(average_dsm)

    # integrity_score = integrity_df[integrity_df['table_name'] == table_name]['average_ref_integrity_score'].values[0]
    dq_scorecard_dict = {"table_name" : table_name,
                         "completeness_score" : completeness_score,
                         "uniqueness_score" : uniqueness_score,
                         "ref_integrity_score" : ref_integrity_score,
                         "freshness_score" : freshness_score,
                         "avg_dq_score" : weighted_avg_dq_score(completeness_score=completeness_score, completeness_weight=completeness_weight, uniqueness_score=uniqueness_score, uniqueness_weight=uniqueness_weight, ref_integrity_score=ref_integrity_score, ref_integrity_weight=ref_integrity_weight, freshness_score=freshness_score, freshness_weight=freshness_weight),
                         "total_rows" : total_number_of_rows(df=original_df),
                         "total_columns" : total_number_of_columns(df=original_df),
                         "count_of_column_name_containing_whitespace" : count_of_column_name_containing_whitespace(df=original_df),
                         "null_values" : count_of_null_values,
                         "duplicate_rows" : count_of_duplicate_rows,
                         "job_run_timestamp" : job_run_timestamp,
                         "job_run_timestamp_timezone" : job_run_timestamp_timezone}
       
    return original_df, dq_scorecard_dict


