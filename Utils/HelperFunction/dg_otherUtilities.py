import json
from importlib import import_module
from pyspark.sql.functions import *
from pyspark.sql.types import *

def load_config(config_file_path):
    try:
        with open(config_file_path, 'r') as file:
            print('123')
            return json.load(file)
    except FileNotFoundError:
        raise FileNotFoundError(f"Configuration file '{config_file_path}' not found.")
    except json.JSONDecodeError:
        raise json.JSONDecodeError(f"Invalid JSON data in '{config_file_path}'.")
    

column_level_checkList = ["max_null_count", "max_null_percentage", "min_notnull_count","min_notnull_percentage",
                          "uniques_count_match","min_uniqueness_percentage","max_duplicates_count", "max_duplicates_percentage",
                          "min_value_match", "max_value_match", "mean_value_match", ""]

def call_dq_check(**kwargs):
    
    dq_checks_module = import_module("src.inHousedqchecks.dqchecks")
    dq_check_name = kwargs['dq_check_name']

    # Check if the function exists
    if not hasattr(dq_checks_module, dq_check_name):
        raise AttributeError(f"Data quality check function '{dq_check_name}' not found in 'dqchecks_functions.py'.")

    if dq_check_name in ["max_duplicate_entity_count", "max_duplicate_entity_percentage"]:
        df = kwargs["df"]
        column_list = kwargs["column_list"]
        expected_value = kwargs["expected_value"]
        check_function = getattr(dq_checks_module, dq_check_name)
        return check_function(df, column_list, expected_value)
    elif dq_check_name in column_level_checkList:
        pass
    else:
        df = kwargs["df"]
        expected_value = kwargs["expected_value"]
        check_function = getattr(dq_checks_module, dq_check_name)
        return check_function(df, expected_value)
    


def create_dataframe_integrity_results(spark, data):
    # Define the schema
    schema = StructType([
        StructField("table_name", StringType(), True),
        StructField("ref_integrity_score", FloatType(), True)
    ])
    # Create the DataFrame
    df = spark.createDataFrame(data, schema)
    return df


# Calculates a weighted avg score inclusive of all dimensions
def weighted_avg_dq_score(completeness_score, uniqueness_score, ref_integrity_score, freshness_score, completeness_weight, uniqueness_weight, ref_integrity_weight, freshness_weight):

    total_score = 0
    total_weight = 0

    if completeness_score is not None:
        total_score += completeness_score * completeness_weight
        total_weight += completeness_weight

    if uniqueness_score is not None:
        total_score += uniqueness_score * uniqueness_weight
        total_weight += uniqueness_weight

    if ref_integrity_score is not None:
        total_score += ref_integrity_score * ref_integrity_weight
        total_weight += ref_integrity_weight

    if freshness_score is not None:
        total_score += freshness_score * freshness_weight
        total_weight += freshness_weight

    if total_weight == 0:
        return None  # Handle the case where all scores are None

    weighted_avg = total_score / total_weight
    return weighted_avg
