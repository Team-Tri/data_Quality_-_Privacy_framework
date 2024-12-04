from Utils.dq_utils.dqsensors_table import *
from Utils.dq_utils.dqsensors_column import *
from Utils.dq_utils.dqrules import *
from pyspark.sql.functions import lit


####### TABLE-LEVEL DATA QUALITY CHECKS ########

#### COMPLETENESS #######
def column_count_match(**kwargs):

    df = kwargs['df']
    expected_value = kwargs['expected_value']

    sensor_value = total_number_of_columns(df)
    check_result = {"check_name" : "column_count_match",
                    "check_dimension" : "Completeness",
                    "sensor_value" : sensor_value,
                    "expected_value" : expected_value,
                    "sensor_name" : "total_number_of_columns",
                    "rule_name" : "equals_integer",
                    "passed" : equals_integer(sensor_value, expected_value)}   
    return check_result
##########################

############ VOLUME #############

def row_count_match(**kwargs):

    df = kwargs['df']
    expected_value = kwargs['expected_value']

    sensor_value = total_number_of_rows(df)
    check_result = {"check_name" : "row_count_match",
                    "check_dimension" : "Volume",
                    "sensor_value" : sensor_value,
                    "expected_value" : expected_value,
                    "sensor_name" : "total_number_of_rows",
                    "rule_name" : "equals_integer",
                    "passed" : equals_integer(sensor_value, expected_value)}
    return check_result

def min_row_count(**kwargs):

    df = kwargs['df']
    expected_value = kwargs['expected_value']

    sensor_value = total_number_of_rows(df)
    check_result = {"check_name" : "row_count_match",
                    "check_dimension" : "Volume",
                    "sensor_value" : sensor_value,
                    "expected_value" : expected_value,
                    "sensor_name" : "total_number_of_rows",
                    "rule_name" : "min_count_with_equals",
                    "passed" : min_count_with_equals(sensor_value, expected_value)}
    return check_result

def max_row_count(**kwargs):

    df = kwargs['df']
    expected_value = kwargs['expected_value']

    sensor_value = total_number_of_rows(df)
    check_result = {"check_name" : "row_count_match",
                    "check_dimension" : "Volume",
                    "sensor_value" : sensor_value,
                    "expected_value" : expected_value,
                    "sensor_name" : "total_number_of_rows",
                    "rule_name" : "max_count_with_equals",
                    "passed" : max_count_with_equals(sensor_value, expected_value)}
    return check_result

def row_count_between(**kwargs):

    df = kwargs['df']
    lowerbound = kwargs['lowerbound']
    upperbound = kwargs['upperbound']

    sensor_value = total_number_of_rows(df)
    check_result = {"check_name" : "row_count_between",
                    "check_dimension" : "Volume",
                    "sensor_value" : sensor_value,
                    "expected_value" : "NA",
                    "sensor_name" : "total_number_of_rows",
                    "rule_name" : "between_with_equals",
                    "passed" : between_with_equals(sensor_value, lowerbound, upperbound)}
    return check_result

############## UNIQUENESS #################


def max_duplicate_entity_count(**kwargs):

    df = kwargs['df']
    column_list = kwargs['column_list']
    expected_value = kwargs['expected_value']

    sensor_value = duplicate_entity_count(df, column_list)
    check_result = {"check_name" : "max_duplicate_entity_count",
                    "check_dimension" : "Uniqueness",
                    "sensor_value" : sensor_value,
                    "expected_value" : expected_value,
                    "sensor_name" : "duplicate_entity_count",
                    "rule_name" : "equals_integer",
                    "passed" : equals_integer(sensor_value, expected_value)}
    return check_result


def max_duplicate_entity_percentage(**kwargs):

    df = kwargs['df']
    column_list = kwargs['column_list']
    expected_value = kwargs['expected_value']

    sensor_value = duplicate_entity_percentage(df, column_list)
    check_result = {"check_name" : "max_duplicate_entity_percentage",
                    "check_dimension" : "Uniqueness",
                    "sensor_value" : sensor_value,
                    "expected_value" : expected_value,
                    "sensor_name" : "duplicate_entity_percentage",
                    "rule_name" : "equals",
                    "passed" : equals(sensor_value, expected_value)}
    return check_result


########### COLUMN-LEVEL CHECKS ##########

def check_datatype(**kwargs):

    df = kwargs['df']
    col_name = kwargs['col_name']
    expected_value = kwargs['expected_value']

    sensor_value = get_column_dtype(df,col_name)
    check_result = {"check_name" : "check_datatype",
                    "check_dimension" : "Accuracy",
                    "sensor_value" : sensor_value,
                    "expected_value" : expected_value,
                    "sensor_name" : "get_column_dtype",
                    "rule_name" : "equals_string",
                    "passed" : equals_string(sensor_value, expected_value)}
    return check_result

############ COMPLETENESS ################



def max_null_count(**kwargs):

    df = kwargs['df']
    col_name = kwargs['col_name']
    expected_value = kwargs['expected_value']

    sensor_value = get_count_of_null_values(df, col_name)
    check_result = {"check_name" : "max_null_count",
                    "check_dimension" : "Completeness",
                    "sensor_value" : sensor_value,
                    "expected_value" : expected_value,
                    "sensor_name" : "get_count_of_null_values",
                    "rule_name" : "max_count_with_equals",
                    "passed" : max_count_with_equals(sensor_value, expected_value)}
    
    new_col_name = col_name + "_" + check_result['check_name']
    if check_result['passed'] == False:
        df_upd = get_passed_rows_status_completeness(df, col_name, new_col_name)
    else:
        df_upd = df.withColumn(new_col_name, lit(True))
    
    return check_result, df_upd


def max_null_percentage(**kwargs):

    df = kwargs['df']
    col_name = kwargs['col_name']
    expected_value = kwargs['expected_value']

    sensor_value = get_percentage_of_null_values(df, col_name)
    check_result = {"check_name" : "max_null_percentage",
                    "check_dimension" : "Completeness",
                    "sensor_value" : sensor_value,
                    "expected_value" : expected_value,
                    "sensor_name" : "get_percentage_of_null_values",
                    "rule_name" : "max_with_equals",
                    "passed" : max_with_equals(sensor_value, expected_value)}
    
    new_col_name = col_name + "_" + check_result['check_name']
    if check_result['passed'] == False:
        df_upd = get_passed_rows_status_completeness(df, col_name, new_col_name)
    else:
        df_upd = df.withColumn(new_col_name, lit(True))
    
    return check_result, df_upd


def min_notnull_count(**kwargs):

    df = kwargs['df']
    col_name = kwargs['col_name']
    expected_value = kwargs['expected_value']

    sensor_value = get_count_of_notnull_values(df, col_name)
    check_result = {"check_name" : "min_notnull_count",
                    "check_dimension" : "Completeness",
                    "sensor_value" : sensor_value,
                    "expected_value" : expected_value,
                    "sensor_name" : "get_count_of_notnull_values",
                    "rule_name" : "min_count_with_equals",
                    "passed" : min_count_with_equals(sensor_value, expected_value)}
    
    new_col_name = col_name + "_" + check_result['check_name']
    if check_result['passed'] == False:
        df_upd = get_passed_rows_status_completeness(df, col_name, new_col_name)
    else:
        df_upd = df.withColumn(new_col_name, lit(True))
    
    return check_result, df_upd


def min_notnull_percentage(**kwargs):

    df = kwargs['df']
    col_name = kwargs['col_name']
    expected_value = kwargs['expected_value']

    sensor_value = get_percentage_of_notnull_values(df, col_name)
    check_result = {"check_name" : "min_notnull_percentage",
                    "check_dimension" : "Completeness",
                    "sensor_value" : sensor_value,
                    "expected_value" : expected_value,
                    "sensor_name" : "get_percentage_of_notnull_values",
                    "rule_name" : "min_with_equals",
                    "passed" : min_with_equals(sensor_value, expected_value)}
    
    new_col_name = col_name + "_" + check_result['check_name']
    if check_result['passed'] == False:
        df_upd = get_passed_rows_status_completeness(df, col_name, new_col_name)
    else:
        df_upd = df.withColumn(new_col_name, lit(True))
    
    
    return check_result, df_upd

 ####################################

 ########## UNIQUENESS ##############

def uniques_count_match(**kwargs):

    df = kwargs['df']
    col_name = kwargs['col_name']
    expected_value = kwargs['expected_value']

    sensor_value = get_count_of_unique_values(df, col_name)
    check_result = {"check_name" : "uniques_count_match",
                    "check_dimension" : "Uniqueness",
                    "sensor_value" : sensor_value,
                    "expected_value" : expected_value,
                    "sensor_name" : "get_count_of_unique_values",
                    "rule_name" : "equals_integer",
                    "passed" : equals_integer(sensor_value, expected_value)}
    
    # new_col_name = col_name + "_" + check_result['check_name']
    # if check_result['passed'] == False:
    #     df_upd = get_passed_rows_status_uniqueness(df, col_name, new_col_name)
    # else:
    #     df_upd = df.withColumn(new_col_name, lit(True))
    
    return check_result


def min_uniqueness_percentage(**kwargs):

    df = kwargs['df']
    col_name = kwargs['col_name']
    expected_value = kwargs['expected_value']

    sensor_value = get_percentage_of_unique_values(df, col_name)
    check_result = {"check_name" : "min_uniqueness_percentage",
                    "check_dimension" : "Uniqueness",
                    "sensor_value" : sensor_value,
                    "expected_value" : expected_value,
                    "sensor_name" : "get_percentage_of_unique_values",
                    "rule_name" : "min_with_equals",
                    "passed" : min_with_equals(sensor_value, expected_value)}
    
    new_col_name = col_name + "_" + check_result['check_name']
    if check_result['passed'] == False:
        df_upd = get_passed_rows_status_uniqueness(df, col_name, new_col_name)
    else:
        df_upd = df.withColumn(new_col_name, lit(True))
    
    return check_result, df_upd



def max_duplicates_count(**kwargs):

    df = kwargs['df']
    col_name = kwargs['col_name']
    expected_value = kwargs['expected_value']

    sensor_value = get_count_of_duplicate_values(df, col_name)
    check_result = {"check_name" : "max_duplicates_count",
                    "check_dimension" : "Uniqueness",
                    "sensor_value" : sensor_value,
                    "expected_value" : expected_value,
                    "sensor_name" : "get_count_of_duplicate_values",
                    "rule_name" : "max_count_with_equals",
                    "passed" : max_count_with_equals(sensor_value, expected_value)}
    
    new_col_name = col_name + "_" + check_result['check_name']
    if check_result['passed'] == False:
        df_upd = get_passed_rows_status_uniqueness(df, col_name, new_col_name)
    else:
        df_upd = df.withColumn(new_col_name, lit(True))
    
    return check_result, df_upd


def max_duplicates_percentage(**kwargs):

    df = kwargs['df']
    col_name = kwargs['col_name']
    expected_value = kwargs['expected_value']

    sensor_value = get_percentage_of_duplicate_values(df, col_name)
    check_result = {"check_name" : "max_duplicates_percentage",
                    "check_dimension" : "Uniqueness",
                    "sensor_value" : sensor_value,
                    "expected_value" : expected_value,
                    "sensor_name" : "get_percentage_of_duplicate_values",
                    "rule_name" : "max_with_equals",
                    "passed" : max_with_equals(sensor_value, expected_value)}
    
    new_col_name = col_name + "_" + check_result['check_name']
    if check_result['passed'] == False:
        df_upd = get_passed_rows_status_uniqueness(df, col_name, new_col_name)
    else:
        df_upd = df.withColumn(new_col_name, lit(True))
    
    return check_result, df_upd



######################################

########### VALIDITY ##############


def min_value_match(**kwargs):

    df = kwargs['df']
    col_name = kwargs['col_name']
    expected_value = kwargs['expected_value']

    sensor_value = get_min_value(df, col_name)
    check_result = {"check_name" : "min_value_match",
                    "check_dimension" : "Validity",
                    "sensor_value" : sensor_value,
                    "expected_value" : expected_value,
                    "sensor_name" : "get_min_value",
                    "rule_name" : "equals",
                    "passed" : equals(sensor_value, expected_value)}
    
    return check_result


def min_value_match_with_error_margin(**kwargs):

    df = kwargs['df']
    col_name = kwargs['col_name']
    expected_value = kwargs['expected_value']
    error_margin = kwargs['error_margin']

    sensor_value = get_min_value(df, col_name)
    check_result = {"check_name" : "min_value_match_with_error_margin",
                    "check_dimension" : "Validity",
                    "sensor_value" : sensor_value,
                    "expected_value" : expected_value,
                    "sensor_name" : "get_min_value",
                    "rule_name" : "equals_with_error_margin",
                    "passed" : equals_with_error_margin(sensor_value, expected_value, error_margin)}
    return check_result


def max_value_match(**kwargs):

    df = kwargs['df']
    col_name = kwargs['col_name']
    expected_value = kwargs['expected_value']

    sensor_value = get_max_value(df, col_name)
    check_result = {"check_name" : "max_value_match",
                    "check_dimension" : "Validity",
                    "sensor_value" : sensor_value,
                    "expected_value" : expected_value,
                    "sensor_name" : "get_max_value",
                    "rule_name" : "equals",
                    "passed" : equals(sensor_value, expected_value)}
    return check_result


def max_value_match_with_error_margin(**kwargs):

    df = kwargs['df']
    col_name = kwargs['col_name']
    expected_value = kwargs['expected_value']
    error_margin = kwargs['error_margin']

    sensor_value = get_max_value(df, col_name)
    check_result = {"check_name" : "max_value_match_with_error_margin",
                    "check_dimension" : "Validity",
                    "sensor_value" : sensor_value,
                    "expected_value" : expected_value,
                    "sensor_name" : "get_max_value",
                    "rule_name" : "equals_with_error_margin",
                    "passed" : equals_with_error_margin(sensor_value, expected_value, error_margin)}
    return check_result


def mean_value_match(**kwargs):

    df = kwargs['df']
    col_name = kwargs['col_name']
    expected_value = kwargs['expected_value']

    sensor_value = get_mean_value(df, col_name)
    check_result = {"check_name" : "mean_value_match",
                    "check_dimension" : "Validity",
                    "sensor_value" : sensor_value,
                    "expected_value" : expected_value,
                    "sensor_name" : "get_mean_value",
                    "rule_name" : "equals",
                    "passed" : equals(sensor_value, expected_value)}
    return check_result


def mean_value_match_with_error_margin(**kwargs):

    df = kwargs['df']
    col_name = kwargs['col_name']
    expected_value = kwargs['expected_value']
    error_margin = kwargs['error_margin']

    sensor_value = get_mean_value(df, col_name)
    check_result = {"check_name" : "mean_value_match_with_error_margin",
                    "check_dimension" : "Validity",
                    "sensor_value" : sensor_value,
                    "expected_value" : expected_value,
                    "sensor_name" : "get_mean_value",
                    "rule_name" : "equals_with_error_margin",
                    "passed" : equals_with_error_margin(sensor_value, expected_value, error_margin)}
    return check_result


def max_outliers_percentage(**kwargs):

    df = kwargs['df']
    col_name = kwargs['col_name']
    expected_value = kwargs['expected_value']

    sensor_value = get_percentage_of_outliers_boxplot(df, col_name)
    check_result = {"check_name" : "max_outliers_percentage",
                    "check_dimension" : "Validity",
                    "sensor_value" : sensor_value,
                    "expected_value" : expected_value,
                    "sensor_name" : "get_percentage_of_outliers_boxplot",
                    "rule_name" : "equals",
                    "passed" : equals(sensor_value, expected_value)}
    return check_result


def max_negatives_count(**kwargs):

    df = kwargs['df']
    col_name = kwargs['col_name']
    expected_value = kwargs['expected_value']

    sensor_value = get_count_of_negative_values(df, col_name)
    check_result = {"check_name" : "max_negatives_count",
                    "check_dimension" : "Validity",
                    "sensor_value" : sensor_value,
                    "expected_value" : expected_value,
                    "sensor_name" : "get_count_of_negative_values",
                    "rule_name" : "equals_integer",
                    "passed" : max_count_with_equals(sensor_value, expected_value)}
    
    new_col_name = col_name + "_" + check_result['check_name']
    if check_result['passed'] == False:
        df_upd = get_passed_rows_status_negatives(df, col_name, new_col_name)
    else:
        df_upd = df.withColumn(new_col_name, lit(True))
    
    return check_result, df_upd


def max_negatives_percentage(**kwargs):

    df = kwargs['df']
    col_name = kwargs['col_name']
    expected_value = kwargs['expected_value']

    sensor_value = get_percentage_of_negative_values(df, col_name)
    check_result = {"check_name" : "max_negatives_percentage",
                    "check_dimension" : "Validity",
                    "sensor_value" : sensor_value,
                    "expected_value" : expected_value,
                    "sensor_name" : "get_percentage_of_negative_values",
                    "rule_name" : "equals",
                    "passed" : equals(sensor_value, expected_value)}
    
    new_col_name = col_name + "_" + check_result['check_name']
    if check_result['passed'] == False:
        df_upd = get_passed_rows_status_negatives(df, col_name, new_col_name)
    else:
        df_upd = df.withColumn(new_col_name, lit(True))
    
    return check_result, df_upd


def max_text_length(**kwargs):

    df = kwargs['df']
    col_name = kwargs['col_name']
    expected_value = kwargs['expected_value']

    sensor_value = get_max_text_length(df, col_name)
    check_result = {"check_name" : "max_text_length",
                    "check_dimension" : "Validity",
                    "sensor_value" : sensor_value,
                    "expected_value" : expected_value,
                    "sensor_name" : "get_max_text_length",
                    "rule_name" : "max_count_with_equals",
                    "passed" : max_count_with_equals(sensor_value, expected_value)}
    return check_result


def min_text_length(**kwargs):

    df = kwargs['df']
    col_name = kwargs['col_name']
    expected_value = kwargs['expected_value']

    sensor_value = get_min_text_length(df, col_name)
    check_result = {"check_name" : "min_text_length",
                    "check_dimension" : "Validity",
                    "sensor_value" : sensor_value,
                    "expected_value" : expected_value,
                    "sensor_name" : "get_min_text_length",
                    "rule_name" : "min_count_with_equals",
                    "passed" : min_count_with_equals(sensor_value, expected_value)}
    return check_result


def max_text_length_value_match(**kwargs):

    df = kwargs['df']
    col_name = kwargs['col_name']
    expected_value = kwargs['expected_value']

    sensor_value = get_max_text_length_value(df, col_name)
    check_result = {"check_name" : "max_text_length_value_match",
                    "check_dimension" : "Validity",
                    "sensor_value" : sensor_value,
                    "expected_value" : expected_value,
                    "sensor_name" : "get_max_text_length_value",
                    "rule_name" : "equals_string",
                    "passed" : equals_string(sensor_value, expected_value)}
    return check_result


def min_text_length_value_match(**kwargs):

    df = kwargs['df']
    col_name = kwargs['col_name']
    expected_value = kwargs['expected_value']

    sensor_value = get_min_text_length_value(df, col_name)
    check_result = {"check_name" : "min_text_length_value_match",
                    "check_dimension" : "Validity",
                    "sensor_value" : sensor_value,
                    "expected_value" : expected_value,
                    "sensor_name" : "get_min_text_length_value",
                    "rule_name" : "equals_string",
                    "passed" : equals_string(sensor_value, expected_value)}
    return check_result


def min_valid_values_text_count(**kwargs):

    df = kwargs['df']
    col_name = kwargs['col_name']
    value_list = kwargs['value_list']
    expected_value = kwargs['expected_value']

    sensor_value = get_count_of_valid_values_text(df=df, col_name=col_name, value_list=value_list)
    check_result = {"check_name" : "min_valid_values_text_count",
                    "check_dimension" : "Validity",
                    "sensor_value" : sensor_value,
                    "expected_value" : expected_value,
                    "sensor_name" : "get_count_of_valid_values_text",
                    "rule_name" : "min_count_with_equals",
                    "passed" : min_count_with_equals(sensor_value, expected_value)}
    
    new_col_name = col_name + "_" + check_result['check_name']
    if check_result['passed'] == False:
        df_upd = get_passed_rows_status_validity_text(df=df, col_name=col_name, value_list=value_list, new_col_name=new_col_name)
    else:
        df_upd = df.withColumn(new_col_name, lit(True))

    return check_result, df_upd


def min_valid_values_text_percentage(**kwargs):

    df = kwargs['df']
    col_name = kwargs['col_name']
    value_list = kwargs['value_list']
    expected_value = kwargs['expected_value']

    sensor_value = get_percentage_of_valid_values_text(df=df, col_name=col_name, value_list=value_list)
    check_result = {"check_name" : "min_valid_values_text_percentage",
                    "check_dimension" : "Validity",
                    "sensor_value" : sensor_value,
                    "expected_value" : expected_value,
                    "sensor_name" : "get_percentage_of_valid_values_text",
                    "rule_name" : "min_with_equals",
                    "passed" : min_with_equals(sensor_value, expected_value)}
    
    new_col_name = col_name + "_" + check_result['check_name']
    if check_result['passed'] == False:
        df_upd = get_passed_rows_status_validity_text(df=df, col_name=col_name, value_list=value_list, new_col_name=new_col_name)
    else:
        df_upd = df.withColumn(new_col_name, lit(True))

    return check_result, df_upd


    








