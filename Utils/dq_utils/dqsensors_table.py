from pyspark.sql.window import Window
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime, timedelta
import hashlib
from src.inHousedqchecks.dqsensors_column import *


# Table-Level Sensors

# Provides the total number of columns in dataframe
def total_number_of_columns(df):
    return len(df.columns)

# Provides the total number of rows in dataframe
def total_number_of_rows(df):
    return df.count()

# Creates a hash of all column names joined in order. To track changes in schema.
def hash_of_column_list_ordered(df): 
    ordered_hash = hashlib.sha256("".join(df.columns).encode()).hexdigest()
    return ordered_hash

# Creates a hash of all column names joined unordered. To track changes in schema.
def hash_of_column_list_unordered(df): 
    unordered_columns = set(df.columns)
    unordered_hash = hashlib.sha256("".join(sorted(unordered_columns)).encode()).hexdigest()
    return unordered_hash


# Provides the count of columns containing whitespace
def count_of_column_name_containing_whitespace(df):
    count = 0
    for col in df.columns:
        if " " in col:
          count += 1
    return count

# Provides the list of columns containing whitespace
def list_of_column_name_containing_whitespace(df):
    columns_with_whitespace = []
    for col in df.columns:
        if " " in col:
          columns_with_whitespace.append(col)
    return columns_with_whitespace



# Provides the count of duplicated rows based on a given column_list
def duplicate_entity_count(df,column_list):
    return df.groupBy(column_list).count().filter(col('count') > 1).count()

# Provides the percentage of duplicated rows based on a given column_list
def duplicate_entity_percentage(df,column_list):
    duplicate_entity_count = df.groupBy(column_list).count().filter(col('count') > 1).count()
    total_entity_count = df.groupBy(column_list).count().count()
    return round((duplicate_entity_count/total_entity_count)*100, 2)

# Provides the count of unique rows based on a given column_list
def unique_entity_count(df,column_list):
    return df.groupBy(column_list).count().filter(col('count') == 1).count()

# Provides the percentage of unique rows based on a given column_list
def duplicate_entity_percentage(df,column_list):
    duplicate_entity_count = df.groupBy(column_list).count().filter(col('count') == 1).count()
    total_entity_count = df.groupBy(column_list).count().count()
    return round((duplicate_entity_count/total_entity_count)*100, 2)

# Shows the list of duplicated entities based on a given column list
def show_duplicate_entity_values(df,column_list):
    partition_window = Window.partitionBy(column_list).orderBy(column_list)
    return df.withColumn('CountColumns',count('*').over(partition_window)).filter('CountColumns>1').drop('CountColumns')


# # Takes a df and SQL expression(condition) as input and gives the count of rows passing the given condition
# def custom_sql_expression_passed_count(df,condition):
#     filtered_df = df.filter(condition)
#     return filtered_df.count()


# Calculates the number of days since the most recent event
def get_data_freshness(df,event_timestamp_col):
    pass

# Calculates the average ingestion delay in hours
def get_data_ingestion_delay(df,event_timestamp_col,ingestion_timestamp_col):
    pass

# Calculates the number of days since the most resecent ingestion job
def get_data_staleness(df,ingestion_timestamp_col):
    pass





def get_table_completeness_score(df):
    #percentage of notnull values in a table
    count_of_nulls = 0
    total_number_of_values = total_number_of_rows(df=df) * total_number_of_columns(df=df)
    for col in df.columns:
        count_of_nulls = count_of_nulls + get_count_of_null_values(df=df, col_name=col)

    completeness_score = ((total_number_of_values - count_of_nulls)/total_number_of_values)*100

    return completeness_score, count_of_nulls


def get_table_uniqueness_score(df):
    # percentage of unique rows in a table
    count = df.distinct().count()
    count_rows = total_number_of_rows(df)
    uniqueness_score = (count / count_rows) * 100
    return uniqueness_score, count_rows - count



def get_referential_integrity_score(original_df, column_name, referenced_df, referenced_column_name):

    unique_values_original_df = original_df.filter(col(column_name).isNotNull()).select(column_name)
    unique_values_referenced_df = referenced_df.select(referenced_column_name)

    if column_name != referenced_column_name:
    # Join the two DataFrames to find missing values
        left_joined_df = unique_values_original_df.join(unique_values_referenced_df, col(column_name) == col(referenced_column_name), 'left')
    else:
        left_joined_df = unique_values_original_df.join(unique_values_referenced_df, referenced_column_name, 'left')

    # Count the missing values
    filtered_df = left_joined_df.filter(col(referenced_column_name).isNull())
    missing_count = filtered_df.count()
    total_row_count = unique_values_original_df.count()
    
    referential_integrity_score = ((total_row_count - missing_count) / total_row_count ) * 100

    return referential_integrity_score





# Define a simple freshness score function (you can adjust this based on your needs)
def calculate_score(days_since_modified):
 
    decay_rate = 0.0027 
    base_score = 100

    score = base_score * (1 - decay_rate * days_since_modified)
    if score < 0:
        return 0
    else:
        return score


def calculate_freshness_score(df, modified_date_column, recent_date=datetime.now().date()):
    # Convert the modified date to a date object
    df = df.withColumn("modified_date", to_date(col(modified_date_column), format="dd/MM/yyyy"))
    # Calculate the difference in days between the recent date and the modified date
    df = df.withColumn("days_since_modified", datediff(lit(recent_date), col("modified_date")))
    # udf to calculate freshness score based on days_since_modified
    # score_udf = udf(calculate_score, FloatType())
    # # Apply the UDF to calculate the freshness score
    # df = df.withColumn("freshness_score", score_udf(col("days_since_modified")))
    # df = df.drop("modified_date")

    return df

