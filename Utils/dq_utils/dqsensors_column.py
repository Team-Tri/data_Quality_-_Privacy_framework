from pyspark.sql.window import Window
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime, timedelta



# Column-Level Sensors

def get_total_row_count(df):
    return df.count()



############# Generic Column-Level Sensors #################

# Data type of column
def get_column_dtype(df,col_name):
    return [dtype for name, dtype in df.dtypes if name == col_name][0]


# Calculates the count of null values in a given column
def get_count_of_null_values(df,col_name):
    count_of_null_values = df.filter(col(col_name).isNull()).count()
    if count_of_null_values == None:
        return 0
    else:
        return int(count_of_null_values)


# Calculates the percentage of null values in a given column
def get_percentage_of_null_values(df,col_name):
    return float((get_count_of_null_values(df,col_name)/df.count())*100)


# Calculates the count of not-null values in a given column
def get_count_of_notnull_values(df,col_name):
    count_of_notnull_values = (df.count() - df.filter(col(col_name).isNull()).count())
    if count_of_notnull_values == None:
        return 0
    else:
        return int(count_of_notnull_values)


# Calculates the percentage of not-null values in a given column
def get_percentage_of_notnull_values(df,col_name):
    return float((get_count_of_notnull_values(df,col_name)/df.count())*100)


def get_passed_rows_status_completeness(df,col_name,new_col_name):
    failed_rows = df.withColumn(f"{new_col_name}", when(col(col_name).isNull(), False).otherwise(True))
    return failed_rows


# Calculates the count of unique values in a given column
def get_count_of_unique_values(df,col_name):
    count_of_unique_values = df.select(col_name).distinct().count()
    if count_of_unique_values == None:
        return 0
    else:
        return int(count_of_unique_values)


# Calculates the percentage of unique values in a given column
def get_percentage_of_unique_values(df,col_name):
    count_of_notnull = get_count_of_notnull_values(df,col_name)
    if count_of_notnull == 0:
        return 0
    else:
        return (get_count_of_unique_values(df,col_name)/count_of_notnull)*100
      

# Calculates the count of duplicate values in a given column
def get_count_of_duplicate_values(df,col_name):
    count_of_duplicate_values = df.groupBy(col_name).count().filter(col('count') > 1).select(sum('count')).collect()[0][0]
    if count_of_duplicate_values == None:
        return 0
    else:
        return int(count_of_duplicate_values)


# Calculates the percentage of duplicate values in a given column
def get_percentage_of_duplicate_values(df,col_name):
    count_of_notnull = get_count_of_notnull_values(df,col_name)
    if count_of_notnull == 0:
        return 0
    else:
        return (get_count_of_duplicate_values(df,col_name)/count_of_notnull)*100
    

def get_passed_rows_status_uniqueness(df, col_name, new_col_name):
    window = Window.partitionBy(col_name)
    df = df.withColumn("duplicate_count", count(col_name).over(window))

    # Create a flag column based on the count
    df = df.withColumn(f"{new_col_name}", when(col("duplicate_count") > 1, False).otherwise(True))
    df = df.drop("duplicate_count")
    return df


def get_passed_rows_status_negatives(df, col_name, new_col_name):
    failed_rows = df.withColumn(f"{new_col_name}", when(col(col_name) < 0, False).otherwise(True))
    return failed_rows


# Calculates the minimum value of a given column
def get_min_value(df,col_name):
    return df.select(min(col_name)).collect()[0][0]


# Provides the maximum value of a given column
def get_max_value(df,col_name):
    return df.select(max(col_name)).collect()[0][0]



############### NUMERICAL COLUMN-LEVEL SENSORS #################

# Calculates the mean value of a given numerical column
def get_mean_value(df,col_name):
    if get_column_dtype(df,col_name) in ['string', 'date', 'timestamp']:
        return 'NA'
    count_of_notnull = get_count_of_notnull_values(df,col_name)
    if count_of_notnull == 0:
        return 0
    else:
        return df.select(mean(col_name)).collect()[0][0]


# Calculates the median value of a given numerical column
def get_median_value(df,col_name):
    if get_column_dtype(df,col_name) in ['string', 'date', 'timestamp']:
        return 'NA'
    else:
        return df.select(median(col_name)).collect()[0][0]


# Calculates the standard deviation of a given numerical column
def get_standard_deviation_value(df,col_name):
    if get_column_dtype(df,col_name) in ['string', 'date', 'timestamp']:
        return 'NA'
    else:
        return df.select(std(col_name)).collect()[0][0]


# Calculates the count of values within 1 standard deviation of the mean
def get_count_of_values_within_1_std(df,col_name):
    if get_column_dtype(df,col_name) in ['string', 'date', 'timestamp']:
        return 'NA'
    else:
        mean_value = df.select(mean(col(col_name))).collect()[0][0]
        std_dev = df.select(stddev(col(col_name))).collect()[0][0]
        # Calculate z-scores
        z_scores = (col(col_name) - mean_value) / std_dev
        # Filter values within 1 standard deviation
        filtered_df = df.filter((z_scores <= 1) & (z_scores >= -1))
        # Count the filtered values
        count_within_1_std = filtered_df.count()
        return count_within_1_std


# Calculates the percentage of values within 1 standard deviation of the mean
def get_percentage_of_values_within_1_std(df,col_name):
    if get_column_dtype(df,col_name) in ['string', 'date', 'timestamp']:
        return 'NA'
    else:
        return (get_count_of_values_within_1_std(df,col_name)/df.count())*100


# Calculates the count of values within 2 standard deviation of the mean
def get_count_of_values_within_2_std(df,col_name):
    if get_column_dtype(df,col_name) in ['string', 'date', 'timestamp']:
        return 'NA'
    else:
        mean_value = df.select(mean(col(col_name))).collect()[0][0]
        std_dev = df.select(stddev(col(col_name))).collect()[0][0]
        # Calculate z-scores
        z_scores = (col(col_name) - mean_value) / std_dev
        # Filter values within 2 standard deviation
        filtered_df = df.filter((z_scores <= 2) & (z_scores >= -2))
        # Count the filtered values
        count_within_2_std = filtered_df.count()
        return count_within_2_std


# Calculates the percentage of values within 2 standard deviation of the mean
def get_percentage_of_values_within_2_std(df,col_name):
    if get_column_dtype(df,col_name) in ['string', 'date', 'timestamp']:
        return 'NA'
    else:
        return (get_count_of_values_within_2_std(df,col_name)/df.count())*100


# Calculates the count of values within 3 standard deviation of the mean
def get_count_of_values_within_3_std(df,col_name):
    if get_column_dtype(df,col_name) in ['string', 'date', 'timestamp']:
        return 'NA'
    else:
        mean_value = df.select(mean(col(col_name))).collect()[0][0]
        std_dev = df.select(stddev(col(col_name))).collect()[0][0]
        # Calculate z-scores
        z_scores = (col(col_name) - mean_value) / std_dev
        # Filter values within 3 standard deviation
        filtered_df = df.filter((z_scores <= 3) & (z_scores >= -3))
        # Count the filtered values
        count_within_3_std = filtered_df.count()
        return count_within_3_std

# Calculates the percentage of values within 3 standard deviation of the mean
def get_percentage_of_values_within_3_std(df,col_name):
    if get_column_dtype(df,col_name) in ['string', 'date', 'timestamp']:
        return 'NA'
    else:
        return (get_count_of_values_within_3_std(df,col_name)/df.count())*100

# Calculates the count of values outside 3 standard deviation of the mean
def get_count_of_values_outside_3_std(df,col_name):
    if get_column_dtype(df,col_name) in ['string', 'date', 'timestamp']:
        return 'NA'
    else:
        mean_value = df.select(mean(col(col_name))).collect()[0][0]
        std_dev = df.select(stddev(col(col_name))).collect()[0][0]
        # Calculate z-scores
        z_scores = (col(col_name) - mean_value) / std_dev
        # Filter values outside 3 standard deviation
        filtered_df = df.filter((z_scores > 3) | (z_scores < -3))
        # Count the filtered values
        count_outside_3_std = filtered_df.count()
        return count_outside_3_std

# Calculates the percentage of values ouside 3 standard deviation of the mean
def get_percentage_of_values_outside_3_std(df,col_name):
    if get_column_dtype(df,col_name) in ['string', 'date', 'timestamp']:
        return 'NA'
    else:
        return (get_count_of_values_outside_3_std(df,col_name)/df.count())*100


# Calculates the 1st quantile(25th percentile, Q1) for a given column
def get_quantile_1_of_values(df,col_name):
    if get_column_dtype(df,col_name) in ['string', 'date', 'timestamp']:
        return 'NA'
    else:
        return df.select(percentile(col(col_name), 0.25)).collect()[0][0]


# Calculates the 3rd quantile(75th percentile, Q1) for a given column
def get_quantile_3_of_values(df,col_name):
    if get_column_dtype(df,col_name) in ['string', 'date', 'timestamp']:
        return 'NA'
    else:
        return df.select(percentile(col(col_name), 0.75)).collect()[0][0]


# Calculates count of valid values in a boxplot
def get_count_of_valid_values_boxplot(df,col_name):
    if get_column_dtype(df,col_name) in ['string', 'date', 'timestamp']:
        return 'NA'
    else:
        q1 = df.select(percentile(col(col_name), 0.25)).collect()[0][0]
        q3 = df.select(percentile(col(col_name), 0.75)).collect()[0][0]
        # Calculate interquartile range
        iqr = q3 - q1
        lower_bound = q1 - (1.5*iqr)
        upper_bound = q3 + (1.5*iqr)
        # Count values within lower bound and upper bound
        return df.filter((col(col_name) >= lower_bound) & (col(col_name) <= upper_bound)).count()


# Calculates percentage of valid values in a boxplot
def get_percentage_of_valid_values_boxplot(df,col_name):
    if get_column_dtype(df,col_name) in ['string', 'date', 'timestamp']:
        return 'NA'
    else:
        return (get_count_of_valid_values_boxplot(df,col_name)/df.count())*100


# Calculates count of outliers in a boxplot
def get_count_of_outliers_boxplot(df,col_name):
    if get_column_dtype(df,col_name) in ['string', 'date', 'timestamp']:
        return 'NA'
    else:
        q1 = df.select(percentile(col(col_name), 0.25)).collect()[0][0]
        q3 = df.select(percentile(col(col_name), 0.75)).collect()[0][0]
        # Calculate interquartile range
        iqr = q3 - q1
        lower_bound = q1 - (1.5*iqr)
        upper_bound = q3 + (1.5*iqr)
        # Count values within lower bound and upper bound
        return df.filter((col(col_name) < lower_bound) | (col(col_name) > upper_bound)).count()


# Calculates percentage of outliers in a boxplot
def get_percentage_of_outliers_boxplot(df,col_name):
    if get_column_dtype(df,col_name) in ['string', 'date', 'timestamp']:
        return 'NA'
    count_of_notnull = get_count_of_notnull_values(df,col_name)
    if count_of_notnull == 0:
        return 0
    else:
        return (get_count_of_outliers_boxplot(df,col_name)/count_of_notnull)*100


# Calculates count of negative values in a given column
def get_count_of_negative_values(df,col_name):
    if get_column_dtype(df,col_name) in ['string', 'date', 'timestamp']:
        return 'NA'
    else:
        return df.filter(col(col_name) < 0).count()

# Calculates percentage of negative values in a given column
def get_percentage_of_negative_values(df,col_name):
    if get_column_dtype(df,col_name) in ['string', 'date', 'timestamp']:
        return 'NA'
    count_of_notnull = get_count_of_notnull_values(df,col_name)
    if count_of_notnull == 0:
        return 0
    else:
        return (get_count_of_negative_values(df,col_name)/count_of_notnull)*100


# Calculates count of non-negative values in a given column
def get_count_of_nonnegative_values(df,col_name):
    if get_column_dtype(df,col_name) in ['string', 'date', 'timestamp']:
        return 'NA'
    else:
        return df.filter(col(col_name) >= 0).count()

# Calculates percentage of non-negative values in a given column
def get_percentage_of_nonnegative_values(df,col_name):
    if get_column_dtype(df,col_name) in ['string', 'date', 'timestamp']:
        return 'NA'
    count_of_notnull = get_count_of_notnull_values(df,col_name)
    if count_of_notnull == 0:
        return 0
    else:
        return (get_count_of_nonnegative_values(df,col_name)/count_of_notnull)*100



############## Text Column-Level Sensors ###############

# Provides the length of shortest value present in a given column
def get_min_text_length(df,col_name):
    if get_column_dtype(df,col_name) in ['int', 'bigint', 'long','double','date','timestamp']:
        return 'NA'
    else:
        filtered_df = df.filter(col(col_name).isNotNull())
        # Calculate the length of each non-null value
        filtered_df = filtered_df.withColumn("length", length(col(col_name)))
        # return the minimum length
        return filtered_df.select(min("length")).collect()[0][0]
    
# Provides the length of longest value present in a given column
def get_max_text_length(df,col_name):
    if get_column_dtype(df,col_name) in ['int', 'bigint', 'long','double','date','timestamp']:
        return 'NA'
    else:
        filtered_df = df.filter(col(col_name).isNotNull())
        # Calculate the length of each non-null value
        filtered_df = filtered_df.withColumn("length", length(col(col_name)))
        # return the minimum length
        return filtered_df.select(max("length")).collect()[0][0]


# Provides the shortest(length-wise) value present in a given column
def get_min_text_length_value(df,col_name):
    if get_column_dtype(df,col_name) in ['int', 'bigint', 'long','double','date','timestamp']:
        return 'NA'
    else:
        filtered_df = df.filter(col(col_name).isNotNull())
        # Calculate the length of each non-null value
        filtered_df = filtered_df.withColumn("length", length(col(col_name)))
        min_length_row = filtered_df.orderBy(col("length").asc()).first()
        # Extract the value from the row
        min_length_value = min_length_row[col_name]
        return min_length_value


# Provides the longest(length-wise) value present in a given column
def get_max_text_length_value(df,col_name):
    if get_column_dtype(df,col_name) in ['int', 'bigint', 'long','double','date','timestamp']:
        return 'NA'
    else:
        filtered_df = df.filter(col(col_name).isNotNull())
        # Calculate the length of each non-null value
        filtered_df = filtered_df.withColumn("length", length(col(col_name)))
        max_length_row = filtered_df.orderBy(col("length").desc()).first()
        # Extract the value from the row
        max_length_value = max_length_row[col_name]
        return max_length_value


def get_count_of_valid_values_text(df, col_name, value_list):
    filtered_df = df.filter(col(col_name).isin(value_list) & col(col_name).isNotNull())
    return filtered_df.count()


def get_percentage_of_valid_values_text(df, col_name, value_list):
    count_of_valid_values = get_count_of_valid_values_text(df, col_name, value_list)
    count_of_notnull = get_count_of_notnull_values(df,col_name)
    if count_of_notnull == 0:
        return 0
    else:
        return (count_of_valid_values/count_of_notnull)*100

################## GOOD/BAD DATA SEGREGATORS ##################

def get_passed_rows_status_validity_text(df, col_name, value_list, new_col_name):
    df_upd = df.withColumn(f"{new_col_name}", when(col(col_name).isin(value_list) | col(col_name).isNull(), True).otherwise(False))
    return df_upd

####################################

def get_count_of_consistent_values_text(df, col_name, reg_exp):
    extracted_values = df.withColumn("extracted_value", regexp_extract(col(col_name), reg_exp, 0))

    # Filter the DataFrame to keep only rows where the extracted value is not empty
    filtered_df = extracted_values.filter(col("extracted_value") != "")

    # Count the number of rows in the filtered DataFrame
    consistent_value_count = filtered_df.count()
    return consistent_value_count

def get_percentage_of_consistent_values_text(df, col_name, reg_exp):
    count_of_consistent_values = get_count_of_consistent_values_text(df, col_name, reg_exp)
    count_of_notnull = get_count_of_notnull_values(df,col_name)
    if count_of_notnull == 0:
        return 0
    else:
        return (count_of_consistent_values/count_of_notnull)*100


def get_passed_rows_status_consistency_text(df, col_name, reg_exp, new_col_name):
    df_upd = df.withColumn(f"{new_col_name}", when(regexp_extract(col(col_name), reg_exp, 0) != "", True).otherwise(False))
    return df_upd

####### Custom SQL Sensor ##########


# def custom_sql_condition(df,table_name,query):
#     df.createOrReplaceTempView(table_name)
#     return spark.sql(query).collect()[0][0]
