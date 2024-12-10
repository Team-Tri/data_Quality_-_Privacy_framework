import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import pyodbc
from datetime import datetime
import pandas as pd
 
# Initialize Spark session
#spark = SparkSession.builder \
 #   .appName("SQL Server Data Dictionary") \
  ## .getOrCreate()
 
def create_data_dictionary(spark,source_name, host, user, password, database, cnxn):
    # Initialize Spark session
    # spark = SparkSession.builder \
    # .appName("SQL Server Data Dictionary") \
    # .config("spark.driver.extraClassPath", r"C:\path\to\sqljdbc_12.2\enu\mssql-jdbc-12.2.0.jre8.jar") \
    # .getOrCreate()
    try:
        connection_string = f"Driver={{ODBC Driver 17 for SQL Server}};Server={host};Database={database};UID={user};PWD={password};Trusted_Connection={cnxn};"
        with pyodbc.connect(connection_string) as conn:
            cursor = conn.cursor()
 
            # Query to get table names
            cursor.execute("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'")
            tables = cursor.fetchall()
 
            data = []
            for (table_name,) in tables:
                # Query to get columns information for each table
                cursor.execute(f"""
                SELECT
                c.COLUMN_NAME,
                    c.DATA_TYPE,
                    c.IS_NULLABLE,
                    CASE
                        WHEN pk.COLUMN_NAME IS NOT NULL THEN 'PK'
                        WHEN uk.COLUMN_NAME IS NOT NULL THEN 'UK'
                        WHEN fk.COLUMN_NAME IS NOT NULL THEN 'FK'
                        ELSE ''
                    END AS KEY_TYPE,
                    c.CHARACTER_MAXIMUM_LENGTH,
                    c.NUMERIC_PRECISION,
                    c.NUMERIC_SCALE,
                    CONVERT(NVARCHAR(MAX), c.COLUMN_DEFAULT) AS COLUMN_DEFAULT,
                    CONVERT(NVARCHAR(MAX), ep.value) AS COLUMN_DESCRIPTION,
                    COALESCE(fk.REFERENCED_TABLE, '') AS REFERENCE_TABLE,
                    COALESCE(fk.REFERENCED_COLUMN, '') AS REFERENCE_COLUMN,
                    lmd.LastModifiedDate AS LAST_MODIFIED_DATE
 
                FROM INFORMATION_SCHEMA.COLUMNS c
                LEFT JOIN (
                    SELECT ku.TABLE_CATALOG, ku.TABLE_SCHEMA, ku.TABLE_NAME, ku.COLUMN_NAME
                    FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
                    JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE ku
                        ON tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
                        AND tc.CONSTRAINT_NAME = ku.CONSTRAINT_NAME
                ) pk
                ON c.TABLE_CATALOG = pk.TABLE_CATALOG
                AND c.TABLE_SCHEMA = pk.TABLE_SCHEMA
                AND c.TABLE_NAME = pk.TABLE_NAME
                AND c.COLUMN_NAME = pk.COLUMN_NAME
                LEFT JOIN (
                    SELECT ku.TABLE_CATALOG, ku.TABLE_SCHEMA, ku.TABLE_NAME, ku.COLUMN_NAME
                    FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
                    JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE ku
                        ON tc.CONSTRAINT_TYPE = 'UNIQUE'
                        AND tc.CONSTRAINT_NAME = ku.CONSTRAINT_NAME
                ) uk
                ON c.TABLE_CATALOG = uk.TABLE_CATALOG
                AND c.TABLE_SCHEMA = uk.TABLE_SCHEMA
                AND c.TABLE_NAME = uk.TABLE_NAME
                AND c.COLUMN_NAME = uk.COLUMN_NAME
                LEFT JOIN (
                    SELECT
                        fk.name AS FK_NAME,
                        OBJECT_NAME(fk.parent_object_id) AS TABLE_NAME,
                        COL_NAME(fkc.parent_object_id, fkc.parent_column_id) AS COLUMN_NAME,
                        OBJECT_NAME(fk.referenced_object_id) AS REFERENCED_TABLE,
                        COL_NAME(fkc.referenced_object_id, fkc.referenced_column_id) AS REFERENCED_COLUMN
                    FROM
                        sys.foreign_keys fk
                    INNER JOIN
                        sys.foreign_key_columns fkc ON fk.object_id = fkc.constraint_object_id
                ) fk
                ON c.TABLE_NAME = fk.TABLE_NAME
                AND c.COLUMN_NAME = fk.COLUMN_NAME
                LEFT JOIN(
                      SELECT
                        OBJECT_NAME(object_id) AS TableName,
                        CONVERT(VARCHAR(10), modify_date, 103) AS LastModifiedDate
                      FROM
                        sys.objects
                      WHERE
                        type = 'U'
                ) lmd
                ON c.TABLE_NAME = lmd.TableName
                LEFT JOIN sys.extended_properties ep
                    ON ep.major_id = OBJECT_ID(c.TABLE_SCHEMA + '.' + c.TABLE_NAME)
                    AND ep.minor_id = c.ORDINAL_POSITION
                    AND ep.name = 'MS_Description'
                WHERE c.TABLE_NAME = ?
                """, table_name)
 
                columns = cursor.fetchall()
                for column in columns:
                    column_name, data_type, is_nullable, key_type, char_max_length, numeric_precision, numeric_scale, column_default, column_description, reference_table, reference_column, last_modified_date = column
 
                    # Determine data length based on data type
                    if char_max_length is not None:
                        data_length = str(char_max_length)
                    elif numeric_precision is not None:
                        data_length = f"{numeric_precision},{numeric_scale}" if numeric_scale else str(numeric_precision)
                    else:
                        data_length = None
 
                    # Convert None values to empty strings
                    column_default = column_default or ''
                    column_description = column_description or ''
 
                    data.append((
                        table_name.upper(),
                        column_name.upper(),
                        data_type.upper(),
                        data_length,
                        key_type or 'NONE',
                        reference_table.upper() if reference_table else '',
                        reference_column.upper() if reference_column else '',
                        last_modified_date,
                        is_nullable,
                        column_default,
                        column_description
                    ))
 
        # Define schema for PySpark DataFrame
        schema = StructType([
            StructField('Table Name', StringType(), True),
            StructField('Column Name', StringType(), True),
            StructField('Data Type', StringType(), True),
            StructField('Data Length', StringType(), True),
            #StructField('Value Range', IntegerType(),True),
            StructField('Key Type', StringType(), True),
            StructField('Reference Table', StringType(), True),
            StructField('Reference Column', StringType(), True),
            StructField('Last Modified Date', StringType(), True),
            StructField('Is Nullable', StringType(), True),
            StructField('Column Default', StringType(), True),
            StructField('Column Description', StringType(), True)
        ])
 
        # Create PySpark DataFrame from the collected data
        df = spark.createDataFrame(data, schema)
 
        # Write DataFrame to CSV file
        timestamp = datetime.now().strftime("%Y%m%d")
        filename = f"{source_name}_{database}_Export_{timestamp}.csv"
 
        try:
            # Try writing with Spark
            df.coalesce(1).write.csv(filename, header=True, mode='overwrite')
            print(f'Data dictionary for {database} created successfully! File: {filename}')
        except Exception as spark_error:
            print(f"Error writing with Spark: {spark_error}")
            #print("Attempting to write with pandas...")
            try:
                pandas_df = df.toPandas()
                pandas_df.to_csv(filename, index=False)
                print(f'Data dictionary for {database} created successfully using pandas! File: {filename}')
            except Exception as pandas_error:
                print(f"Error writing with pandas: {pandas_error}")
 
    except Exception as e:
        print(f"Error creating data dictionary: {e}")
 
    spark.stop()
 
# Call the function with the correct arguments
# create_data_dictionary("SQLServer","YI1100231LT\\SQLEXPRESS","YASH\\thakur.nikita","Caren=667788","AdventureWorksDW2022","yes")
 
# Stop the Spark session
#spark.stop()