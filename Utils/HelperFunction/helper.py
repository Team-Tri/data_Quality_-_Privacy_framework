# pip install pyspark pycryptodome
# pip install cryptography == 41.0.2
from cryptography.fernet import Fernet
import pandas as pd
from Crypto.Cipher import AES
from Crypto.Util.Padding import pad ,unpad
from pyspark.sql.functions import when 
import pyodbc
from src.Utils.connectors.connector import connectMssql
# from helper import fetchDataMssql,createTable
import sqlalchemy as sa
from sqlalchemy import insert
import datetime
import pyodbc,os
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
import ast
# from datetime import datetime
# To Read Excel into spark
def generate_non_deterministic_key():
    """
    Generates a random key for non-deterministic encryption using Fernet.
    """
    return Fernet.generate_key().decode()

def generate_deterministic_key(secret):
    """
    Generates a deterministic key for AES encryption using a secret.
    The key is derived using SHA-256 hashing and truncated to 16 bytes.
    """
    return os.urandom(16)

def create_db_connection(username, config):
    password = config[username]["PASSWORD"]
    server_name = config[username]["SERVER_NAME"]
    database_name = config[username]["DATABASE_NAME"]
    
    # Create the connection string
    connection_string = f"mssql+pyodbc://{username}:{password}@{server_name}/{database_name}?driver=ODBC+Driver+17+for+SQL+Server"
    
    # Create the engine
    engine = create_engine(connection_string)
    
    return engine

def excel_to_spark_df(filepath,spark):
    """
    This function will create spark dataframe using provide local excel 
    file path and spark session ,function returns a Spark DataFrame object
    Function uses pandas to read the data
    """
    df = pd.read_excel(filepath)
    df_columns = df.columns
    for col in df_columns:
        if pd.api.types.is_numeric_dtype(df[col]):
        # Fill nulls with NaN for numeric dtype preservation
            df[col].fillna(pd.NA, inplace=True)
        else:
        # Fill nulls with appropriate empty value for other dtypes
            df[col].fillna('', inplace=True)
 
    df = spark.createDataFrame(df)
 
    for item in df_columns:
       df = df.withColumn(item, when(df[item] == "", None).otherwise(df[item]))
       df = df.withColumn(item, when(df[item] == "NaN", None).otherwise(df[item]))
 
    return df

# Function to encrypt data
def encrypt_data(key, data,en_type="Non-Deterministic"):
  """
  THis Function returns encrypted data 
  based on type of encryption needed 
  i.e deterministic or non-deterministic 
  created using key 
  """
  if en_type=="Non-Deterministic":
    fernet = Fernet(key)
    encrypted_data = fernet.encrypt(str(data).encode())
    return encrypted_data.decode()
  elif en_type=="Deterministic":
    key=ast.literal_eval(key)
    if len(key) != 16:
        raise ValueError("AES key must be 16 bytes long")
    else:
      cipher = AES.new(key, AES.MODE_ECB)
      encrypted_data = cipher.encrypt(pad(data.encode(), AES.block_size))
      return encrypted_data.hex()
  else:
    print("Pass encryption type ")




# Function to decrypt data
def decrypt_data(key, encrypted_data ,en_type):
  if en_type=="Non-Deterministic":
    fernet = Fernet(key)
    decrypted_data = fernet.decrypt(encrypted_data).decode()
    return decrypted_data
  elif en_type=="Deterministic" :
    if len(key) != 16:
        raise ValueError("AES key must be 16 bytes long")
    else:
      cipher = AES.new(key, AES.MODE_ECB)
      decrypted_padded = cipher.decrypt(bytes.fromhex(encrypted_data))
      decrypted_data = unpad(decrypted_padded, AES.block_size)
      return decrypted_data.decode()
  else:
    print("Pass encryption type ")


def fetchDataMssql(connection, table_name, columns):
    """Generator function to yield data chunks from SQL Server."""
    
    query = sa.text(f"SELECT {columns} FROM {table_name};")
    print(f"Constructed query: {query}")
    
    if isinstance(connection, pyodbc.Connection):
        print("pyodbc instance")
        cursor = connection.cursor()
        data = cursor.execute(str(query)).fetchall()
        cursor.close()
        return data
    elif isinstance(connection, sa.engine.Connection):
        print("sqlalchemy instance")
        result = connection.execute(query)
        data = result.fetchall()
        result.close()
        return data
    else:
        raise TypeError("Unsupported connection type")



def createTable(connection, table_name, column_data):
    """
    Creates a table in the specified database with the given column names and types.

    Args:
        connection (pyodbc.Connection): The ODBC connection object.
        table_name (str): The name of the table to create.
        column_names (list): A list of column names.
        column_types (list): A list of corresponding data types for the columns.

    Returns:
        None
    """

    cursor = connection

    try:
        # Construct the CREATE TABLE statement
        create_table_sql = f"IF (OBJECT_ID('{table_name}') IS NULL) BEGIN CREATE TABLE  {table_name} ("
        flag=1
        for column_name, column_type,CHARACTER_MAXIMUM_LENGTH,Key_Type,Reference in column_data.values:
            # print("this is column name :" + column_name)
            data_type=" NVARCHAR("+f"{int(CHARACTER_MAXIMUM_LENGTH)})" if column_type.upper() == "NVARCHAR" or column_type.upper() == "NVARCHAR" else column_type

            if Key_Type.upper() == "PK":
                if flag:
                    create_table_sql += f"{column_name} " + f"{data_type} " + f"Primary Key,"
                    flag=0
            elif Key_Type.upper() == "UK":
               create_table_sql += f"{column_name} " + f"{data_type} " + f"Unique ({column_name}),"
            elif Key_Type.upper() == "FK" and table_name.split("_")[1] != Reference:
                create_table_sql += f"{column_name} " + f"{data_type} " + f"FOREIGN KEY REFERENCES {Reference},"
            else:
               create_table_sql += f"{column_name} " + f"{data_type},"

        create_table_sql = create_table_sql[:-1]  # Remove the trailing comma
        create_table_sql += ") END "

        gen_query =sa.text(create_table_sql)
        # print(gen_query)

        # Execute the statement
        cursor.execute(gen_query)
        connection.commit()
        print(f"Table '{table_name}' created successfully.")

    except pyodbc.Error as e:
        print(f"Error creating table: {e}")
    finally :
    #    cursor.close()
        pass
    

    

def insert_data_to_table(conn, table, df):
    # data_type = df.dtypes 
    # print(data_type)
    try:
        df1 = df.where((pd.notnull(df)), None)
        datetime_cols = df1.select_dtypes(include='datetime64').columns
        print(datetime_cols)
        df1[datetime_cols] = df1[datetime_cols].apply(convert_datetime)
        print(df1.head())
        value_=df1.to_dict(orient='records')
        print(value_)
        for row in value_:
            # Convert datetime columns
            print(row)  # For debugging; consider removing in production
            # Prepare the column names and values
            column_list = df.columns.values.tolist()
            columns_ = ', '.join(column_list)
            placeholders = ', '.join(['?'] * len(column_list))  # Parameterized placeholders
            query_ = f"INSERT INTO {table} ({columns_}) VALUES ({placeholders})"
            print(query_)  # For debugging; consider removing in production
            # Execute the query with the row values
            conn.execute(sa.text(query_), row)
            conn.commit()
    except pyodbc.Error as e:
        print(f"Error inserting into table: {e}")

def is_datetime_dtype(dtype):
    """Check if a dtype is a datetime type."""
    return pd.api.types.is_datetime64_any_dtype(dtype)

def convert_datetime(value):
    """Convert pandas timestamp to Python datetime."""
    if pd.isna(value):
        return None
    if isinstance(value, pd.Timestamp):
        return value.to_pydatetime().strftime("%Y-%m-%d %H:%M:%S")
    return value
# def insert_data_to_table(conn, table, df):
#     # Create a cursor object
#     # cursor = conn.cursor()

#     data_type = df.dtypes

#     try:
#         for _, row in df.iterrows():
#             # Convert datetime columns
#             row = [convert_datetime(value) if is_datetime_dtype(dtype) else value 
#                    for value, dtype in zip(row, data_type)]
#             row = tuple('NULL' if v is None else f'{v}' if isinstance(v, str) else v for v in row)

            
#             column_list = df.columns.values.tolist()
#             columns_ = str(tuple(column_list) if len(column_list) > 1 else f"({column_list})").replace("[","").replace("]","").replace("'","").replace(" ","") 
#             print(column_list)
#             print(columns_)
#             query_ = "INSERT INTO " + f"{table} {columns_}" + f" VALUES {tuple(row)}"
#             print(query_)
#             conn.execute(sa.text(query_))
#             conn.commit()
#     except pyodbc.Error as e:
#         print(f"Error inserting into table: {e}")
#     finally:
#         # cursor.close()
#         pass

# def is_datetime_dtype(dtype):
#     """Check if a dtype is a datetime type."""
#     return pd.api.types.is_datetime64_any_dtype(dtype)
# def convert_datetime(value):
#     """Convert pandas timestamp to Python datetime."""
#     if pd.isna(value):
#         return None
#     if isinstance(value, pd.Timestamp):
#         return value.to_pydatetime()
#     return value

    
def create_dependency_graph(pii_metadata):
    graph = {}
    for _, row in pii_metadata.iterrows():
        table_name = row['Table Name']
        if table_name not in graph:
            graph[table_name] = []
        if row['Key Type'] == 'FK' and pd.notna(row['Reference Table']):
            graph[table_name].append(row['Reference Table'])
    return graph

def topological_sort(graph):
    visited = set()
    stack = []

    def dfs(node):
        visited.add(node)
        for neighbor in graph.get(node, []):
            if neighbor not in visited:
                dfs(neighbor)
        stack.append(node)

    # Create a list of nodes to iterate over
    nodes = list(graph.keys())
    for node in nodes:
        if node not in visited:
            dfs(node)

    # Reverse the stack to get the correct order
    return stack[::-1]



def dataLoaderToMssql(source_connection,target_connection,pii_metadata):
    """
    This function will fetch data from mssql into pandas dataframe ,
    then load it back into mssql PII tables which is present in Pivate Database

    ARGUMENTS:
    source_connection =  Its an connection instance from either sqlalchemy or pyodbc to source database
    target_connection = Its an connection instance from either sqlalchemy to target database
    ITS COMPLUSORY TO HAVE SQLALCHEMY CONNECTOR AS PANDAS USE SQLALCHEMY WHICH WIlL BE USED FOR DATA TRANSFER
    pii_metadata = Required metadata of source in pandas dataframe 

    Flow:
    1.Connection will be established with Mssql
    2.Pii_Metadata will be looped for all table names in metadata
    3.Inside loop table will be created using metadata [createTable()]
    4.Pii Column Data and Primary and foriegn key data (which are necessary to maintain relationship)
    Will be fecthed for each table in loop using pii metadata [fetchDataMssql()]
    5.Senstive Data will be moved to Pandas dataframe 
    6.From dataframe will be appended into newly created pii tables using target_connection & pd.df.to_sql()
    """

    # Create dependency graph
    dependency_graph = create_dependency_graph(pii_metadata)
    # print(dependency_graph)
    
    # Get the sorted order of tables
    sorted_tables = topological_sort(dependency_graph)
    
    # Filter the sorted tables to include only those in the original list
    tables_name_list = pii_metadata["Table Name"].unique()
    sorted_tables = [table for table in sorted_tables if table in tables_name_list]
    
    # Add any remaining tables that weren't in the dependency graph
    sorted_tables.extend([table for table in tables_name_list if table not in sorted_tables])
    # print(sorted_tables)
    
    for table in sorted_tables:
        #This variable will contain all the tables column names which are either pii or pk/fk  in dataframe 
        pii_columns_df = pii_metadata[(pii_metadata["Table Name"]==table)\
                                            &((pii_metadata["Pii"]=='Yes')\
                                            |((pii_metadata["Key Type"] != "NONE")&(pii_metadata["Key Type"] != "UK")))]
        #This variable will saving column names in string format to be ussed in automate query generation
        pii_columns_str=str(pii_columns_df["Column Name"].to_list()).replace("[","").replace("]","").replace("'","").replace(" ","") 
        # print(pii_columns_str)

        #This is template for sql query to fetch data for tables which are in loop
        data_query=f"""     
                    SELECT 
                        {pii_columns_str}
                    FROM 
                        {table};
                    """
        #This if statement works as checker to pass only tables which has pii/pk/fk columns to be copied in data vault
        if pii_columns_str:
            #fecthDataMssql will fetch data from source for 
            data_= fetchDataMssql(source_connection,data_query)
            #Data will be converted to pandas dataframe
            data_df =  pd.DataFrame.from_records(data_,columns=pii_columns_str.split(","))
            # print(data_df.head())
            #try block will make sure code wouldnt stop if theres issue with table creation as sometimes connection timesout 
            try :
                #this variable will store necessary data for table creation from metadata
                ddl_data=pii_columns_df[["Column Name","Data Type","Data Length","Key Type","Reference Table"]]
                createTable(target_connection,f"Pii_{table}",ddl_data)
            except Exception as e1:
                # Check if the error is related to table already existing (e.g., error code or message)
                if 'already exists' in str(e1):
                    print(f"Table 'Pii_{table}' already exists. Skipping creation.")
                else:
                    current_time = datetime.datetime.now().strftime("%d-%m-%Y %H:%M:%S")
                    # print(e1)
                    # print(current_time)
                    pd.DataFrame([[current_time,str(e1)]],columns=["Time","Error"]).to_csv("logs\\creationError_log.csv",mode = "a",index=False)

            #This Block will make sure if insert fails then code doesnt stop 
            try :
                #This code will write data into mssql
                # data_df[pii_columns_str.split(",")].to_sql(f"Pii_{table}", target_connection, index=False , if_exists="append",
                                                        #    dtype={col: sa.types.VARCHAR(data_df[col].str.len().max()) for col in data_df.select_dtypes(include=['object'])})
                insert_data_to_table(target_connection,f"Pii_{table}",data_df)
                print(f"Successfull  inserted into {table}")
            except Exception as e2:
                print("insertion error " + str(e2))
                current_time = datetime.datetime.now().strftime("%d-%m-%Y %H:%M:%S")
                pd.DataFrame([[current_time,e2]],columns=["Time","Error"]).to_csv("logs\\insertionError_log.csv",mode = "a",index=False)