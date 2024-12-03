
# Returns a spark dataframe for given table_name, database_ip, database_name
def get_table_from_mssql(spark, table_name, db_ip, db_name):

    driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    jdbc_url = f"jdbc:sqlserver://{db_ip};databaseName={db_name};integratedSecurity=true;trustServerCertificate=true"

    df = spark.read \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", table_name) \
        .option("driver", driver) \
        .load()

    return df

def get_table_from_csv(spark, file_path):
    df = spark.read.csv(file_path,header=True,multiLine=True,inferSchema=True)
    return df
    

