from Utils.connectors.spark_connectors import get_table_from_mssql
from Utils.sparksession.local_spark_utility import get_mssql_sparkContext
spark = get_mssql_sparkContext()
DATABASE_NAME = "AdventureWorksDW2022"
DATABASE_IP = "localhost"
table_name = "DIMCUSTOMER"
user_name = "Dhanush"
passwd = "root_123_123"
# df = get_table_from_mssql(spark=spark, table_name=table_name, db_ip=DATABASE_IP, db_name=DATABASE_NAME)
# df.show()
driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
jdbc_url = f"jdbc:sqlserver://10.4.9.84:1433;databaseName={DATABASE_NAME}"
# jdbc_url = f"jdbc:sqlserver://10.4.9.84:1433;databaseName={DATABASE_NAME};encrypt=false;trustServerCertificate=false"


df = spark.read \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", table_name) \
    .option("user", user_name) \
    .option("password", passwd) \
    .option("driver", driver) \
    .load()

# df = spark.read \
#     .format("jdbc") \
#     .option("url", jdbc_url) \
#     .option("dbtable", table_name) \
#     .option("driver", driver) \
#     .load()

df.show()