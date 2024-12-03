import pyodbc 
import sqlalchemy as sa
def connectMssql(SERVER,DATABASE,USERNAME,PASSWORD,SCHEMA,connector="pyodbc | sqlalchemy"):
    """
    This fucntion will form connection to MS-SQL server using Server name ,database name ,
    username, password and schema . Fucntion uses pyodbc library and ODBC Driver 18 for 
    SQL Server.
    """
    if connector=="pyodbc":
        print("pyodbc")
        connectionString = f'DRIVER={{ODBC Driver 18 for SQL Server}};\
                            SERVER={SERVER};\
                            DATABASE={DATABASE};\
                            UID={USERNAME};\
                            PWD={PASSWORD};\
                            TrustServerCertificate=yes'
        try:
            conn = pyodbc.connect(connectionString)
            return conn
        except pyodbc.Error as e:
            print("Failed to connect : {}".format(e) )  
    elif connector == "sqlalchemy":
        print("sqlalchemy")
        connectionString = f'mssql+pyodbc://{USERNAME}:{PASSWORD}@{SERVER}/{DATABASE}?driver=ODBC+Driver+17+for+SQL+Server'
        engine = sa.create_engine(connectionString)
        return engine.connect()





