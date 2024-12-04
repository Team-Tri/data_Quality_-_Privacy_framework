from pyspark.sql import SparkSession
# import pydeequ
import pyspark
import os

# os.environ["HADOOP_HOME"] = r"C:\Users\sahil.mate\Downloads\hadoop-3.3.0"
os.environ['SPARK_VERSION'] = "3.5"

print("testing")

def get_local_sparkContext():
    spark = SparkSession\
            .builder\
            .master("local[*]")\
            .appName("test")\
            .getOrCreate() 
    return spark


def get_mssql_sparkContext():
    jars_dir = r"C:\Users\dhanush.shetty\DContracts_DQP\src\jars"
    jdbc_driver_path = [os.path.join(jars_dir, i) for i in os.listdir(jars_dir)]
    jdbc_driver_path = ",".join(jdbc_driver_path)

    print(jdbc_driver_path)
    spark = (SparkSession
        .builder
        .config("spark.jars", jdbc_driver_path)
        .config("spark.ui.port", 4040)
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .appName("app_mssql")
        .getOrCreate())
    
    return spark

def get_dq_segregator_sparkContext():
    jars_dir = r"C:\Users\dhanush.shetty\DContracts_DQP\src\jars"
    jdbc_driver_path = [os.path.join(jars_dir, i) for i in os.listdir(jars_dir)]
    jdbc_driver_path = ",".join(jdbc_driver_path)

    print(jdbc_driver_path)
  
    spark = (SparkSession
        .builder
        .config("spark.jars", jdbc_driver_path)
        .config("spark.ui.port", 4040)
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .appName("dq_segregator12345")
        .getOrCreate())
    
    return spark



# def get_sugestionRunner_sparkContext():

#     # jars_dir = os.path.join(os.getcwd().replace("src", ""), "jars")
#     jars_dir = r"C:\Users\sahil.mate\Desktop\dguniversal\jars"
#     jdbc_driver_path = [os.path.join(jars_dir, i) for i in os.listdir(jars_dir)]
#     jdbc_driver_path = ",".join(jdbc_driver_path)

#     print(jdbc_driver_path)

#     spark = (SparkSession
#         .builder
#         .config("spark.jars", jdbc_driver_path)
#         .config("spark.ui.port", 4040)
#         .config("spark.sql.execution.arrow.pyspark.enabled", "true")
#         .config("spark.jars.packages", pydeequ.deequ_maven_coord)
#         .config("spark.jars.excludes", pydeequ.f2j_maven_coord)
#         .appName("app1")
#         .getOrCreate())
    
#     return spark


# def get_testing123_sparkContext():

#     # jars_dir = os.path.join(os.getcwd().replace("src", ""), "jars")
#     jars_dir = r"C:\Users\sahil.mate\Desktop\dguniversal\jars"
#     jdbc_driver_path = [os.path.join(jars_dir, i) for i in os.listdir(jars_dir)]
#     jdbc_driver_path = ",".join(jdbc_driver_path)

#     print(jdbc_driver_path)

#     spark = (SparkSession
#         .builder
#         .config("spark.jars", jdbc_driver_path)
#         .config("spark.ui.port", 4040)
#         .config("spark.sql.execution.arrow.pyspark.enabled", "true")
#         .config("spark.jars.packages", pydeequ.deequ_maven_coord)
#         .config("spark.jars.excludes", pydeequ.f2j_maven_coord)
#         .appName("testing123")
#         .getOrCreate())
    
#     return spark


# def get_pydeequ_sparkContext():

#     spark = (SparkSession
#     .builder
#     .config("spark.jars.packages", pydeequ.deequ_maven_coord)
#     .config("spark.jars.excludes", pydeequ.f2j_maven_coord)
#     .getOrCreate())

#     return spark


# def get_checkRunner_sparkContext():

#     # jars_dir = os.path.join(os.getcwd().replace("src", ""), "jars")
#     jars_dir = r"C:\Users\sahil.mate\Desktop\dguniversal\jars"
#     jdbc_driver_path = [os.path.join(jars_dir, i) for i in os.listdir(jars_dir)]
#     jdbc_driver_path = ",".join(jdbc_driver_path)

#     print(jdbc_driver_path)

#     spark = (SparkSession
#         .builder
#         .config("spark.jars", jdbc_driver_path)
#         .config("spark.ui.port", 4040)
#         .config("spark.sql.execution.arrow.pyspark.enabled", "true")
#         .config("spark.jars.packages", pydeequ.deequ_maven_coord)
#         .config("spark.jars.excludes", pydeequ.f2j_maven_coord)
#         .appName("app1")
#         .getOrCreate())
    
#     return spark