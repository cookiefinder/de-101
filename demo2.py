from pyspark.sql import SparkSession, functions
from pyspark.sql.functions import year, udf
from pyspark.sql.types import StringType

model_map = {
    'CT': 'Crucial',
    'DELLBOSS': 'Dell BOSS',
    'HGST': 'HGST',
    'Seagate': 'Seagate',
    'ST': 'Seagate',
    'TOSHIBA': 'Toshiba',
    'WDC': 'Western Digital'
}


@udf(returnType=StringType())
def get_brand(model):
    for key in model_map.keys():
        if model.startswith(key):
            return model_map[key]
    return 'Others'


spark = SparkSession.builder.appName("demo").getOrCreate()
data = spark.read.csv("data_Q3_2023", header=True, inferSchema=True)

data = (data.groupBy(year("date").alias("year"), get_brand("model").alias("brand"))
        .agg(functions.sum("failure").alias("failures")))

url = 'jdbc:mysql://localhost:13306/pyspark?characterEncoding=UTF-8'
table = 'data_Q3_2023_2'
mode = 'overwrite'  # append
prop = {'user': 'root', 'password': '123456', 'driver': 'com.mysql.cj.jdbc.Driver'}

data.write.jdbc(url, table, mode, prop)
