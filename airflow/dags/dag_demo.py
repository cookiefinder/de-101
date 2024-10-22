import requests
import io
import zipfile
import pandas
import pendulum
from airflow.decorators import task, dag
from pyspark.sql import functions, SparkSession
from pyspark.sql.functions import year, udf
from pyspark.sql.types import StringType

# API = "https://f001.backblazeb2.com/file/Backblaze-Hard-Drive-Data/data_Q3_2023.zip"
API = "https://github.com/cookiefinder/de-101/raw/refs/heads/main/data_Q3_2023.zip"
base = "./airflow/files"

url = 'jdbc:mysql://localhost:13306/pyspark?characterEncoding=UTF-8'
mode = 'overwrite'  # append
prop = {'user': 'root', 'password': '123456', 'driver': 'com.mysql.cj.jdbc.Driver'}

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


@dag(
    dag_id="etl",
    schedule=None,
    start_date=pendulum.datetime(2024, 10, 17),
    catchup=False,
    tags=["dag_demo"],
)
def etl():
    spark = SparkSession.builder.appName("dag_demo").getOrCreate()

    @task
    def extract():
        res = requests.get(API)
        zip_file = zipfile.ZipFile(io.BytesIO(res.content))
        zip_file.extractall(base)
        return base + '/data_Q3_2023'

    @task
    def transfer1(_path: str):
        data = spark.read.csv(_path, header=True, inferSchema=True)
        data = data.groupBy("date").agg(functions.count("*").alias("count"), functions.sum("failure").alias("failures"))
        return data.toPandas().to_json()

    @task
    def transfer2(_path: str):
        data = spark.read.csv(_path, header=True, inferSchema=True)
        data = (data.groupBy(year("date").alias("year"), get_brand("model").alias("brand"))
                .agg(functions.sum("failure").alias("failures")))
        return data.toPandas().to_json()

    @task
    def load(json_data: str, _tbl_name: str):
        data = spark.createDataFrame(pandas.read_json(json_data))
        data.write.jdbc(url, _tbl_name, mode, prop)

    _path = extract()
    load(transfer1(_path), 'data_Q3_2023')
    load(transfer2(_path), 'data_Q3_2023_2')


try:
    etl()
except Exception as e:
    print(e)
