from pyspark.sql import SparkSession, functions

spark = SparkSession.builder.appName("demo").getOrCreate()
data = spark.read.csv("/Users/zijie.jiang/Documents/IdeaProjects/data-engineering/DE/data_Q3_2023", header=True, inferSchema=True)

data = data.groupBy("date").agg(functions.count("*").alias("count"), functions.sum("failure").alias("failures"))

url = 'jdbc:mysql://localhost:13306/pyspark?characterEncoding=UTF-8'
table = 'data_Q3_2023'
mode = 'overwrite'  # append
prop = {'user': 'root', 'password': '123456', 'driver': 'com.mysql.cj.jdbc.Driver'}

data.write.jdbc(url, table, mode, prop)

