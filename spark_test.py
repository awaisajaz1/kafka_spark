import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, from_json, to_json, struct


# import os
# print("SPARK_HOME:", os.environ.get("SPARK_HOME"))
# print("PYTHONPATH:", os.environ.get("PYTHONPATH"))
# print("sys.path includes Spark paths?")

# import sys
# for path in sys.path:
#     if "spark" in path.lower():
#         print("  ✔", path)

spark = SparkSession.builder \
      .master("local[1]") \
      .appName("kafkastream") \
      .getOrCreate() 

df = spark.range(5).toDF("numbers")
data = [('James','','Smith','1991-04-01','M',3000),
  ('Michael','Rose','','2000-05-19','M',4000),
  ('Robert','','Williams','1978-09-05','M',4000),
  ('Maria','Anne','Jones','1967-12-01','F',4000),
  ('Jen','Mary','Brown','1980-02-17','F',-1)
]

columns = ["firstname","middlename","lastname","dob","gender","salary"]
df = spark.createDataFrame(data=data, schema = columns)


df = spark.read.csv("G:/sample_data/customer.csv", header=True, inferSchema=True)


df.createOrReplaceTempView("customer")

df2 = spark.sql("SELECT * FROM customer")

df2.show()

spark.stop()




