#!usr/bin/env python

from pyspark.sql import SQLContext
from pyspark.sql.types import *
sqlContext = SQLContext(sc)

behavior_log_schema = StructType([\
StructField("user", IntegerType(), True),\
StructField("time_stamp", IntegerType(), True),\
StructField("btag", StringType(), True),\
StructField("cate", IntegerType(), True),\
StructField("brand", IntegerType(), True),])

behavior_log = spark.read.csv("/user/yl5090/data/behavior_log.csv", header=True, mode="DROPMALFORMED", schema = behavior_log_schema)
