#!usr/bin/env python

from pyspark.sql import SQLContext
from pyspark.sql.types import *
sqlContext = SQLContext(sc)

raw_sample_schema = StructType([\
StructField("user", IntegerType(), True),\
StructField("time_stamp", IntegerType(), True),\
StructField("adgroup_id", IntegerType(), True),\
StructField("pid", StringType(), True),\
StructField("nonclk", IntegerType(), True),\
StructField("clk", IntegerType(), True),])

raw_sample = spark.read.csv("/user/yl5090/data/raw_sample.csv", header=True, mode="DROPMALFORMED", schema = raw_sample_schema)
