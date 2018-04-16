#!usr/bin/env python

from pyspark.sql import SQLContext
from pyspark.sql.types import *
sqlContext = SQLContext(sc)

user_profile_schema = StructType([\
StructField("userid", IntegerType(), True),\
StructField("cms_segid", IntegerType(), True),\
StructField("cms_group_id", IntegerType(), True),\
StructField("final_gender_code", IntegerType(), True),\
StructField("age_level", IntegerType(), True),\
StructField("pvalue_level", IntegerType(), True),\
StructField("shopping_level", IntegerType(), True),\
StructField("occupation", IntegerType(), True),\
StructField("new_user_class_level", IntegerType(), True)])

user_profile = spark.read.csv("/user/yl5090/data/user_profile.csv", header=True, mode="DROPMALFORMED", schema = user_profile_schema)
