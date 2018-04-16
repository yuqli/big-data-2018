#!usr/bin/env python

from pyspark.sql import SQLContext
from pyspark.sql.types import *
sqlContext = SQLContext(sc)

ad_feature_schema = StructType([\
StructField("adgroup_id", IntegerType(), True),\
StructField("cate_id", IntegerType(), True),\
StructField("campaign_id", IntegerType(), True),\
StructField("customer", IntegerType(), True),\
StructField("brand", IntegerType(), True),\
StructField("price", DoubleType(), True)])

ad_feature = spark.read.csv("/user/yl5090/data/ad_feature.csv", header=True, mode="DROPMALFORMED", schema = ad_feature_schema)

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

raw_sample_schema = StructType([\
StructField("user", IntegerType(), True),\
StructField("time_stamp", IntegerType(), True),\
StructField("adgroup_id", IntegerType(), True),\
StructField("pid", StringType(), True),\
StructField("nonclk", IntegerType(), True),\
StructField("clk", IntegerType(), True),])

raw_sample = spark.read.csv("/user/yl5090/data/raw_sample.csv", header=True, mode="DROPMALFORMED", schema = raw_sample_schema)

behavior_log_schema = StructType([\
StructField("user", IntegerType(), True),\
StructField("time_stamp", IntegerType(), True),\
StructField("btag", StringType(), True),\
StructField("cate", IntegerType(), True),\
StructField("brand", IntegerType(), True),])

behavior_log = spark.read.csv("/user/yl5090/data/behavior_log.csv", header=True, mode="DROPMALFORMED", schema = behavior_log_schema)


# show the schemas
ad_feature.printSchema()
behavior_log.printSchema()
raw_sample.printSchema()
user_profile.printSchema()

# Creating SQL temp view from DF
ad_feature.createOrReplaceTempView("ad_feature")
behavior_log.createOrReplaceTempView("behavior_log")
raw_sample.createOrReplaceTempView("raw_sample")
user_profile.createOrReplaceTempView("user_profile")


################################################################################
# Work on ad_feature dataset
################################################################################

### Count numbers
# Question: How many ad groups are there ?
spark.sql('SELECT COUNT(DISTINCT adgroup_id) as Unique_adgroup \
FROM ad_feature').show()

# Question: How many category IDs are there ?
spark.sql('SELECT COUNT(DISTINCT cate_id) as Unique_category \
FROM ad_feature').show()

# Question: How many campgighs are there ?
spark.sql('SELECT COUNT(DISTINCT campaign_id) as Unique_campaign \
FROM ad_feature').show()

# Question: How many customer are there ?
spark.sql('SELECT COUNT(DISTINCT customer) as Unique_customer \
FROM ad_feature').show()

# Question: How many brands are there ?
spark.sql('SELECT COUNT(DISTINCT brand) as Unique_brand \
FROM ad_feature').show()

def count_unique(var, table):
    """
    Input the variable of interest and the table name
    return the count of unique values of that varaible in that dataset
    """
    spark.sql('SELECT COUNT(DISTINCT {0}) as unique_count \
    FROM {1}'.format(var, table)).show()


### Highest price
# Question: The highest price ads?
spark.sql('SELECT * \
FROM ad_feature \
WHERE price = (SELECT MAX(price) FROM ad_feature)').show()

### Aggregated with Group By
## select most freqent values for each category attributes
spark.sql('SELECT adgroup_id, COUNT(*) AS num_count \
FROM ad_feature \
GROUP BY adgroup_id \
ORDER BY num_count \
DESC LIMIT 10').show()

# check if only really one
spark.sql('SELECT count(*) AS num_count \
From ad_feature \
WHERE adgroup_id = 232238').show()

# comment: this query is invalid as all values only appear once in the dataset

## select most freqent values for each category attributes
spark.sql('SELECT cate_id, COUNT(*) AS num_count \
FROM ad_feature \
GROUP BY cate_id \
ORDER BY num_count \
DESC LIMIT 10').show()

spark.sql('SELECT campaign_id, COUNT(*) AS num_count \
FROM ad_feature \
GROUP BY campaign_id \
ORDER BY num_count \
DESC LIMIT 10').show()

spark.sql('SELECT customer, COUNT(*) AS num_count \
FROM ad_feature \
GROUP BY customer \
ORDER BY num_count \
DESC LIMIT 10').show()

spark.sql('SELECT brand, COUNT(*) AS num_count \
FROM ad_feature \
GROUP BY brand \
ORDER BY num_count \
DESC LIMIT 10').show()

# change another aggregate function: the highest price for each ad group
spark.sql('SELECT adgroup_id, MAX(price) \
FROM ad_feature \
GROUP BY adgroup_id').show()

spark.sql('SELECT brand, MAX(price) \
FROM ad_feature \
GROUP BY brand').show()

spark.sql('SELECT customer, MAX(price) \
FROM ad_feature \
GROUP BY customer').show()

spark.sql('SELECT campaign_id, MAX(price) \
FROM ad_feature \
GROUP BY campaign_id').show()

spark.sql('SELECT cate_id, MAX(price) \
FROM ad_feature \
GROUP BY cate_id').show()

# change another aggregate function: the average price for each ad group
spark.sql('SELECT adgroup_id, AVG(price) \
FROM ad_feature \
GROUP BY adgroup_id').show()

spark.sql('SELECT brand, AVG(price) \
FROM ad_feature \
GROUP BY brand').show()

spark.sql('SELECT customer, AVG(price) \
FROM ad_feature \
GROUP BY customer').show()

spark.sql('SELECT campaign_id, AVG(price) \
FROM ad_feature \
GROUP BY campaign_id').show()

spark.sql('SELECT cate_id, AVG(price) \
FROM ad_feature \
GROUP BY cate_id').show()

# change another aggregate function: the minimum price for each ad group
spark.sql('SELECT adgroup_id, MIN(price) \
FROM ad_feature \
GROUP BY adgroup_id').show()

spark.sql('SELECT brand, MIN(price) \
FROM ad_feature \
GROUP BY brand').show()

spark.sql('SELECT customer, MIN(price) \
FROM ad_feature \
GROUP BY customer').show()

spark.sql('SELECT campaign_id, MIN(price) \
FROM ad_feature \
GROUP BY campaign_id').show()

spark.sql('SELECT cate_id, MIN(price) \
FROM ad_feature \
GROUP BY cate_id').show()



# input a column name, return contents of that columns
def show_column(var, dataset):
    """
    Input a column name, return contents of that column
    """
    spark.sql('SELECT {0} FROM {1}'.format(var, dataset)).show()

# show all features and their tables
def show_all_fields():
    """
    When user calls this function, shows all fields of all datasets
    """
    print('The schemas for table ad_feature is: ')
    ad_feature.printSchema()
    print('The schemas for table behavior_log is: ')
    behavior_log.printSchema()
    print('The schemas for table raw_sample is: ')
    raw_sample.printSchema()
    print('The schemas for table user_profile is: ')
    user_profile.printSchema()

# show all features and their tables
def show_individual_field(table):
    """
    When user calls this function, shows the schema of one table
    """
    if table == ad_feature:
        print('The schemas for table ad_feature is: ')
        ad_feature.printSchema()
    elif table == behavior_log:
        print('The schemas for table behavior_log is: ')
        behavior_log.printSchema()
    elif table == raw_sample:
        print('The schemas for table raw_sample is: ')
        raw_sample.printSchema()
    elif table == user_profile:
        print('The schemas for table user_profile is: ')
        user_profile.printSchema()


print('Which column do you want to check?')
spark.sql('SELECT DISTINCT field FROM meta').show()
# To do : add information about each column here !

# given a column name, which dataset contains this column?
# for this function, need a table with all meta information inside it
meta = spark.read.csv("/user/yl5090/data/meta.csv", header=True, mode="DROPMALFORMED")
meta.createOrReplaceTempView("meta")
def show_table_with_column(col):
    """
    Give a column name, returns the dataset that contains this field
    """
    spark.sql("SELECT * FROM meta WHERE field = '{0}'".format(col)).show()

################################################################################
# Patterns that need two datasets to find
################################################################################
