from flask import Flask, jsonify, request
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

app = Flask(__name__)
spark = SparkSession.builder.appName('Big Search').getOrCreate()

ad_feature = spark.read.format('csv').options(
    header='true', inferschema='true').load('/user/yl5090/data/ad_feature.csv')
behavior_log = spark.read.format('csv').options(
    header='true', inferschema='true').load('/user/yl5090/data/behavior_log.csv')
user_profile = spark.read.format('csv').options(
    header='true', inferschema='true').load('/user/yl5090/data/user_profile.csv')
meta = spark.read.format('csv').options(
    header='true', inferschema='true').load('/user/yl5090/data/meta.csv')

behavior_log.createOrReplaceTempView('behavior_log')
user_profile.createOrReplaceTempView('user_profile')
ad_feature.createOrReplaceTempView('ad_feature')
meta.createOrReplaceTempView('meta')
################################################################################
#
################################################################################


def count_unique(var, table):
    """
    Input the variable of interest and the table name
    return the count of unique values of that varaible in that dataset
    """
    return spark.sql('SELECT COUNT(DISTINCT {0}) as unique_count \
    FROM {1}'.format(var, table)).toJSON().collect()

# input a column name, return contents of that columns


def show_column(var, dataset):
    """
    Input a column name, return contents of that column
    """
    return spark.sql('SELECT {0} FROM {1}'.format(var, dataset)).toJSON().collect()


def show_table_with_column(col):
    """
    Give a column name, returns the dataset that contains this field
    """
    columns1 = ['userid', 'user']
    columns2 = ['cate', 'cate_id']
    if col in columns1:
        return spark.sql(
            "SELECT table FROM meta WHERE field = '{0}' OR field = '{1}'"
            .format(columns1[0], columns1[1])).toJSON().collect()
    else:
        if col in columns2:
            return spark.sql(
                "SELECT table FROM meta WHERE field = '{0}' OR field = '{1}'".format(columns2[0], columns2[1])).toJSON().collect()
        else:
            return spark.sql(
                "SELECT table FROM meta WHERE field = '{0}'".format(col)).toJSON().collect()


def col_max(col, dataset):
    """
    Give a column name, returns the max value of this column
    """
    return spark.sql(
        "SELECT MAX({0}) FROM {1}".format(col, dataset)).toJSON().collect()


def col_count(col, dataset):
    return spark.sql('SELECT COUNT {0} as count \
    FROM {1}'.format(col, dataset)).toJSON().collect()


def col_min(col, dataset):
    """
    Give a column name, returns the min value of this column
    """
    return spark.sql(
        'SELECT MIN({0}) FROM {1}'.format(col, dataset)).toJSON().collect()


def col_ave(col, dataset):
    """
    Give a column name, returns the average value of this column
    """
    return spark.sql(
        'SELECT AVG({0}) FROM {1}'.format(col, dataset)).toJSON().collect()


def col_sum(col, dataset):
    """
    Give a column name, returns the sum of this column
    """
    return spark.sql(
        'SELECT SUM({0}) FROM {1}'.format(col, dataset)).toJSON().collect()


def col_most_freq(col, dataset):
    """
    Give a column name, returns the 10 values with most freq
    """
    return spark.sql(
        'SELECT {0}, COUNT(*) AS num_count FROM {1} \
        GROUP BY {2} \
        ORDER BY num_count \
        DESC LIMIT 10'.format(col, dataset, col)).toJSON().collect()


def specific_rows(col, value, dataset):
    """
    Give a column name and a value, returns the rows of the dataset where the
    column value equals to value
    """
    return spark.sql(
        'SELECT * FROM {0} WHERE {1}={2}'.format(
            dataset, col, value)).toJSON().collect()


################################################################################
#
################################################################################
@app.route('/hello', methods=['POST', 'GET'])
def toy_function():
    return 'hello'


@app.route('/test', methods=['GET'])
def test():
    res = col_most_freq('cate_id', 'ad_feature')
    print(res)
    return jsonify(res)


@app.route('/search', methods=['GET'])
def search():
    dataset = request.args['dataset']
    column = request.args['column']
    value = request.args['value']
    aggregate = request.args['aggregate']
    print(dataset, column, value, aggregate)
    res = []
    if aggregate == 'count_unique':
        res = count_unique(column, dataset)
    elif aggregate == 'max':
        res = col_max(column, dataset)
    elif aggregate == 'min':
        res = col_min(column, dataset)
    elif aggregate == 'count':
        pass
    elif aggregate == 'count_freq':
        res = col_most_freq(column, dataset)
    elif aggregate == 'count_min':
        pass
    elif aggregate == 'mean':
        res = col_ave(column, dataset)
    elif aggregate == 'sum':
        res = col_sum(column, dataset)
    # elif aggregation == 'show_column':
    #     res = show_column(column, table)
    # elif aggregation == 'show_table':
    #     res = show_table_with_column(column)
    else:
        res = specific_rows(column, value, date_add)
    return jsonify(res)


app.run(port=8080)
