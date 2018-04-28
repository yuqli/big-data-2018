from flask import Flask, request
app = Flask(__name__, instance_relative_config=True)


################################################################################
#
################################################################################

def count_unique(var, table):
    """
    Input the variable of interest and the table name
    return the count of unique values of that varaible in that dataset
    """
    return spark.sql('SELECT COUNT(DISTINCT {0}) as unique_count \
    FROM {1}'.format(var, table)).toJSON().first()

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
    return spark.sql("SELECT * FROM meta WHERE field = '{0}'".format(col)).toJSON().collect()

################################################################################
#
################################################################################
@app.route('/<database>/<table>/<column>/<aggregation>', methods=['GET'])
def search():
    # return 'hello world'
    if aggregation == 'count_unique':
        return count_unique(column, table)
    elif aggregation == 'show_column':
        return show_column(column, table)
    elif aggregation == 'show_table':
        return show_table_with_column(column)

app.run(port=8080)
