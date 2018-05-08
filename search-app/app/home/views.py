import requests
import json
from flask import render_template
from . import home
from .search_form import SearchForm

# PYSPARK_URL = 'http://0b10e935.ngrok.io/search'
PYSPARK_URL = 'http://e5d6d7b0.ngrok.io/search'


@home.route('/', methods=['GET', 'POST'])
def index():
    form = SearchForm()
    if form.submit.data:
        payload = {
            'dataset': form.dataset.data,
            'column': form.column.data,
            'value': form.value.data,
            'aggregate': form.aggregate.data
        }
        response = requests.get(PYSPARK_URL, params=payload).json()
        table_rows = []
        table_header = []
        if response is not None:
            for result in response:
                row = json.loads(result)
                table_rows.append(row.values())
            table_header = json.loads(response[0]).keys()
        return render_template('home/results.html',
                               table_header=table_header,
                               table_rows=table_rows)

    return render_template('home/home.html', form=form)
