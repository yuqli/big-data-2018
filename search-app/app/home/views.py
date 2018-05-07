import requests
from flask import render_template
from . import home
from .search_form import SearchForm

PYSPARK_URL = 'http://0b10e935.ngrok.io/search'


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
        response = requests.get(PYSPARK_URL, params=payload)
        results = response.json()
        return render_template('home/results.html', results=results)

    return render_template('home/home.html', form=form)
