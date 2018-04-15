from flask import render_template
from . import home
from .search_form import SearchForm

@home.route('/', methods=['GET', 'POST'])
def index():
    form = SearchForm() 
    return render_template('home/home.html', form=form)

