from flask_wtf import FlaskForm
from wtforms import StringField, SubmitField

class SearchForm(FlaskForm):
    dataset = StringField('table')
    column = StringField('colums')
    value = StringField('value')
    submit = SubmitField('Search')