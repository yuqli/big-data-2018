from flask_wtf import FlaskForm
from wtforms import StringField, SubmitField

class SearchForm(FlaskForm):
    dataset1 = StringField('dataset1')
    dataset2 = StringField('dataset2')
    column1 = StringField('dataset1')
    column2 = StringField('dataset2')
    value = StringField('value')
    submit = SubmitField('Search')