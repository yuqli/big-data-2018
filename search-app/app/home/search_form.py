from flask_wtf import FlaskForm
from wtforms import StringField, SubmitField, SelectField


class SearchForm(FlaskForm):
    dataset = SelectField(
        'table', choices=['Ad Feature', 'User Profile', 'Behavior Log'])
    column = StringField('colums')
    value = StringField('value')
    aggregate = SelectField('aggregate', choices=[
                            'Max', 'Min', 'Count', 'Count Max', 'Count Min', 'Mean'])
    submit = SubmitField('Search')
