from flask_wtf import FlaskForm
from wtforms import StringField, SubmitField, SelectField


class SearchForm(FlaskForm):
    dataset = SelectField(
        'dataset', choices=[
            ('select', 'Please Select'),
            ('ad_feature', 'Ad Feature'),
            ('user_profile', 'User Profile'),
            ('behavior_log', 'Behavior Log')])
    column = SelectField('columns', choices=[])
    value = StringField('value')
    aggregate = SelectField('aggregate', choices=[
                            ('select', 'Please Select'),
                            ('max', 'Max'),
                            ('min', 'Min'),
                            ('cnt', 'Count'),
                            ('cnt_max', 'Count Max'),
                            ('cnt_min', 'Count Min'),
                            ('mean', 'Mean')
                            ])
    submit = SubmitField('Search')
