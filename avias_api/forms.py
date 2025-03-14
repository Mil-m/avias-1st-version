from flask_wtf import FlaskForm
from wtforms import StringField, TextAreaField, FloatField, SubmitField, SelectField
from wtforms.validators import DataRequired, Optional

from avias_client.utils import parse_xml


flights_dict = parse_xml()

def prepare_select_field_data(st):
    return sorted([(el, el) for el in st], key=lambda t: (t[0], t[1]))

class FlightForm(FlaskForm):
    departure = SelectField('Departure point',
                            validators=[DataRequired()],
                            choices=prepare_select_field_data(flights_dict['Source']),
                            render_kw = {'rows': 1, 'cols': 5})
    destination = SelectField('Destination point',
                              validators=[DataRequired()],
                              choices=prepare_select_field_data(flights_dict['Destination']),
                              render_kw={'rows': 1, 'cols': 5})
    submit1 = SubmitField('Get flights variations from one point to another')
    submit2 = SubmitField('Get the most expensive/cheapest, fast/long, and best flights')
    #submit3 = SubmitField('Get route differences')
