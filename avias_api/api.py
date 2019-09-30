import logging
import os
import pandas as pd
from pathlib import Path
from flask_bootstrap import Bootstrap
from flask_restplus import Resource, Api, fields
from flask import Flask, jsonify, make_response, url_for, request, Response, render_template

from avias_api.forms import FlightForm
from avias_client.utils import get_best_flights


# some monkey fix of swagger-ui nginx proxy errors
class CustomAPI(Api):
    @property
    def specs_url(self):
        """
        The Swagger specifications absolute url (ie. `swagger.json`)

        :rtype: str
        """

        return url_for(self.endpoint('specs'), _external=False)


app = Flask(__name__, static_url_path='/static', static_folder='static')
app.config['JSON_SORT_KEYS'] = False
app.config['RESTPLUS_VALIDATE'] = True
app.config['SECRET_KEY'] = os.urandom(24)
app.config['WTF_CSRF_ENABLED'] = False
api = CustomAPI(
    app,
    title="Flights API",
    default="Endpoints"
)
app.logger.setLevel(logging.INFO)

bootstrap = Bootstrap(app)


@app.errorhandler(400)
def bad_request(error):
    return make_response(jsonify({'error': "bad request"}), 400)


@app.errorhandler(404)
def not_found(error):
    return make_response(jsonify({'error': "Resource not found"}), 404)


#-----------------------------------------------------------------------------------------------------------------------


@api.route('/flight_options', methods=['GET'], strict_slashes=False)
class FlightOptions(Resource):
    @api.doc(
        responses={
            200: "Success",
            400: "Bad parameters"
        },
        params={
            'departure': "Departure point",
            'destination': "Destination point"
        }
    )
    def get(self):
        """
        Getting flights user form with submit buttons for endpoints

        :return: flask.Response object
        """

        form = FlightForm(request.form, meta={'csrf': False})
        str_html = render_template('flight_form.html', form=form)
        return Response(str_html, mimetype='text/html')


@api.route('/flight_variations', methods=['POST'], strict_slashes=False)
class FlightVariations(Resource):
    @api.doc(
        responses={
            200: "Success",
            400: "Bad parameters"
        },
        params={
            'departure': "Departure point",
            'destination': "Destination point"
        }
    )
    def post(self):
        """
        Getting flight variants from one point to another

        :return: flask.Response object
        """

        form = FlightForm(request.form, meta={'csrf': False})
        departure = request.form['departure']
        destination = request.form['destination']

        show_col = ['FlightNumber', 'Source', 'Destination', 'DepartureTimeStamp', 'ArrivalTimeStamp', 'Class',
                    'NumberOfStops', 'TicketType', 'Pricing [@type/@ChargeType/#text]']

        flights_df = pd.read_csv(Path(os.environ['TMP_FOLDER'], 'flights_df.tsv'), sep='\t').fillna('')
        flights_df = flights_df[(flights_df['Source'] == departure) & (flights_df['Destination'] == destination)][show_col]
        flights_df.set_index('FlightNumber', inplace=True)

        label = "Flights variations from {dep} to {dest}".format(dep=departure, dest=destination)
        label_html = """<html><head> <link rel="stylesheet" href="/static/html_style.css"> </head><body>"""
        label_html += """<p><font size="10">{label}</font></p>""".format(label=label)
        flights_html = label_html + flights_df.to_html() + "</body>"

        return Response(flights_html, mimetype='text/html')


@api.route('/flight_time_price', methods=['POST'], strict_slashes=False)
class FlightTimePrice(Resource):
    @api.doc(
        responses={
            200: "Success",
            400: "Bad parameters"
        },
        params={
            'departure': "Departure point",
            'destination': "Destination point"
        }
    )
    def post(self):
        """
        Getting the most expensive/cheapest, fast/long, and best flights

        :return: flask.Response object
        """

        form = FlightForm(request.form, meta={'csrf': False})
        departure = request.form['departure']
        destination = request.form['destination']

        flights_dict = get_best_flights(departure=departure, destination=destination)
        label = "Getting flights time and price from {dep} to {dest}".format(dep=departure, dest=destination)
        flights_html = """<html><head> <link rel="stylesheet" href="/static/html_style.css"> </head><body>"""
        flights_html += """<p><font size="10">{label}</font></p>""".format(label=label)
        flights_html += "<p>Minimum price and maximum price</p>"
        flights_html += flights_dict['min_PricingCost'].to_html() + '<br>' + flights_dict['max_PricingCost'].to_html()
        flights_html += "<p>Minimum time and maximum time</p>"
        flights_html += flights_dict['min_Time'].to_html() + '<br>' + flights_dict['max_Time'].to_html()
        flights_html += "<p>Best by price and time</p>"
        flights_html += flights_dict['best_cost_time'].to_html() + "</body>"

        return Response(flights_html, mimetype='text/html')


'''@api.route('/route_differences', methods=['POST'], strict_slashes=False)
class RouteDifferences(Resource):
    @api.doc(
        responses={
            200: "Success",
            400: "Bad parameters"
        },
        params={
            'departure': "Departure point",
            'destination': "Destination point"
        }
    )
    def post(self):
        """
        Get the route differences
        """
        
        form = FlightForm(request.form, meta={'csrf': False})
        departure = request.form['departure']
        destination = request.form['destination']
        return Response("Getting route differences from {dep} to {dest}".format(dep=departure, dest=destination),
                        mimetype='text/html')'''
