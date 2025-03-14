import os
import traceback
from pathlib import Path
import xmltodict
import pandas as pd
import copy
import dateutil


def parse_xml():
    """
    Parsing *.xml files and creating pd.dataframe table by these files

    :return: dict, dictionary with Source and Destination unique fields for Flask flights user form
    """
    path_to_local_tmp = os.environ['TMP_FOLDER']
    path_to_local_log = os.environ['LOG_FOLDER']
    path_to_local_data = os.environ['DATA_FOLDER']

    try:

        files = list(Path(path_to_local_data).glob("*.xml"))

        if not files:
            with open(Path(path_to_local_log, 'parse_xml.log'), 'w') as f:
                f.write("XML files were not found\n")
            return {'Source': set(), 'Destination': set()}

        flights_data = []

        for file in files:
            with open(file, 'r', encoding='utf-8') as f:
                xml_dict = xmltodict.parse(f.read())

            resp = xml_dict['AirFareSearchResponse']
            request_time = resp.get('@RequestTime')
            response_time = resp.get('@ResponseTime')
            request_id = resp.get('RequestId')

            flights = resp['PricedItineraries']['Flights']['Flight']
            if isinstance(flights, dict):
                flights = [flights]

            for flight in flights:
                pricing = flight['Pricing']
                service_charges = pricing['ServiceCharges']
                if isinstance(service_charges, dict):
                    service_charges = [service_charges]

                pricing_str = '|'.join(
                    f"{charge['@type']}/{charge['@ChargeType']}/{charge['#text']}"
                    for charge in service_charges
                )

                flight_entry = {
                    'RequestTime': request_time,
                    'ResponseTime': response_time,
                    'RequestId': request_id,
                    'XmlFilePath': str(file),
                    'PricingCurrency': pricing.get('@currency', ''),
                    'Pricing [@type/@ChargeType/#text]': pricing_str,
                    'OnwardCarrier @id/#text': f"{flight['Carrier']['@id']}/{flight['Carrier']['#text']}",
                    'FlightNumber': flight.get('FlightNumber', ''),
                    'Source': flight.get('Source', ''),
                    'Destination': flight.get('Destination', ''),
                    'DepartureTimeStamp': flight.get('DepartureTimeStamp', ''),
                    'ArrivalTimeStamp': flight.get('ArrivalTimeStamp', ''),
                    'Class': flight.get('Class', ''),
                    'NumberOfStops': flight.get('NumberOfStops', ''),
                    'FareBasis': flight.get('FareBasis', ''),
                    'WarningText': flight.get('WarningText', ''),
                    'TicketType': flight.get('TicketType', '')
                }

                flights_data.append(flight_entry)

        flights_df = pd.DataFrame(flights_data)

        for col in ['RequestTime', 'ResponseTime', 'DepartureTimeStamp', 'ArrivalTimeStamp']:
            flights_df[col + '_ts'] = flights_df[col].apply(
                lambda x: dateutil.parser.parse(x) if x else pd.NaT
            )

        flights_df['TimeDelta'] = flights_df['ArrivalTimeStamp_ts'] - flights_df['DepartureTimeStamp_ts']

        flights_df.to_csv(Path(path_to_local_tmp, 'flights_df.tsv'), sep='\t', index=False)

        sources = set(flights_df['Source'].dropna().unique()) - {''}
        destinations = set(flights_df['Destination'].dropna().unique()) - {''}

        return {'Source': sources, 'Destination': destinations}

    except Exception as e:
        error_msg = traceback.format_exc()
        with open(Path(path_to_local_log, 'parse_xml.log'), 'w', encoding='utf-8') as f:
            f.write(error_msg)
        return {'Source': set(), 'Destination': set()}


def get_best_flights(departure, destination):
    """
    Getting best flights for departure and destination values

    :param departure: str, departure point
    :param destination: str, destination point
    :return: dict, dictionary structure for best flights
    """
    path_to_local_tmp = os.environ['TMP_FOLDER']
    flights_df = pd.read_csv(Path(path_to_local_tmp, 'flights_df.tsv'), sep='\t').fillna('')

    flights_df['ArrivalTimeStamp_ts'] = pd.to_datetime(flights_df['ArrivalTimeStamp_ts'], errors='coerce')
    flights_df['DepartureTimeStamp_ts'] = pd.to_datetime(flights_df['DepartureTimeStamp_ts'], errors='coerce')

    flights_df_s_d = flights_df[(flights_df['Source'] == departure) & (flights_df['Destination'] == destination)]

    col_lst = list(set(flights_df_s_d.columns) - {'Pricing [@type/@ChargeType/#text]'})
    flights_df_s_d_price = pd.DataFrame(columns=col_lst + ['PricingType', 'PricingChargeType', 'PricingCost'])

    for i, row in flights_df_s_d.iterrows():
        d_obj = {}
        for idx in row.index:
            if idx != 'Pricing [@type/@ChargeType/#text]':
                d_obj[idx] = row[idx]
        pricing_lst = row['Pricing [@type/@ChargeType/#text]'].split('|')
        for el in pricing_lst:
            d_obj_p = copy.deepcopy(d_obj)
            el_arr = el.split('/')
            d_obj_p['PricingType'] = el_arr[0]
            d_obj_p['PricingChargeType'] = el_arr[1]
            d_obj_p['PricingCost'] = el_arr[2]
            flights_df_s_d_price = pd.concat([flights_df_s_d_price, pd.DataFrame([d_obj_p])], ignore_index=True)

    flights_df_s_d_price['PricingCost'] = flights_df_s_d_price['PricingCost'].astype(float)
    flights_df_s_d_price.to_csv(Path(path_to_local_tmp, 'flights_price_df.tsv'), sep='\t', index=False)

    show_col = ['FlightNumber', 'Source', 'Destination', 'DepartureTimeStamp', 'ArrivalTimeStamp', 'Class',
                'NumberOfStops', 'TicketType', 'PricingCost', 'PricingType', 'PricingChargeType', 'TimeDelta']

    flights_df_s_d_price['TimeDiff'] = flights_df_s_d_price.index.map(
        lambda x: flights_df_s_d_price.loc[x, 'ArrivalTimeStamp_ts'] - flights_df_s_d_price.loc[
            x, 'DepartureTimeStamp_ts']
    )

    flights_df_s_d_price_t_am = flights_df_s_d_price[flights_df_s_d_price['PricingChargeType'] == 'TotalAmount']
    min_PricingCost = flights_df_s_d_price_t_am['PricingCost'].min()
    max_PricingCost = flights_df_s_d_price_t_am['PricingCost'].max()

    df_min_PricingCost = flights_df_s_d_price_t_am[flights_df_s_d_price_t_am['PricingCost'] == min_PricingCost][
        show_col]
    df_min_PricingCost.set_index('FlightNumber', inplace=True)
    df_max_PricingCost = flights_df_s_d_price_t_am[flights_df_s_d_price_t_am['PricingCost'] == max_PricingCost][
        show_col]
    df_max_PricingCost.set_index('FlightNumber', inplace=True)

    min_Time = flights_df_s_d_price_t_am['TimeDiff'].min()
    max_Time = flights_df_s_d_price_t_am['TimeDiff'].max()

    df_min_Time = flights_df_s_d_price_t_am[flights_df_s_d_price_t_am['TimeDiff'] == min_Time][show_col]
    df_min_Time.set_index('FlightNumber', inplace=True)
    df_max_Time = flights_df_s_d_price_t_am[flights_df_s_d_price_t_am['TimeDiff'] == max_Time][show_col]
    df_max_Time.set_index('FlightNumber', inplace=True)

    idxs_arr = flights_df_s_d_price_t_am.sort_values(['PricingCost', 'TimeDiff'], ascending=[True, True]).index.tolist()
    if len(idxs_arr) >= 4:
        best_idxs = [idxs_arr[i] for i in range(round(len(idxs_arr) / 2) - 1, round(len(idxs_arr) / 2) + 1)]
        idxs_arr = flights_df_s_d_price_t_am.sort_values(['TimeDiff', 'PricingCost'],
                                                         ascending=[True, True]).index.tolist()
        best_idxs += [idxs_arr[i] for i in range(round(len(idxs_arr) / 2) - 1, round(len(idxs_arr) / 2) + 1)]
        df_best = flights_df_s_d_price_t_am.loc[best_idxs][show_col]
    else:
        df_best = flights_df_s_d_price_t_am.sort_values(['PricingCost', 'TimeDiff'], ascending=[True, True])[show_col]
    df_best.set_index('FlightNumber', inplace=True)

    return {'min_PricingCost': df_min_PricingCost, 'max_PricingCost': df_max_PricingCost,
            'min_Time': df_min_Time, 'max_Time': df_max_Time, 'best_cost_time': df_best}
