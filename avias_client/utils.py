import os
import pathlib
from pathlib import Path
import xmltodict
import json
import pandas as pd
import copy
import dateutil

def parse_xml():

    def get_flight_dict_obj(onw_ret_flight, d_obj_onw_ret):
        onw_st = onw_ret_flight['Carrier']['@id'] + '/' + onw_ret_flight['Carrier']['#text']
        d_obj_onw_ret['OnwardCarrier @id/#text'] = onw_st
        for k in set(onw_ret_flight.keys()) - {'Carrier'}:
            d_obj_onw_ret[k] = onw_ret_flight[k]
        return d_obj_onw_ret

    flights_df = pd.DataFrame(columns=['RequestTime', 'ResponseTime', 'RequestId', 'PricingCurrency',
                                       'Pricing [@type/@ChargeType/#text]', 'OnwardCarrier @id/#text',
                                       'FlightNumber', 'Source', 'Destination', 'DepartureTimeStamp',
                                       'ArrivalTimeStamp', 'Class', 'NumberOfStops', 'FareBasis',
                                       'WarningText', 'TicketType'])

    path_to_local_tmp = os.environ['TMP_FOLDER']
    path_to_local_log = os.environ['LOG_FOLDER']
    path_to_local_data = os.environ['DATA_FOLDER']
    folder_files = [Path(path_to_local_data, x) for x in os.listdir(path_to_local_data)]
    xml_files = [str(x) for x in folder_files if x.suffix == '.xml']
    if not xml_files:
        with open(Path(path_to_local_log, 'parse_xml.log'), 'w') as f:
            f.write("xml files were not found\n")

    json_files = dict()

    try:
        for f_path in xml_files:
            with open(f_path, 'r') as f:
                data = f.read()
            js_obj = json.loads(json.dumps(xmltodict.parse(data)))
            json_files[f_path] = js_obj

        for key in json_files.keys():
            flights_dict = json_files[key]['AirFareSearchResponse']
            d_obj = dict()
            d_obj['RequestTime'] = flights_dict['@RequestTime']
            d_obj['ResponseTime'] = flights_dict['@ResponseTime']
            d_obj['RequestId'] = flights_dict['RequestId']
            d_obj['XmlFilePath'] = key

            flights_el = flights_dict['PricedItineraries']['Flights']

            for flight in flights_el:
                d_obj['PricingCurrency'] = flight['Pricing']['@currency']
                service_charges_el = flight['Pricing']['ServiceCharges']
                st_arr = []
                for el in service_charges_el:
                    st_arr.append(
                        "{ty}/{ch_type}/{text}".format(ty=el['@type'], ch_type=el['@ChargeType'], text=el['#text']))
                d_obj['Pricing [@type/@ChargeType/#text]'] = '|'.join(st_arr)

                if 'OnwardPricedItinerary' in flight:
                    onward_el = flight['OnwardPricedItinerary']['Flights']['Flight']
                    if isinstance(onward_el, dict):
                        onward_el = [onward_el]
                    for onw_flight in onward_el:
                        d_obj_onw_ret = get_flight_dict_obj(onw_ret_flight=onw_flight, d_obj_onw_ret=copy.deepcopy(d_obj))
                        flights_df = flights_df.append(d_obj_onw_ret, ignore_index=True)

                if 'ReturnPricedItinerary' in flight:
                    ret_el = flight['ReturnPricedItinerary']['Flights']['Flight']
                    if isinstance(ret_el, dict):
                        ret_el = [ret_el]
                    for ret_flight in ret_el:
                        d_obj_onw_ret = get_flight_dict_obj(onw_ret_flight=ret_flight, d_obj_onw_ret=copy.deepcopy(d_obj))
                        flights_df = flights_df.append(d_obj_onw_ret, ignore_index=True)

                flights_df = flights_df.append(d_obj, ignore_index=True)
    except Exception as e:
        with open(Path(path_to_local_log, 'parse_xml.log'), w) as f:
            f.write("{err}\n".format(err=str(e)))

    flights_df = flights_df.fillna('')
    for col in ['RequestTime', 'ResponseTime', 'DepartureTimeStamp', 'ArrivalTimeStamp']:
        flights_df[col + '_ts'] = flights_df[col].apply(lambda x: dateutil.parser.parse(x) if x else None)

    flights_df['TimeDelta'] = list(
        map(lambda x: flights_df.loc[x, 'ArrivalTimeStamp_ts'] - flights_df.loc[x, 'DepartureTimeStamp_ts'], flights_df.index)
    )

    for col in ['RequestTime', 'ResponseTime', 'DepartureTimeStamp', 'ArrivalTimeStamp']:
        flights_df[col + '_ts'] = flights_df[col].apply(lambda x: pd.to_datetime(x).value / 1000000000 if x else None)

    flights_df.to_csv(Path(path_to_local_tmp, 'flights_df.tsv'), sep='\t', index=False)

    return {'Source': set(flights_df['Source'].unique()) - {''},
            'Destination': set(flights_df['Destination'].unique()) - {''}}

def get_best_flights(departure, destination):
    path_to_local_tmp = os.environ['TMP_FOLDER']
    flights_df = pd.read_csv(Path(path_to_local_tmp, 'flights_df.tsv'), sep='\t').fillna('')
    flights_df_s_d = flights_df[(flights_df['Source'] == departure) & (flights_df['Destination'] == destination)]

    col_lst = list(set(flights_df_s_d.columns) - {'Pricing [@type/@ChargeType/#text]'})
    flights_df_s_d_price = pd.DataFrame(
        columns=col_lst + ['PricingType', 'PricingChargeType', 'PricingCost']
    )

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

            flights_df_s_d_price = flights_df_s_d_price.append(d_obj_p, ignore_index=True)

    flights_df_s_d_price['PricingCost'] = flights_df_s_d_price['PricingCost'].astype(float)
    flights_df_s_d_price.to_csv(Path(path_to_local_tmp, 'flights_price_df.tsv'), sep='\t', index=False)

    show_col = ['FlightNumber', 'Source', 'Destination', 'DepartureTimeStamp', 'ArrivalTimeStamp', 'Class',
                'NumberOfStops', 'TicketType', 'PricingCost', 'PricingType', 'PricingChargeType', 'TimeDelta']

    flights_df_s_d_price_t_am = flights_df_s_d_price[flights_df_s_d_price['PricingChargeType'] == 'TotalAmount']
    min_PricingCost = flights_df_s_d_price_t_am['PricingCost'].min()
    max_PricingCost = flights_df_s_d_price_t_am['PricingCost'].max()

    df_min_PricingCost = flights_df_s_d_price_t_am[flights_df_s_d_price_t_am['PricingCost'] == min_PricingCost][show_col]
    df_min_PricingCost.set_index('FlightNumber', inplace=True)
    df_max_PricingCost = flights_df_s_d_price_t_am[flights_df_s_d_price_t_am['PricingCost'] == max_PricingCost][show_col]
    df_max_PricingCost.set_index('FlightNumber', inplace=True)

    flights_df_s_d_price['TimeDiff'] = list(map(
        lambda x: flights_df_s_d_price.loc[x, 'ArrivalTimeStamp_ts'] - flights_df_s_d_price.loc[
            x, 'DepartureTimeStamp_ts'], flights_df_s_d_price.index)
    )
    flights_df_s_d_price_t_am = flights_df_s_d_price[flights_df_s_d_price['PricingChargeType'] == 'TotalAmount']

    min_Time = flights_df_s_d_price_t_am['TimeDiff'].min()
    max_Time = flights_df_s_d_price_t_am['TimeDiff'].max()

    df_min_Time = flights_df_s_d_price_t_am[flights_df_s_d_price_t_am['TimeDiff'] == min_Time][show_col]
    df_min_Time.set_index('FlightNumber', inplace=True)
    df_max_Time = flights_df_s_d_price_t_am[flights_df_s_d_price_t_am['TimeDiff'] == max_Time][show_col]
    df_max_Time.set_index('FlightNumber', inplace=True)

    idxs_arr = flights_df_s_d_price_t_am.sort_values(['PricingCost', 'TimeDiff'], ascending=[True, True]).index.tolist()
    if len(idxs_arr) >= 4:
        best_idxs = [idxs_arr[i] for i in range(round(len(idxs_arr) / 2) - 1, round(len(idxs_arr) / 2) + 1)]
        idxs_arr = flights_df_s_d_price_t_am.sort_values(['TimeDiff', 'PricingCost'], ascending=[True, True]).index.tolist()
        best_idxs += [idxs_arr[i] for i in range(round(len(idxs_arr) / 2) - 1, round(len(idxs_arr) / 2) + 1)]
        df_best = flights_df_s_d_price_t_am.loc[best_idxs][show_col]
    else:
        df_best = flights_df_s_d_price_t_am[show_col]
    df_best.set_index('FlightNumber', inplace=True)

    return {'min_PricingCost': df_min_PricingCost, 'max_PricingCost': df_max_PricingCost,
            'min_Time': df_min_Time, 'max_Time': df_max_Time, 'best_cost_time': df_best}
