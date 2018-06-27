"""Obtain data from Triathlon API"""

import configparser
import json
import pandas as pd
import requests

URL = 'https://api.triathlon.org/v1/statistics/results?'
EVENT_QUERY = 'analysis=count_unique&target_property=event.name&\
                group_by=event.name|program.id|program.name'
PROGRAM_RESULTS_QUERY = 'analysis=minimum&target_property=position&filters=program.id,eq,{0}&\
                        group_by=athlete.name|athlete.id|program.start_time|program.wetsuit|splits.swim|splits.swim_distance|splits.bike|splits.bike_distance|splits.run|splits.run_distance|splits.t1|splits.t2|temperature.air|temperature.water|format|event.id|event.country|event.venue|finish_time|event.name|program.id|program.name'


def get_api_key(filepath):
    """Get the apikey from config"""

    config = configparser.ConfigParser()
    config.read(filepath)
    return config['triathlon']['apikey']


def get_url_query(base, query, param=None):
    """Get the url and query string for request"""

    if not param:
        return base + query
    return base + query.format(param)


def get_data(query):
    """Submit the request and return result"""

    local_filename = 'temp.txt'
    params = {}
    headers = {'content-type': 'application/json',
               'Accept-Charset': 'UTF-8', 'apikey': APIKEY}
    query_result = requests.get(query,
                                headers=headers, params=params, stream=True)

    # read response into temp file as can be large
    with open(local_filename, 'wb') as temp_file:
        for chunk in query_result.iter_content(chunk_size=1024, decode_unicode=True):
            if chunk:  # filter out keep-alive new chunks
                temp_file.write(chunk)

    # load file to json object and past to convertor to create dataframe
    query_data = json.load(open(local_filename))
    return data_convertor(query_data)


def data_convertor(query_result):
    """Convert query result to pandas data frame"""

    raw_data = query_result['data']['result']
    variables = raw_data[0].keys()

    data = pd.DataFrame([[try_get_attr(i, j) for j in variables]
                         for i in raw_data], columns=variables)

    return data


def try_get_attr(item, attrib):
    """Try and get attribute or return None"""

    try:
        return item[attrib]
    except KeyError:
        return None


def get_wts_data(filepath):
    """Run queries and save data to filepath"""

    events = get_wts_event_prg_data()
    programs = events['program.id']

    index = 0
    for program in programs:
        index += 1
        print(
            'Getting data for program id: {0} ({1}/{2})'.format(program, index, len(programs)))
        # first returned dataframe for basis for others
        if index == 1:
            data = get_wts_results_data(program)
            print('Found {0} records'.format(len(data)))
        else:
            result_data = get_wts_results_data(program)
            print('Found {0} records'.format(len(result_data)))
            data = data.append(result_data, ignore_index=True)

    print('{0} total records'.format(len(data)))
    data.to_csv(filepath)
    print('Saved to {0}'.format(filepath))


def get_wts_event_prg_data():
    """Get the wts events and program data"""

    print('Getting list of events')
    return get_data(get_url_query(URL, EVENT_QUERY))


def get_wts_results_data(program_id):
    """Get the results data for a given program"""

    return get_data(get_url_query(URL, PROGRAM_RESULTS_QUERY, program_id))


if __name__ == "__main__":
    APIKEY = get_api_key('apikey.cfg')
    get_wts_data('results.csv')
