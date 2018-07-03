"""Obtain data from Triathlon API"""

import configparser
import json
import pandas as pd
import requests

BASE_URL = 'https://api.triathlon.org/v1/'
EVENT_QUERY = 'statistics/results?analysis=count_unique&target_property=event.name&\
                group_by=event.name|program.id|program.name'
PROGRAM_RESULTS_QUERY = 'statistics/results?analysis=minimum&target_property=position&\
                        filters=program.id,eq,{0}&\
                        group_by=athlete.name|athlete.id|program.start_time|program.wetsuit|splits.swim|splits.swim_distance|splits.bike|splits.bike_distance|splits.run|splits.run_distance|splits.t1|splits.t2|temperature.air|temperature.water|format|event.id|event.country|event.venue|finish_time|event.name|program.id|program.name'
ATHLETE_QUERY = 'athletes/{0}?output=basic'


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


def get_data(query, data_convertor):
    """Submit the request and return result"""

    local_filename = 'temp.txt'
    params = {}
    headers = {'content-type': 'application/json',
               'Accept-Charset': 'UTF-8', 'apikey': APIKEY}
    query_result = requests.get(query,
                                headers=headers, params=params, stream=True)
    if query_result.status_code == 200:
        # read response into temp file as can be large
        with open(local_filename, 'wb') as temp_file:
            for chunk in query_result.iter_content(chunk_size=1024):
                if chunk:  # filter out keep-alive new chunks
                    temp_file.write(chunk)

        # load file to json object and past to convertor to create dataframe
        query_data = json.load(open(local_filename))
        return data_convertor(query_data)
    return None


def stats_data_convertor(query_result):
    """Convert query result to pandas data frame for statistics api calls"""

    raw_data = query_result['data']['result']
    variables = raw_data[0].keys()

    data = pd.DataFrame([[try_get_attr(i, j) for j in variables]
                         for i in raw_data], columns=variables)

    return data


def athlete_data_convertor(query_result):
    """Convert query result to pandas data frame for athlete api calls"""

    raw_data = query_result['data']
    data = pd.DataFrame.from_dict(raw_data)

    return data


def try_get_attr(item, attrib):
    """Try and get attribute or return None"""

    try:
        return item[attrib]
    except KeyError:
        return None


def try_add_data(result_data, parent_data=None):
    """Handle adding data and setting base dataframe"""

    if result_data is not None and not result_data.empty:

        if parent_data is None or parent_data.empty:
            parent_data = result_data
        else:
            parent_data = parent_data.append(result_data, ignore_index=True)

        result_data_size = len(result_data)
        if result_data_size > 1:
            print('Found {0} records'.format(result_data_size))
        else:
            print('Found record')
    else:
        print('No data to add')

    return parent_data


def get_dataframe(items, get_method, get_message):
    """Create a data frame for a set of items from a get method call
    update progress via a get message"""

    index = 0
    data = None
    for item in items:
        index += 1
        print(get_message.format(item, index, len(items)))
        data = try_add_data(get_method(item), data)
    return data


def get_wts_data(results_path, athletes_path):
    """Run queries and save data to filepath"""

    events = get_wts_event_prg_data()
    programs = events['program.id']

    # Results data
    data = get_dataframe(programs, get_wts_results_data,
                         'Getting data for program id: {0} ({1}/{2})')

    print('{0} total records'.format(len(data)))

    # Athlete data
    athletes = data['athlete.id'].unique()

    print('Getting data for {0} athletes'.format(len(athletes)))
    athlete_data = get_dataframe(athletes, get_athlete_data,
                                 'Getting data for athlete id: {0} ({1}/{2})')

    print('{0} total athlete records'.format(len(athlete_data)))

    # athletes can have multiple categories drop this and remove duplicated entries
    athlete_data.drop(columns=['athlete_categories'], inplace=True)
    athlete_data.drop_duplicates(inplace=True)

    print('{0} total athlete records'.format(len(athlete_data)))

    athlete_data.to_csv(athletes_path)
    print('Saved to athletes data to {0}'.format(athletes_path))

    # merge athletes in results data
    data = pd.merge(data, athlete_data, left_on='athlete.id',
                    right_on='athlete_id')

    data.to_csv(results_path)
    print('Saved to results data to {0}'.format(results_path))


def get_wts_event_prg_data():
    """Get the wts events and program data"""

    print('Getting list of events')
    return get_data(get_url_query(BASE_URL, EVENT_QUERY), stats_data_convertor)


def get_wts_results_data(program_id):
    """Get the results data for a given program"""

    return get_data(get_url_query(BASE_URL, PROGRAM_RESULTS_QUERY, program_id),
                    stats_data_convertor)


def get_athlete_data(athlete_id):
    """Get information for athletes"""

    return get_data(get_url_query(BASE_URL, ATHLETE_QUERY, athlete_id), athlete_data_convertor)


if __name__ == "__main__":
    APIKEY = get_api_key('apikey.cfg')
    get_wts_data('results.csv', 'athletes.csv')
