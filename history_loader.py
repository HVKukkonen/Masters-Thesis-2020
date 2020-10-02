import requests
from lxml import etree
import pandas as pd
# openpyxl needs to be installed
from owslib.wfs import WebFeatureService
import calendar
import datetime
import numpy as np
import sys

# Save file as XML from url to working directory
def download_file(url):
    response = requests.get(url)

    # Throw an error for bad status codes
    response.raise_for_status()

    with open('tempXML.xml', 'wb') as handle:
        for block in response.iter_content(1024):
            handle.write(block)


def parse_xml_fields(file):
    tree = etree.parse(file)
    root = tree.getroot()

    TPVs = [child[0] for child in root[0][0][6][0]]

    return TPVs


def load_full_month(year, mm, place):
    import calendar
    assert datetime.date(year, mm, calendar.monthrange(year, mm)[1]) < datetime.date.today(), \
        "wrong date {}/{} given, query only past months excluding current".format(year, mm)

    # create dictionary of empty lists for feature values
    values = dict([(feature, []) for feature in ['Temperature', 'Humidity', 'WindDirection', 'WindSpeedMS',
                                                 'TotalCloudCover', 'Precipitation1h']
                   ])

    wfs11 = WebFeatureService(url='https://opendata.fmi.fi/wfs', version='2.0.0')

    # query one week at a time
    for day in range(1, calendar.monthrange(year, mm)[1], 7):
        starttime = datetime.datetime(year, mm, day)
        endtime = datetime.datetime(year, mm, min(day + 6, calendar.monthrange(year, mm)[1]))
        endtime = endtime.strftime('%Y-%m-%d 23:00:00')

        # fetch data for given feature
        for feature in ['Temperature', 'Humidity', 'WindDirection', 'WindSpeedMS', 'TotalCloudCover',
                        'Precipitation1h']:

            response = wfs11.getGETGetFeatureRequest(storedQueryID='fmi::observations::weather::timevaluepair',
                                                     storedQueryParams={'parameters': feature, 'place': place,
                                                                        'timestep': 60, 'starttime': starttime,
                                                                        'endtime': endtime})
            # save response to temp XML file
            download_file(response)

            # returns TPV pairs
            try:
                TPVs = parse_xml_fields("tempXML.xml")
            except:
                print("Error occurred in parsing the XML response: ", response)
                print('Place: {}, feature: {}'.format(place, feature))
                sys.exit()

            for pair in TPVs:
                #time = pair[0].text
                value = pair[1].text

                # append value to the list of the feature
                values[feature].append(value)

    return values


def load_history(start, end, places):

    assert isinstance(start, tuple), "give start time as tuple of form (year, month)"
    year, month = start
    end_y, end_m = end
    # difference between dates as type timedelta
    tdelta = datetime.date(end_y, end_m, calendar.monthrange(end_y, end_m)[1]) - datetime.date(year, month, 1)
    # interval in months
    n_months = int(round(tdelta.days / 30.5))

    # dict of empty lists for features
    values = dict([(feature, []) for feature in ['Temperature', 'Humidity', 'WindDirection', 'WindSpeedMS',
                                                 'TotalCloudCover', 'Precipitation1h']
                   ])

    for place in places:

        # fetch data for given interval
        for m in range(n_months):

            feature_values = load_full_month(year + int((month + m - 1) / 12), 1 + (month + m - 1) % 12, place)

            for feature in values.keys():
                values[feature] += feature_values[feature]

    # create dframe from feature values and add time and place
    all_values = pd.DataFrame(values, dtype='float')
    time_len = int((tdelta.days+1)*24)
    all_values['Time'] = [datetime.datetime(year, month, 1, 0, 0, 0) + datetime.timedelta(hours=h)
                          for h in range(0, time_len)] * len(places)
    all_values['Place'] = np.array(places).repeat(time_len)
    # rearrange columns
    all_values = all_values[['Time', 'Place', 'Temperature', 'TotalCloudCover', 'WindDirection', 'WindSpeedMS',
                             'Humidity', 'Precipitation1h']]

    return all_values


# quick test
#print(load_history((2020, 1), (2020, 1), ['Turku', 'Helsinki']).dtypes)
