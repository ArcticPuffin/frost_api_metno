#!/usr/bin/python

"""

This program shows how to retrieve a time series of observations from the following
combination of source, element and time range:

source:     SN18700
element:    mean(wind_speed P1D)
time range: 2010-04-01 .. 2010-05-31

The time series is written to standard output as lines of the form:

  <observation time as date/time in ISO 8601 format> \
  <observation time as seconds since 1970-01-01T00:00:00> \
  <observed value>

Save the program to a file example.py, make it executable (chmod 755 example.py),
and run it e.g. like this:

  $ CLIENTID=8e6378f7-b3-ae4fe-683f-0db1eb31b24ec ./example.py

(Note: the client ID used in the example should be replaced with a real one)

The program has been tested on the following platforms:
  - Python 2.7.3 on Ubuntu 12.04 Precise
  - Python 2.7.12 and 3.5.2 on Ubuntu 16.04 Xenial

"""

import pandas as pd
import sys, os
import dateutil.parser as dp
import requests # See http://docs.python-requests.org/

def get_metno_data(station, voi, start, end, client_id):
    """
    function to collect data from metno.
    required arguments:
    station: the station ID of the station you want to retrieve data
    voi: string of variables of interest, for example 'air_temperature, wind_speed, relative_humidity'
    start: start date, for example '2010-04-01'
    end: end date, for example '2010-06-01'
    client_id: your ID to identify yourself to the database. You can get this from here: https://frost.met.no/auth/requestCredentials.html

    What is unclear:
    - How to set time resolution?
    - What are all the possible variable names?
    - Where to find available station IDs

    This function follows the metno example. https://frost.met.no/langex_python
    """


    # issue an HTTP GET request
    r = requests.get(
        'https://frost.met.no/observations/v0.jsonld',
        {'sources': station, 
         'elements': voi, #, sum(precipitation_amount PT12H)', 
         'referencetime': start+'/'+end},
        auth=(client_id, '')
    )

    time = []
    data = {}

    # extract the time series from the response
    if r.status_code == 200:
        for element in range(0, len(r.json()['data'][0]['observations'])):
            data[r.json()['data'][0]['observations'][element]['elementId']] = []
        for item in r.json()['data']:
            iso8601 = item['referenceTime']
            secsSince1970 = dp.parse(iso8601).strftime('%s')
            #sys.stdout.write('{} {} {}\n'.format(iso8601, secsSince1970, item['observations'][0]['value']))
            time.append(iso8601)
            for element in range(0, len(item['observations'])):
                data[item['observations'][element]['elementId']].append(item['observations'][element]['value'])

    else:
        sys.stdout.write('error:\n')
        sys.stdout.write('\tstatus code: {}\n'.format(r.status_code))
        if 'error' in r.json():
            assert(r.json()['error']['code'] == r.status_code)
            sys.stdout.write('\tmessage: {}\n'.format(r.json()['error']['message']))
            sys.stdout.write('\treason: {}\n'.format(r.json()['error']['reason']))
        else:
            sys.stdout.write('\tother error\n')

    df = pd.DataFrame(data, index=time)
    
    return df
