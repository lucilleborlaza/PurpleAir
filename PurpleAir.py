# -*- coding: utf-8 -*-
"""
Created on Thu Sep  7 10:11:32 2023
Updated script to extract data from PurpleAir.
@author: lb945465
"""

import requests
import pandas as pd
from datetime import datetime
import time
import json
from io import StringIO

# API Key provided by PurpleAir(c)
key_read = 'insert your key here'

# Sleep Seconds
sleep_seconds = 3  # wait sleep_seconds after each query

#Set the folder where you want your data stored
folderpath = r'C:\Users\LB945465\OneDrive - University at Albany - SUNY\State University of New York\Spyder'

def get_sensorslist(nwlng, nwlat, selng, selat, location, key_read):
    # PurpleAir API URL
    root_url = 'https://api.purpleair.com/v1/sensors/'

    # Box domain: lat_lon = [nwlng, nwlat, selng, selat]
    lat_lon = [nwlng, nwlat, selng, selat]
    for i, l in enumerate(lat_lon):
        if i == 0:
            ll_api_url = f'&nwlng={l}'
        elif i == 1:
            ll_api_url += f'&nwlat={l}'
        elif i == 2:
            ll_api_url += f'&selng={l}'
        elif i == 3:
            ll_api_url += f'&selat={l}'

    # Fields to get
    fields_list = ['sensor_index', 'name', 'latitude', 'longitude', 'location_type']
    fields_api_url = '&fields=' + '%2C'.join(fields_list)

    # Indoor, outdoor, or all
    loc_api = f'&location_type={0 if location == "outdoor" else 1}'

    # Final API URL
    api_url = root_url + f'?api_key={key_read}' + fields_api_url + ll_api_url + loc_api

    # Getting data
    response = requests.get(api_url)

    if response.status_code == 200:
        json_data = json.loads(response.content)['data']
        df = pd.DataFrame.from_records(json_data)
        df.columns = fields_list
    else:
        raise requests.exceptions.RequestException

    # Writing to csv file
    filename = folderpath + '\sensors_list.csv'
    df.to_csv(filename, index=False, header=True)

    # Creating a Sensors
    sensorslist = list(df.sensor_index)

    return sensorslist

def get_historicaldata(sensors_list, bdate, edate, average_time, key_read):
    # PurpleAir API URL
    root_api_url = 'https://api.purpleair.com/v1/sensors/'

    # Average time: The desired average in minutes, one of the following: 0 (real-time), 10 (default if not specified), 30, 60
    average_api = f'&average={average_time}'

    # Creating fields api url from fields list to download the data: Note: Sensor ID/Index will not be downloaded as default
    fields_list = ['pm2.5_atm_a', 'pm2.5_atm_b', 'pm2.5_cf_1_a', 'pm2.5_cf_1_b', 'humidity_a', 'humidity_b',
                   'temperature_a', 'temperature_b', 'pressure_a', 'pressure_b']
    fields_api_url = '&fields=' + '%2C'.join(fields_list)

    # Dates of Historical Data period
    begindate = datetime.fromisoformat(bdate)
    enddate = datetime.fromisoformat(edate)

    # Downlaod days based on average
    freq = '14d' if average_time == 60 else '2d'
    datelist = pd.date_range(begindate, enddate, freq=freq)
    datelist = datelist.tolist()
    datelist.reverse()

    # Converting to PA required format
    date_list = [dt.strftime('%Y-%m-%d') + 'T' + dt.strftime('%H:%M:%S') + 'Z' for dt in datelist]

    # to get data from end date to start date
    len_datelist = len(date_list) - 1

    # Getting 2-data for one sensor at a time
    for s in sensors_list:
        # Adding sensor_index & API Key
        hist_api_url = root_api_url + f'{s}/history/csv?api_key={key_read}'

        # Creating start and end date api url
        for i, d in enumerate(date_list):
            # Wait time
            time.sleep(sleep_seconds)

            if i < len_datelist:
                print(f'Downloading for PA: {s} for Dates: {date_list[i + 1]} and {d}.')
                dates_api_url = f'&start_timestamp={date_list[i + 1]}&end_timestamp={d}'
                api_url = hist_api_url + dates_api_url + average_api + fields_api_url

                try:
                    response = requests.get(api_url)
                    response.raise_for_status()  # Check for HTTP errors
                except requests.exceptions.RequestException as e:
                    print(f"RequestException: {e}")
                    continue  # Skip this sensor and try the next one

                # Creating a Pandas DataFrame
                df = pd.read_csv(StringIO(response.text), sep=",", header=0)

                if df.empty:
                    print('------------- No Data Available -------------')
                else:
                    # Dropping duplicate rows
                    df = df.drop_duplicates(subset=None, keep='first', inplace=False)

                    # Writing to CSV file
                    filename = folderpath + f'\sensorsID_{s}_{date_list[i + 1]}_{d}.csv'
                    df.to_csv(filename, index=False, header=True)

# Data download period
bdate = '2022-06-01T00:00:00+00:00'
edate = '2022-06-15T00:00:00+00:00'

# Specify the coordinates for New York City
nwlng = -74.2591  # North-West Longitude
nwlat = 40.9176   # North-West Latitude
selng = -73.7004  # South-East Longitude
selat = 40.4774   # South-East Latitude

# Set the location parameter to 'outdoor' or 'both', depending on your preference
location = 'outdoor'  # 'indoor' or 'both'

# Get the list of PurpleAir sensors in New York City
sensors_list = get_sensorslist(nwlng, nwlat, selng, selat, location, key_read)

# Average_time. The desired average in minutes, one of the following: 0 (real-time), 
# 10 (default if not specified), 30, 60, 360 (6 hours), 1440 (1 day)
average_time = 10  # or 10  or 0 (Current script is set only for real-time, 10, or 60 minutes data)

# Getting PA data
get_historicaldata(sensors_list, bdate, edate, average_time, key_read)
