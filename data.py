import pandas as pd
import numpy as np
import requests
import re
from bs4 import BeautifulSoup
import datetime
import os

requests.packages.urllib3.disable_warnings(requests.packages.urllib3.exceptions.InsecureRequestWarning)

# NOAA Weather Data (https://www.cpc.ncep.noaa.gov/products/analysis_monitoring/cdus/degree_days/) & (https://www.cpc.ncep.noaa.gov/products/analysis_monitoring/cdus/pastdata/degree_days/)
# Current: (https://www.cpc.ncep.noaa.gov/products/analysis_monitoring/cdus/degree_days/wsacddy.txt) & (https://www.cpc.ncep.noaa.gov/products/analysis_monitoring/cdus/degree_days/wsahddy.txt)
# 7 Day Outlooks: https://www.cpc.ncep.noaa.gov/products/analysis_monitoring/cdus/degree_days/DDF_index.shtml

def get_folder(url,start_point):

    url_request = requests.get(url,verify=False).text
    html = BeautifulSoup(url_request,'html.parser')
    tbl = html.find("table").find_all("tr")

    urls = []

    count = 0
    for row in tbl:
        count = count + 1
        row = row.find_all("td")
        if row:
            href_tag = row[0].find_all('a', href=True)[0].get('href')
            if count >= start_point:
                item_url = url + href_tag
                urls.append(item_url)

    return urls

def read_txt_files(txt_file_urls,previous,fcst=False):

    txt_data = []

    for folder in txt_file_urls:
        for txt in folder:
            if str(txt) in previous['SOURCE'].to_list():
                continue
            
            response = requests.get(txt,verify=False,stream=True)
            response = response.text
            lines = response.splitlines()

            lines_for_df = []
            start_switch = 'Off'
            line_length = 0

            for line in lines:
                
                line = re.sub("\s+",",",line.strip())
                line = line.split(",")

                words = []
                nums = []

                for val in line:

                    try:
                        check_sum = int(val)
                        nums.append(val)
                    except:
                        words.append(val)
                
                words = [" ".join(str(item) for item in words)]
                line = words + nums

                if fcst == True:
                    date_line = "LAST DATE OF FORECAST WEEK IS"
                else:
                    date_line = 'LAST DATE OF DATA COLLECTION'

                if any(date_line in sub for sub in line) == True:

                    month_num = {
                        'JAN':1,
                        'FEB':2,
                        'MAR':3,
                        "APR":4,
                        'MAY':5,
                        'JUN':6,
                        'JUL':7,
                        'AUG':8,
                        'SEP':9,
                        'OCT':10,
                        'NOV':11,
                        'DEC':12
                    }

                    year_ = line[-1]
                    day_ = line[-2]
                    month_ = month_num[re.sub("\s+",",",line[0].strip())[-3:]]

                if 'ALABAMA' in line:
                    start_switch = 'On'
                    line_length = line_length + len(line)

                if start_switch == 'On':
                    if line_length == len(line):
                        lines_for_df.append(line)
                        if 'UNITED STATES' in line:
                            start_switch = 'Off'

            noaa_historical_HDD_CDD = pd.DataFrame(lines_for_df, columns=[
                'AREA',
                'WEEK TOTAL',
                'WEEK DEV FROM NORM',
                'WEEK DEV FROM L YR',
                'CUM TOTAL',
                'CUM DEV FROM NORM',
                'CUM DEV FROM L YR',
                'CUM DEV FROM NORM PRCT',
                'CUM DEV FROM L YR PRCT'
            ])

            noaa_historical_HDD_CDD.insert(
                loc=0,
                column='SOURCE',
                value=txt
                )
            
            noaa_historical_HDD_CDD.insert(
                loc=1,
                column='DATE',
                value=datetime.datetime(int(year_),int(month_), int(day_))
            )

            noaa_historical_HDD_CDD = noaa_historical_HDD_CDD.replace('TENNESSE','TENNESSEE')

            txt_data.append(noaa_historical_HDD_CDD)

    if txt_data:
        return pd.concat(txt_data)
    
def scrape_weather_data():

    os.makedirs('data', exist_ok=True)
    filename = 'data\weekly_hdd_cdd_state_archive.csv'
    columns = [
        'SOURCE', 'DATE', 'HDD/CDD', 'AREA', 'WEEK TOTAL', 'WEEK DEV FROM NORM',
        'WEEK DEV FROM L YR', 'CUM TOTAL', 'CUM DEV FROM NORM', 'CUM DEV FROM L YR',
        'CUM DEV FROM NORM PRCT', 'CUM DEV FROM L YR PRCT'
        ]
    if not os.path.exists(filename):
        df = pd.DataFrame(columns=columns)
        df.to_csv(filename, index=False)

    weekly_state_previous = pd.read_csv(filename)

    cdd_base_url = 'https://ftp.cpc.ncep.noaa.gov/htdocs/products/analysis_monitoring/cdus/degree_days/archives/Cooling%20Degree%20Days/weekly%20cooling%20degree%20days%20state/'
    cdd_yr_folders = get_folder(cdd_base_url,5)
    cdd_txt_files = [get_folder(yr_urls,5) for yr_urls in cdd_yr_folders]
    weekly_cdd_state = read_txt_files(cdd_txt_files,previous = weekly_state_previous)
    weekly_cdd_state.insert(loc=2,column='HDD/CDD',value='CDD')
    weekly_cdd_state['DATE'] = pd.to_datetime(weekly_cdd_state['DATE']).dt.date

    hdd_base_url = 'https://ftp.cpc.ncep.noaa.gov/htdocs/products/analysis_monitoring/cdus/degree_days/archives/Heating%20degree%20Days/weekly%20states/'
    hdd_yr_folders = get_folder(hdd_base_url,5)
    hdd_txt_files = [get_folder(yr_urls,5) for yr_urls in hdd_yr_folders]
    weekly_hdd_state = read_txt_files(hdd_txt_files,previous = weekly_state_previous)
    weekly_hdd_state.insert(loc=2,column='HDD/CDD',value='HDD')
    weekly_hdd_state['DATE'] = pd.to_datetime(weekly_hdd_state['DATE']).dt.date

    weekly_state = pd.concat([weekly_state_previous, weekly_cdd_state, weekly_hdd_state])
    weekly_state.to_csv(filename,index=False)

    filename_current = 'data\weekly_hdd_cdd_state_current.csv'
    columns = [
        'SOURCE', 'DATE', 'HDD/CDD', 'AREA', 'WEEK TOTAL', 'WEEK DEV FROM NORM',
        'WEEK DEV FROM L YR', 'CUM TOTAL', 'CUM DEV FROM NORM', 'CUM DEV FROM L YR',
        'CUM DEV FROM NORM PRCT', 'CUM DEV FROM L YR PRCT'
        ]
    if not os.path.exists(filename_current):
        df = pd.DataFrame(columns=columns)
        df.to_csv(filename_current, index=False)

    cdd_txt_files_current = read_txt_files([['https://www.cpc.ncep.noaa.gov/products/analysis_monitoring/cdus/degree_days/wsacddy.txt']],previous = weekly_state_previous)
    cdd_txt_files_current.insert(loc=2,column='HDD/CDD',value='CDD')
    hdd_txt_files_current = read_txt_files([['https://www.cpc.ncep.noaa.gov/products/analysis_monitoring/cdus/degree_days/wsahddy.txt']],previous = weekly_state_previous)
    hdd_txt_files_current.insert(loc=2,column='HDD/CDD',value='HDD')
    weekly_state_current = pd.concat([cdd_txt_files_current, hdd_txt_files_current])
    weekly_state_current['DATE'] = pd.to_datetime(weekly_state_current['DATE'])
    weekly_state_current.to_csv(filename_current,index=False)

    filename_forecast = 'data\weekly_hdd_cdd_state_forecast.csv'
    columns = [
        'SOURCE', 'DATE', 'HDD/CDD', 'AREA', 'WEEK TOTAL', 'WEEK DEV FROM NORM',
        'WEEK DEV FROM L YR', 'CUM TOTAL', 'CUM DEV FROM NORM', 'CUM DEV FROM L YR',
        'CUM DEV FROM NORM PRCT', 'CUM DEV FROM L YR PRCT'
        ]
    if not os.path.exists(filename_forecast):
        df = pd.DataFrame(columns=columns)
        df.to_csv(filename_forecast, index=False)

    cdd_txt_files_forecast = read_txt_files([['https://www.cpc.ncep.noaa.gov/products/analysis_monitoring/cdus/degree_days/cfstwpws.txt']],previous = weekly_state_previous, fcst=True)
    cdd_txt_files_forecast.insert(loc=2,column='HDD/CDD',value='CDD')
    hdd_txt_files_forecast = read_txt_files([['https://www.cpc.ncep.noaa.gov/products/analysis_monitoring/cdus/degree_days/hfstwpws.txt']],previous = weekly_state_previous, fcst=True)
    hdd_txt_files_forecast.insert(loc=2,column='HDD/CDD',value='HDD')
    weekly_state_forecast = pd.concat([cdd_txt_files_forecast, hdd_txt_files_forecast])
    weekly_state_forecast['DATE'] = pd.to_datetime(weekly_state_forecast['DATE'])
    weekly_state_forecast.to_csv(filename_forecast,index=False)

if __name__ == '__main__':
    scrape_weather_data()