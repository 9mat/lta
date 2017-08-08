import pandas as pd
import numpy as np
from glob import glob
import os
import sys
import json

first = 0
if len(sys.argv) > 1:
    first = int(sys.argv[1])

last = first+1
if len(sys.argv) > 2:
    last = int(sys.argv[2]) + 1

with open('settings.json', 'r') as setting_file:
    settings = json.load(setting_file)

datapath = settings[u'ezlink_path'].encode('ascii')
filenames = sorted(glob(datapath + '/ezlink*.dta'))
cash_id = "19F8D1F1B66FC67EBB7C591CB506144C38446822"

tmp = settings[u'tmp_path'].encode('ascii')

def destring(a, na=-999, type=int):
    return pd.to_numeric(a,errors='coerce').fillna(na).astype(int)


feeding_stn = [54009, 84009, 53009, 22009, 28091, 43009, 10009,
               45009, 44009, 17009, 67759, 64009, 28009, 77009,
               65009, 67009, 66009, 3239, 75009, 52009, 46008,
               46009, 55509, 59009, ]
stn = 'alighting_stop_stn'
prefix = 'feedstn_'

def gen_ride_start(a):
    a.loc[:, 'ride_start'] = pd.to_datetime(a.ride_start_date + " " + a.ride_start_time)
    a.loc[:, 'date'] = a.ride_start.dt.date
    a.loc[:, 'hour'] = a.ride_start.dt.hour.astype(np.byte)
    return a

def count_daily_riders_alight_feeding_stn(a):
    a = a.loc[a.travel_mode == "Bus"]
    a.loc[:,'alighting_stop_stn'] = destring(a[stn])
    a = a.loc[a.alighting_stop_stn.isin(feeding_stn)]
    a = gen_ride_start(a)
    a = a.loc[(a.hour >= 8) & (a.hour <= 20)]
    a['numOfRiders'] = 1
    return a.groupby([stn, 'date', 'card_type'])['numOfRiders'].sum()

def count_boarding_by_stn(a):
    a = a.loc[a.card_number != cash_id]
    a = gen_ride_start(a)
    a = a.loc[(a.hour >= 8) & (a.hour <= 20)]
    a.loc[:,'numOfRiders'] = 1
    return a.groupby(['boarding_stop_stn', 'card_type', 'date'])['numOfRiders'].sum()

def collate_ride(a):
    a.loc[:,'ride_start'] = pd.to_datetime(a.ride_start_date + " " + a.ride_start_time)
    a.loc[:,'ride_end'] = pd.to_datetime(a.ride_end_date + " " + a.ride_end_time)
    a.sort_values(['card_number', 'ride_start'], inplace=True)
    a.loc[:,'gap'] = a.ride_start - a.ride_end.shift(1)
    a.loc[:,'duration'] = a.ride_end - a.ride_start
    a.loc[:,'new_ride'] = (a.card_number != a.card_number.shift(1)) | (a.gap > np.timedelta64(30, 'm'))
    a.loc[:,'ride_num'] = a.groupby('card_number')['new_ride'].cumsum().astype(np.int16)
    a.loc[:,'trip_num'] = a.groupby(['card_number', 'ride_num']).cumcount()+1
    a.loc[:,'ride_len'] = a.groupby(['card_number', 'ride_num'])['trip_num'].transform('count')
    return a

def count_hourly_ride_by_boarding_stn(a):
    a = collate_ride(a)
    a['hour'] = a.ride_start.dt.hour.astype(np.byte)
    a['date'] = a.ride_start.dt.date
    a['numOfRiders'] = 1
    groupbyIdx = ['date', 'hour', 'card_type', 'boarding_stop_stn']
    return a.loc[a.trip_num == 1].groupby(groupbyIdx)['numOfRiders'].sum()

def count_hourly_ride_by_alighting_stn(a):
    a = collate_ride(a)
    a['hour'] = a.ride_end.dt.hour.astype(np.byte)
    a['date'] = a.ride_end.dt.date
    a['numOfRiders'] = 1
    groupbyIdx = ['date', 'hour', 'card_type', 'alighting_stop_stn']
    return a.loc[a.trip_num == a.ride_len].groupby(groupbyIdx)['numOfRiders'].sum()


def count_hourly_trip_by_card_type_and_travel_mode(a):
    a = gen_ride_start(a)
    a['numOfTrips'] = 1
    groupbyIdx = ['date', 'hour', 'card_type', 'travel_mode']
    return a.loc[a.card_number != cash_id].groupby(groupbyIdx)['numOfTrips'].sum()

def count_daily_trip_by_route(a):
    a = gen_ride_start(a)
    a['numOfTrips'] = 1
    groupbyIdx = ['date', 'bus_service_number', 'direction']
    to_keep = (a.card_number != cash_id) & (a.hour >= 8) & (a.hour <= 20)
    return a.loc[to_keep].groupby(groupbyIdx)['numOfTrips'].sum()

def count_daily_trip_by_hour(a):
    a = gen_ride_start(a)
    a['numOfTrips'] = 1
    groupbyIdx = ['date', 'hour', 'card_type', 'travel_mode']
    return a.groupby(groupbyIdx)['numOfTrips'].sum()

def count_daily_trip_by_route_6amto8pm(a):
    a = gen_ride_start(a)
    a['numOfTrips'] = 1
    groupbyIdx = ['date', 'bus_service_number', 'direction']
    to_keep = (a.card_number != cash_id) & (a.hour >= 6) & (a.hour <= 20)
    return a.loc[to_keep].groupby(groupbyIdx)['numOfTrips'].sum()

def batch_process(func, prefix):
    for filename in filenames[first:last]:
        basename = os.path.splitext(os.path.basename(filename))[0]
        outname = tmp + "/" + prefix + basename + '.csv'

        if os.path.isfile(outname):
            continue

        func(pd.read_stata(filename)).to_csv(outname, header=True)

# batch_process(count_riders_alight_feeding_stn, prefix)
# batch_process(count_boarding_by_stn, 'boarding_by_stn_')
# batch_process(count_daily_trip_by_route, 'daily_trip_by_route_')
batch_process(count_daily_trip_by_hour, 'daily_trip_by_hour_')