#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Gather field stats from CLIWOC c99
"""
import os
import sys
sys.path.append('/home/users/brecinos/c3s_work')
import mdf_reader
import pandas as pd
import numpy as np
import mdf_reader
import json
import pickle
from collections import defaultdict

funPath = os.path.dirname(os.path.abspath(__file__))
#data_path = os.path.join(funPath,'data/133-730/')
#print(data_path)

data_jasmin = '/gws/nopw/j04/glamod_marine/data/datasets/ICOADS_R3.0.0T/level0/133-730'
print(data_jasmin)

years = np.arange(1661,1895)
print(years)

output_path = '/home/users/brecinos/c3s_work/133-730/'

#i = 1
i = int(sys.argv[1])

year = years[i]
print(year)

months = ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12']

# TODO: for running in jasmin we must change several things.. the year and data_path
paths_files = []
for m in months:
    path = os.path.join(data_jasmin, str(year)+'-'+m+'.imma')
    #print(path)
    if os.path.exists(path):
        paths_files.append(path)

print(paths_files)

schema_lib = os.path.join(os.path.dirname(funPath),'data_models','lib')

print(schema_lib)

schema_name = 'imma1_d730'

model_path = os.path.join(schema_lib, schema_name)
print(model_path)

d = defaultdict(list)
ship_types = []
lat_inds = []
lon_inds = []
at_units = []
sst_units = []
ap_units = []
bart_units = []
lon_units = []
baro_types = []
releases = []


for path in paths_files:
    data = mdf_reader.read(path, data_model_path= model_path)
    names = os.path.split(path)[1][0:7]

    # Getting elements from voyage section
    rig = data.data[["c99_voyage"]].c99_voyage.Ship_type.value_counts(dropna=False).to_frame()
    lat_ind = data.data[["c99_voyage"]].c99_voyage.LatInd.value_counts(dropna=False).to_frame()
    lon_ind = data.data[["c99_voyage"]].c99_voyage.LonInd.value_counts(dropna=False).to_frame()

    # Getting elements from data section
    at_unit = data.data[["c99_data"]].c99_data.AT_reading_units.value_counts(dropna=False).to_frame()
    sst_unit = data.data[["c99_data"]].c99_data.SST_reading_units.value_counts(dropna=False).to_frame()
    ap_unit = data.data[["c99_data"]].c99_data.AP_reading_units.value_counts(dropna=False).to_frame()
    bart_unit = data.data[["c99_data"]].c99_data.BART_reading_units.value_counts(dropna=False).to_frame()
    lon_unit = data.data[["c99_data"]].c99_data.Longitude_units.value_counts(dropna=False).to_frame()
    baro_type = data.data[["c99_data"]].c99_data.BARO_type.value_counts(dropna=False).to_frame()
    release = data.data[["c99_data"]].c99_data.Release.value_counts(dropna=False).to_frame()

    ship_types.append(rig)
    lat_inds.append(lat_ind)
    lon_inds.append(lon_ind)
    at_units.append(at_unit)
    sst_units.append(sst_unit)
    ap_units.append(ap_unit)
    bart_units.append(bart_unit)
    lon_units.append(lon_unit)
    baro_types.append(baro_type)
    releases.append(release)


d['ship_types'] = pd.concat(ship_types, axis=1).sum(axis=1)
d['lan_inds'] = pd.concat(lat_inds, axis=1).sum(axis=1)
d['lon_inds'] = pd.concat(lon_inds, axis=1).sum(axis=1)
d['at_units'] = pd.concat(at_units, axis=1).sum(axis=1)
d['sst_units'] = pd.concat(sst_units, axis=1).sum(axis=1)
d['ap_units'] = pd.concat(ap_units, axis=1).sum(axis=1)
d['bart_units'] = pd.concat(bart_units, axis=1).sum(axis=1)
d['lon_units'] = pd.concat(lon_units, axis=1).sum(axis=1)
d['baro_types'] = pd.concat(baro_types, axis=1).sum(axis=1)
d['releases'] = pd.concat(releases, axis=1).sum(axis=1)

print(d)
fp = os.path.join(output_path, str(year) + '.pkl')
print(fp)

with open(fp, 'wb') as f:
    pickle.dump(d, f, protocol=-1)

print('Done!')

