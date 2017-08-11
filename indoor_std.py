# coding: utf-8
#!/usr/bin/env python
#-*- coding: 'utf-8' -*-
import pandas as pd
import numpy as np
import csv
import json
import glob
import datetime
import pytz
#Program Configuration-------------------
import config
DIR_DATA=config.DIR_DATA
LIST_SOURCES=config.LIST_SOURCES
SPATIAL_THRESHOLD=config.SPATIAL_THRESHOLD
DIR_DATAANALYSIS=config.DIR_DATAANALYSIS
DIR_HEALTH_LOG=config.DIR_HEALTH_LOG
NEIGHBOR_PATH=config.NEIGHBOR_PATH

## std
current=datetime.datetime.now(pytz.timezone('Asia/Taipei'))
today=current.date()
yesterday=today-datetime.timedelta(1)
date=yesterday.strftime('%Y%m')
day=yesterday.strftime('%d')
array={}	##array formate eq. device_id : std, median
for source in LIST_SOURCES:
	src_directory=DIR_DATA+source
	for device_directory in glob.glob(src_directory+'/*'):
		csv_path=device_directory+'/'+date+'.csv'
		try:
			df=pd.read_csv(csv_path,delimiter=' ',header=None,dtype={0:object})
			df[0]=df[0].astype(str)
			df=df[df[0].str.startswith(day)]
			if df.shape[0]>=50:
				splitted=csv_path.split('/')
				temp ={str(splitted[-2]):[np.std(df[1]),np.median(df[1])]}
				array.update(temp)
		except:pass

feeds=[]
with open(NEIGHBOR_PATH,'r') as f:
	neighbor=json.load(f)
for tmp in array:
	# print tmp, 'main'
	try:
		# print len(neighbor[tmp])
		if len(neighbor[tmp])>=3:
			neighbor_std=[]
			neighbor_med=[]
			for neighbor_id in neighbor[tmp]:
				# print neighbor_id
				# print array[neighbor_id][0], array[neighbor_id][1]
				try:
					neighbor_std.append(array[neighbor_id][0]) or neighbor_med.append(array[neighbor_id][1])
				except:pass
			pm_level=config.get_pm_level(np.median(neighbor_med))
			if len(neighbor_std)>= 3 and np.median(neighbor_std) - array.get(tmp)[0] >= 1 and np.median(neighbor_med) - array.get(tmp)[1] >= SPATIAL_THRESHOLD[pm_level]:
				temp2={'device_id':tmp,'rate':1}
				feeds.append(temp2)
	except:pass

msg = {}
msg["source"] = str("device_indoor by IIS-NRL").encode("utf-8")
msg["feeds"] = feeds
utc_datetime = datetime.datetime.utcnow()
msg["version"] = utc_datetime.strftime("%Y-%m-%dT%H:%M:%SZ")
with open(DIR_DATAANALYSIS+'device_indoor_1_std.json','w') as fout:
	json.dump(msg,fout)





