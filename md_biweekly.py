#!/usr/bin/env python
#-*- coding: 'utf-8' -*-
import csv
import glob
import os
import datetime
import pytz
import json
import math
import pandas as pd

#Program Configuration-------------------
import config
DIR_DATA=config.DIR_DATA
LIST_SOURCES=config.LIST_SOURCES
SPATIAL_THRESHOLD=config.SPATIAL_THRESHOLD
DIR_DATAANALYSIS=config.DIR_DATAANALYSIS
#########################################
feeds=[]
ind_feeds=[]
emi_feeds=[]
last_week={}
current=datetime.datetime.now(pytz.timezone('Asia/Taipei'))
today=current.date()
for i in xrange(14):
	date=today-datetime.timedelta(days=i+1)
	# last_week.append(date)
	ym=date.strftime('%Y%m')
	D=date.day
	if ym not in last_week:
		last_week[ym]=[]
	last_week[ym].append(D)
# print last_week
for source in LIST_SOURCES:
	src_directory=DIR_DATA+source
	for device_directory in glob.glob(src_directory+'/*'):
		total=0.0
		indoor=0.0
		emission=0.0
		nocomment=0.0
		for file in last_week:
			csv_path=device_directory+'/'+file+'.csv'
			if os.path.exists(csv_path):
				df=pd.read_csv(csv_path,delimiter=' ',header=None)
				# df[0]=df[0].astype(str)
				# for day in last_week[file]:
					# print day
					# df=df[df[0].str.startswith(day)]
				df=df[df[1]>=0]
				df=df[df[0]>=(min(last_week[file])*10000)]
				df=df[df[0]<=(max(last_week[file])+1)*10000]
				if df.shape[0]!=0:
					df[4]=df[2].apply(config.get_pm_level)
					df[4]=df[4].replace(SPATIAL_THRESHOLD)
					indoor_df=df[df[2]-df[1]>df[4]]
					indoor_df=indoor_df[indoor_df[3]>=2]
					emission_df=df[df[1]-df[2]>df[4]]
					emission_df=emission_df[emission_df[3]>=2]
					emission_df=emission_df[emission_df[1]>30.0]
					nocomment_df=df[df[3]<2]
					total=total+float(df.shape[0])
					indoor=indoor+float(indoor_df.shape[0])
					emission=emission+float(emission_df.shape[0])
					nocomment=nocomment+float(nocomment_df.shape[0])
		if total!=0:
			splitted=csv_path.split('/')
			indoor=indoor/total
			emission=emission/total
			nocomment=nocomment/total
			temp = {'device_id':splitted[-2],'1':nocomment,'2':emission,'3':indoor}
			feeds.append(temp)
			compare=[indoor,emission,nocomment]
			if compare.index(max(compare))==0 and indoor>=1.0/3.0:
				temp = {'device_id':splitted[-2],'rate':indoor}
				ind_feeds.append(temp)
			if compare.index(max(compare))==1 and emission>=1.0/3.0:
				temp = {'device_id':splitted[-2],'rate':emission}
				emi_feeds.append(temp)

msg = {}
msg["source"] = str("device_malfunction_biweekly by IIS-NRL").encode("utf-8")
msg["feeds"] = feeds
msg["description-type"]={"type-1":"non-detectable","type-2":"spatially greater","type-3":"spatially less"}
utc_datetime = datetime.datetime.utcnow()
msg["version"] = utc_datetime.strftime("%Y-%m-%dT%H:%M:%SZ")
with open(DIR_DATAANALYSIS+'device_malfunction_biweekly.json','w') as fout:
	json.dump(msg,fout)

msg = {}
msg["source"] = str("device_indoor by IIS-NRL").encode("utf-8")
msg["feeds"] = ind_feeds
utc_datetime = datetime.datetime.utcnow()
msg["version"] = utc_datetime.strftime("%Y-%m-%dT%H:%M:%SZ")
with open(DIR_DATAANALYSIS+'device_indoor_14.json','w') as fout:
	json.dump(msg,fout)

msg = {}
msg["source"] = str("device_emission by IIS-NRL").encode("utf-8")
msg["feeds"] = emi_feeds
utc_datetime = datetime.datetime.utcnow()
msg["version"] = utc_datetime.strftime("%Y-%m-%dT%H:%M:%SZ")
with open(DIR_DATAANALYSIS+'device_emission_14.json','w') as fout:
	json.dump(msg,fout)

