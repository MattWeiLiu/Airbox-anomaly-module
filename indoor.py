#!/usr/bin/env python
#-*- coding: 'utf-8' -*-
import json
import config
import datetime

threshold=(8.0/24.0)*0.2+(40.0/168.0)*0.3+(80.0/336.0)*0.5
DIR_DATAANALYSIS=config.DIR_DATAANALYSIS
INDOOR_1_PATH=DIR_DATAANALYSIS+'device_indoor_1.json'
INDOOR_7_PATH=DIR_DATAANALYSIS+'device_indoor_7.json'
INDOOR_14_PATH=DIR_DATAANALYSIS+'device_indoor_14.json'

d1={}
d7={}
d14={}
feeds=[]
key=[]
with open(INDOOR_1_PATH,'r') as f:
	indoor1=json.load(f)
for device in indoor1['feeds']:
	d1[device['device_id']]=device['rate']
	key.append(device['device_id'])

with open(INDOOR_7_PATH,'r') as f:
	indoor7=json.load(f)
for device in indoor7['feeds']:
	d7[device['device_id']]=device['rate']
	key.append(device['device_id'])

with open(INDOOR_14_PATH,'r') as f:
	indoor14=json.load(f)
for device in indoor14['feeds']:
	d14[device['device_id']]=device['rate']
	key.append(device['device_id'])

key=set(key)
a=0.2
b=0.3
for i in key:
	if i not in d1:
		d1[i]=0
	if i not in d7:
		d7[i]=0
	if i not in d14:
		d14[i]=0
	D=a*d1[i]+b*d7[i]+(1-a-b)*d14[i]
	if D>threshold:
		temp={'device_id':i,'rate':D}
		feeds.append(temp)

msg = {}
msg["source"] = str("device_indoor by IIS-NRL").encode("utf-8")
msg["feeds"] = feeds
utc_datetime = datetime.datetime.utcnow()
msg["version"] = utc_datetime.strftime("%Y-%m-%dT%H:%M:%SZ")
with open(DIR_DATAANALYSIS+'device_indoor.json','w') as fout:
	json.dump(msg,fout)

