#!/usr/bin/env python
#-*- coding: 'utf-8' -*-
import json
import config
import datetime

#threshold=(8.0/24.0)*0.2+(40.0/168.0)*0.3+(80.0/336.0)*0.5
threshold = (80.0/336.0)
DIR_DATAANALYSIS=config.DIR_DATAANALYSIS
INDOOR_14_PATH=DIR_DATAANALYSIS+'device_indoor_14.json'
INDOOR_1_std=DIR_DATAANALYSIS+'device_indoor_1_std.json'

d14={}
d1std = {}
feeds=[]
# key=[]
a=[]
b=[]


with open(INDOOR_14_PATH,'r') as f:
	indoor14=json.load(f)
for device in indoor14['feeds']:
	d14[device['device_id']]=device['rate']
	b.append(device['device_id'])

with open(INDOOR_1_std,'r') as f:
	indoor1std=json.load(f)
for device in indoor1std['feeds']:
	d1std[device['device_id']]=device['rate']
	a.append(device['device_id'])


feeds=set(b).difference(set(a))

# for i in key:
# 	if i not in d1std:
# 		d1std[i]=0
# 	if i not in d14:
# 		d14[i]=0
# 	if d14[i]>threshold or d1std[i] >= 1 :
# 		temp={'device_id':i}
# 		feeds.append(temp)
# 		print i


msg = {}
msg["source"] = str("device_indoor by IIS-NRL").encode("utf-8")
msg["feeds"] = feeds
utc_datetime = datetime.datetime.utcnow()
msg["version"] = utc_datetime.strftime("%Y-%m-%dT%H:%M:%SZ")
with open(DIR_DATAANALYSIS+'device_indoor_v2.json','w') as fout:
	json.dump(msg,fout)

