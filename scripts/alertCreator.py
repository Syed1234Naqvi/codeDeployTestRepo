#!/usr/bin/python

import pymongo
from pymongo import MongoClient
from pyspark.sql import SparkSession
import socket
import subprocess
import os 

filename = 'alertsRecords.txt'
if not os.path.exists(filename):
	print "creating records file"
	file(filename,'w').close()
#threading.Thread(target=actionImpl.start).start()
databaseSensors = []
postNewSensorsMessage = []
newSensorDict ={}
#completeValuesPerNode = []
sensorsAlreadySeen = set(databaseSensors)

# Spark application name
application_name = 'AlertDetector&Sender'
#Start the spark session and initial application
spark = SparkSession\
    .builder\
    .appName(application_name)\
    .getOrCreate()

client = pymongo.MongoClient("mongodb://admin:admin@cluster0-shard-00-00-xowtx.mongodb.net:27017,cluster0-shard-00-01-xowtx.mongodb.net:27017,cluster0-shard-00-02-xowtx.mongodb.net:27017/admin?replicaSet=Cluster0-shard-0&ssl=true")
db = client.System
sensors = db.Sensors
thresholds = db.Thresholds
sensors_thresholds = thresholds.find()
listOfSensorsWithThresholds = list(sensors_thresholds[0].keys())

def getNumUni(s):
	if isinstance(s,int):
		#print s + " : mn num is int"
		return s
	if isinstance(s,float):
		#print s + " : mn num is float"
		return s
	try:
		float(s.decode("utf-8","ignore"))
		#print s + " : mn num converted to float"
		return float(s.decode("utf-8","ignore"))
	except:
		try: 
			int(s.decode("utf-8","ignore"))
			#print s + " : mn num converted to int"
			return int(s.decode("utf-8","ignore"))
		except:
			#print s+" : mn num is unconvertable"
			return -1

while (1):
	for node in sensors.find({"values":{'$exists':True}}):
		if not node["values"]: 
			break;
		for key in node["values"][-1]:
			print ("key viewed: " + key.decode("utf-8","ignore"))
			#print ("2) pass first check? " + str(key != "Node" and key !="Epoch" and key !="Timestamp"))
			if(key != "Node" and key !="Epoch" and key !="Timestamp"):
				#print ("3) threshold not in dB for " +key.decode("utf-8","ignore") + " ? "  + str(key not in listOfSensorsWithThresholds))
				if(key not in listOfSensorsWithThresholds):
					#print ("X) key not in db : " + key.decode("utf-8","ignore"))
					thresholds.update_one({"docName":"threshold"},{'$set':{key:[{"value":"UNSET","check":"UNSET"}] }},upsert=False)
					listOfSensorsWithThresholds = list(sensors_thresholds[0].keys())
			
				#print ("4) sensor in database :" + key.decode("utf-8","ignore"))
				#for sensorThreshold in sensors_thresholds:
				#print ("5) currently checking danger for " + key.decode("utf-8","ignore"))
				#print ("6) being checked : " + key.decode("utf-8","ignore"))
				#print ", threshold: " + str(sensors_thresholds[0][key][0]["value"])
				#print ", value: " + str(node["values"][-1][key])
				if sensors_thresholds[0][key][0]["check"] and sensors_thresholds[0][key][0]["value"]:
					if sensors_thresholds[0][key][0]["check"] != "UNSET" and sensors_thresholds[0][key][0]["value"]!="UNSET":	
						#print ("8) able to compare : " + key.decode("utf-8","ignore") + ", threshold: " + str(sensors_thresholds[0][key][0]["value"]) + ", value: " + str(node["values"][-1][key]))
						if(getNumUni(node["values"][-1][key]) == -1):
							print "ERROR: invalid number from MN,skipping value "
							print node["values"][-1][key]
							print getNumUni(node["values"][-1][key])
							break
						else:
							mnNumber = getNumUni(node["values"][-1][key])
							#print type(mnNumber)
							#print type(sensors_thresholds[0][key][0]["value"])
						if sensors_thresholds[0][key][0]["check"] == "greater":
					 		if sensors_thresholds[0][key][0]["value"] < mnNumber:
								print ("dangerous" +key.decode("utf-8","ignore"))
								with open (filename,'a+') as f:
									f.write(key.decode("utf-8","ignore") + " : " + str(mnNumber) + '\n')
								if os.stat(filename).st_size != 0:
									print subprocess.check_output(['tail','-1',filename])[0:-1]
							#else:
								#print ("9) safe : " +key.decode("utf-8","ignore"))
						elif sensors_thresholds[0][key][0]["check"] == "lesser":
					 		if sensors_thresholds[0][key][0]["value"] > mnNumber:
								print ("dangerous" +key.decode("utf-8","ignore"))
								with open (filename,'a+') as f:
									f.write(key.decode("utf-8","ignore") + " : " + str(mnNumber) + '\n')
								if os.stat(filename).st_size != 0:
									print subprocess.check_output(['tail','-1',filename])[0:-1]
							#else:
								#print ("9) safe : " +key.decode("utf-8","ignore"))
						elif sensors_thresholds[0][key][0]["check"] == "absolute":		
							if abs(sensors_thresholds[0][key][0]["value"]) - abs(mnNumber) > .5:
								print ("dangerous" +key.decode("utf-8","ignore"))
								with open (filename,'a+') as f:
									f.write(key.decode("utf-8","ignore") + " : " + str(mnNumber) + '\n')
								if os.stat(filename).st_size != 0:
									print subprocess.check_output(['tail','-1',filename])[0:-1]
							#else:
								#print ("9) safe : " +key.decode("utf-8","ignore")) 
				#print ("10) after checking for danger" + key.decode("utf-8","ignore"))
alertFile.close()
client.close()
spark.stop()
