from pymongo import MongoClient, database
import numpy as np  # Importing NumPy library for mathematical operations.
import subprocess
import threading
import pymongo
from datetime import datetime, timedelta
import time

DBName = "test" #Use this to change which Database we're accessing
connectionURL = "mongodb+srv://jesusdonate:Jdr081201@cluster0.sc8urqs.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0" #Put your database URL here
sensorTable = "sensor data" #Change this to the name of your sensor data table

def QueryToList(query):
	#TODO: Convert the query that you get in this function to a list and return it
	#HINT: MongoDB queries are iterable
    queryList = []
    for doc in query:
        queryList.append(doc)
    return queryList


def ParseDocument(doc):
	# Function to parse individual MongoDB documents and extract relevant information.

	# Extracting the road name from the 'topic' field of the document.
	road = doc['topic'].split('/')[0]
	traffic = (value for key, value in doc['payload'].items() if 'Traffic Sensor' in key), None
	traffic_time = 2
	record_time = 3

	return road, traffic_time, record_time  # Returning extracted data as a tuple.


def QueryDatabase() -> []:
	global DBName
	global connectionURL
	global currentDBName
	global running
	global filterTime
	global sensorTable
	cluster = None
	client = None
	db = None
	try:
		cluster = connectionURL
		client = MongoClient(cluster)
		db = client[DBName]
		print("Database collections: ", db.list_collection_names())

		#We first ask the user which collection they'd like to draw from.
		sensorTable = db[sensorTable]
		print("Table:", sensorTable)
		#We convert the cursor that mongo gives us to a list for easier iteration.
		timeCutOff = datetime.now() - timedelta(minutes=5) #TODO: Set how many minutes you allow

		oldDocuments = QueryToList(sensorTable.find({"time":{"$lt":timeCutOff}}))
		currentDocuments = QueryToList(sensorTable.find({"time":{"$gte":timeCutOff}}))

		print("Current Docs:",currentDocuments)
		print("Old Docs:",oldDocuments)

		#TODO: Parse the documents that you get back for the sensor data that you need
		#Return that sensor data as a list
		road_data = {}
		for doc in oldDocuments:
			try:
				road, traffic_time, _ = ParseDocument(doc)  # Parsing individual document.
				road_data.setdefault(road, []).append(traffic_time)  # Adding traffic time to road data.
			except ValueError:
				# Handling case where sensor data is not found in the document.
				continue

		# Calculating average time and count of readings for each road.
		results = [(road, len(times), np.mean(times)) for road, times in road_data.items()]

		return results  # Returning results.


	except Exception as e:
		print("Please make sure that this machine's IP has access to MongoDB.")
		print("Error:",e)
		exit(0)
