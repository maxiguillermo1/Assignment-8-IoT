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
    road = doc['payload']['topic'].split('/')[1]
	
    # Extracting traffic sensor data from the 'payload'.
    # Assuming the sensor data is a string starting with 'Traffic Sensor' followed by time value.
    sensor_data = next((value for key, value in doc['payload'].items() if 'Traffic Sensor' in key), None)
    if sensor_data is None:
        raise ValueError("Sensor data not found in document.")
    traffic_time = int(sensor_data.split(': ')[1])  # Extracting the time value.

    # Parsing the timestamp from the 'time' field of the payload.
    record_time = datetime.strptime(doc['payload']['time'], "%Y-%m-%dT%H:%M:%S.%f%z")

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
		timeCutOff = datetime.now() - timedelta(minutes=30) #TODO: Set how many minutes you allow

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



# import numpy as np  # Importing NumPy library for mathematical operations.
# from pymongo import MongoClient  # Importing MongoClient from PyMongo to connect to MongoDB.
# from datetime import datetime, timedelta  # Importing datetime module for handling dates and times.

# # Database parameters
# DBName = "test"  # Name of the MongoDB database.
# # Connection URL to the MongoDB database, including username, password, and cluster information.
# connectionURL = "mongodb+srv://jesusdonate:Jdr081201@cluster0.sc8urqs.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
# sensorTable = "sensor_data"  # Name of the MongoDB collection containing sensor data.

# def QueryToList(query):
#     # Function to convert MongoDB query result to a list.
#     return list(query)

# def ParseDocument(doc):
#     # Function to parse individual MongoDB documents and extract relevant information.

#     # Extracting the road name from the 'topic' field of the document.
#     road = doc['payload']['topic'].split('/')[1]

#     # Extracting traffic sensor data from the 'payload'.
#     # Assuming the sensor data is a string starting with 'Traffic Sensor' followed by time value.
#     sensor_data = next((value for key, value in doc['payload'].items() if 'Traffic Sensor' in key), None)
#     if sensor_data is None:
#         raise ValueError("Sensor data not found in document.")
#     traffic_time = int(sensor_data.split(': ')[1])  # Extracting the time value.

#     # Parsing the timestamp from the 'time' field of the payload.
#     record_time = datetime.strptime(doc['payload']['time'], "%Y-%m-%dT%H:%M:%S.%f%z")

#     return road, traffic_time, record_time  # Returning extracted data as a tuple.

# def QueryDatabase():
#     global DBName, connectionURL, sensorTable
#     client = None  # Initializing MongoDB client variable.
#     try:
#         # Establishing connection to MongoDB using the provided connection URL.
#         client = MongoClient(connectionURL)
#         db = client[DBName]  # Accessing the specified database.
#         sensor_collection = db[sensorTable]  # Accessing the sensor data collection.

#         # Defining a cutoff time for current documents based on current UTC time.
#         timeCutOff = datetime.utcnow()

#         # Querying MongoDB for old documents (before cutoff time) and current documents (after cutoff time).
#         oldDocuments = QueryToList(sensor_collection.find({"time": {"$lt": timeCutOff}}))
#         currentDocuments = QueryToList(sensor_collection.find({"time": {"$gte": timeCutOff}}))

#         # Printing old and current documents for debugging purposes.
#         print("Old Docs:", oldDocuments)
#         print("Current Docs:", currentDocuments)

#         # Aggregating data from current documents.
#         road_data = {}
#         for doc in currentDocuments:
#             try:
#                 road, traffic_time, _ = ParseDocument(doc)  # Parsing individual document.
#                 road_data.setdefault(road, []).append(traffic_time)  # Adding traffic time to road data.
#             except ValueError:
#                 # Handling case where sensor data is not found in the document.
#                 continue

#         # Calculating average time and count of readings for each road.
#         results = [(road, len(times), np.mean(times)) for road, times in road_data.items()]

#         return results  # Returning results.

#     except Exception as e:
#         # Handling any exceptions that occur during database query or processing.
#         print("An error occurred:", e)
#         return []  # Returning empty list in case of error.
#     finally:
#         if client:
#             client.close()  # Closing MongoDB client connection to release resources.

# # Calling the QueryDatabase function to retrieve and process sensor data.
# road_info = QueryDatabase()

# # Printing out the processed data for each road.
# for info in road_info:
#     print(f"Road: {info[0]}, Cars: {info[1]}, Average Time: {info[2]}")