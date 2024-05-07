from pymongo import MongoClient, database
import numpy as np  # Importing NumPy library for mathematical operations.
import subprocess
import threading
import pymongo
from datetime import datetime, timedelta
import time
import pytz

DBName = "test" 
connectionURL = "mongodb+srv://jesusdonate:Jdr081201@cluster0.sc8urqs.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0" #Put your database URL here
sensorTableStr = "sensor data" 
sensorTableMetaDataStr = "sensor data_metadata"

def QueryToList(query):
    queryList = []
    for doc in query:
        queryList.append(doc)
    return queryList

def print_sensor_metadata(sensorMetaData):
    docs = sensorMetaData.find({})
    metadata_list = []
    for doc in docs:
        uid, location = ParseMetaDataDocument(doc)
        metadata_list.append((uid, location))
    return metadata_list

def ParseMetaDataDocument(doc):
    lat = doc.get('latitude', 'Unknown')
    long = doc.get('longitude', 'Unknown')
    location = f"{lat},{long}"
    uid = doc.get('assetUid', 'default_uid')
    return uid, location

def ParseDocument(doc):
    uid = doc.get('assetUid', 'default_uid')  # Extract UID directly
    road = doc['topic'].split('/')[0]
    traffic_values = [value for key, value in doc['payload'].items() if 'Traffic Sensor' in key]
    return uid, road, traffic_values

# Build a mapping from asset UIDs to metadata UIDs using the metadata collection
def build_uid_map(sensorMetaData):
    uid_map = {}
    docs = sensorMetaData.find({})
    for doc in docs:
        meta_uid, location = ParseMetaDataDocument(doc)
        uid_map[meta_uid] = meta_uid  
    return uid_map

def QueryDatabase() -> []: # type: ignore
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
       
        #print("Database collections: ", db.list_collection_names())
        sensorTable = db[sensorTableStr]
        #print("Table:", sensorTable)
        sensorMetaData = db[sensorTableMetaDataStr]
        #print_sensor_metadata(sensorMetaData)
        metadata_list = print_sensor_metadata(sensorMetaData)
        # Example validation function to cross-check UIDs between metadata and traffic data
        # Create the mapping between UIDs in metadata and sensor data
        uid_map = build_uid_map(sensorMetaData)
       
               
        metaDoc = QueryToList
        #We convert the cursor that mongo gives us to a list for easier iteration.
        timeCutOff = datetime.now() - timedelta(minutes=5) #TODO: Set how many minutes you allow
        timeCutOff = timeCutOff.astimezone(pytz.utc)
        
        print(" ")
        print(f"Data TimeCutOff {timeCutOff}")
        oldDocuments = QueryToList(sensorTable.find({"time":{"$lt":timeCutOff}}))
        currentDocuments = QueryToList(sensorTable.find({"time":{"$gte":timeCutOff}}))
        

        print(" ")
        print("Current & Old Documents...")
        print(" ")
        #print("Current Docs:",currentDocuments)
        #print("Old Docs:",oldDocuments)

        

        road_data = {}

        # Create a new road metadata map with formatted highway names
        formatted_road_metadata_map = {f"Highway {i + 1}": (metadata[0], metadata[1]) for i, metadata in enumerate(metadata_list)}


        # Debugging output
        print("Road Metadata Map:", formatted_road_metadata_map)
        print(" ")
        
        for doc in oldDocuments:
            try:
                parse_uid, road, traffic_values = ParseDocument(doc)
                meta_uid = uid_map.get(parse_uid, "Unknown")  # Get the UID from metadata
                road_data.setdefault(road, []).extend([(value, meta_uid) for value in traffic_values])
            except ValueError:
                continue

        results = []
        for i, (road, values_with_uid) in enumerate(road_data.items()):
            if values_with_uid:
                traffic_values = [value[0] for value in values_with_uid]  # Extract traffic values only
                average_traffic = np.mean(traffic_values)
                uid, location = metadata_list[i] if i < len(metadata_list) else ("Unknown", "Unknown")
                formatted_str = f"|------Highway {i + 1}------|\n" \
                                f"|Name: {road}   |\n" \
                                f"|Uid: {uid} |\n" \
                                f"|Location: {location}      |\n" \
                                f"|Avg. traffic: {average_traffic:.2f}  |\n" \
                                f"|---------------------|\n"
                print(formatted_str)
                results.append((road, average_traffic, uid, location))


        if results:
            best_traffic = min(results, key=lambda x: x[1])
            best_index = results.index(best_traffic)
            uid, location = metadata_list[best_index] if best_index < len(metadata_list) else ("Unknown", "Unknown")
        return results 

    except Exception as e:
        print("Please make sure that this machine's IP has access to MongoDB.")
        print("Error:",e)
        exit(0)
