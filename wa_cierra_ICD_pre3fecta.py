import os
import requests
import pymongo
from pymongo import MongoClient

#
#
# revisa si hay en RAISEDel tkt de ICD que estan cerrado en origenId y lo cierra
#
# se uso en la semana de implementacion de 3fecta para los no cerrados por el 
# corte del API de ICD
#
#

# Step 1: MongoDB connection setup using pymongo
def connect_db():
    username = os.getenv('MONGO_USER')
    password = os.getenv('MONGO_PASS')
    hostmongodb = os.getenv('MONGODB_URI')

    if username and password and hostmongodb:
        connection_string = hostmongodb.replace('${MONGO_USER}', username).replace('${MONGO_PASS}', password)
    else:
        raise ValueError("Environment variables MONGODB_URI, MONGO_USER, or MONGO_PASS are not defined.")

    # Create a MongoDB client using pymongo
    client = MongoClient(connection_string)
    return client

# Step 2: Setup connection to the database using MONGODB_DATABASENAME
client = connect_db()
db = client[os.getenv('MONGODB_DATABASENAME')]  # Get the DB name from the environment variable

# URL for POST request
post_url = 'https://oum-receiver.telecom.com.ar/api/v1/notifications'

# Step 3: Query the "alarm" collection with the new filters
def query_alarm():
    return list(db.alarm.find(
        {
            "$and": [
                {"alarmState": {"$eq": "RAISED"}},
                {"$expr": {"$eq": [{"$strLenCP": "$alarmId"}, 8]}}
                #,{"$expr": {"$ne": ["$alarmId", "$origenId"]}}  # Ensure alarmId is not equal to origenId
            ]
        },
        {"_id": 0, "alarmId": 1, "origenId":1,  "networkElementId": 1}
    ).sort("_id", -1))  # Use list() to convert cursor to list

# Step 4: For each result, query the "trifecta-prod-ps" collection with aggregation and union
def query_trifecta_for_result(alarmId, networkElementId):
    pipeline = [
        {
            "$match": {
                "$and": [
                    {"origenId": alarmId},
                    {"sourceSystemId": "ICD"},
                    {"json_recibido.assets.alarmState": "CLEARED"},
                    {"json_recibido.assets.networkElementId": networkElementId}
                ]
            }
        },
        {
            "$unionWith": {
                "coll": "trifecta-prod-ph",
                "pipeline": [
                    {
                        "$match": {
                            "$and": [
                                {"origenId": alarmId},
                                {"sourceSystemId": "ICD"},
                                {"json_recibido.assets.alarmState": "CLEARED"},
                                {"json_recibido.assets.networkElementId": networkElementId}
                            ]
                        }
                    }
                ]
            }
        },
        {
            "$project": {
                "alarmId": 1,
                "json_recibido": 1
            }
        }
    ]
    return db.get_collection("trifecta-prod-ps").aggregate(pipeline)

# Step 5: Send POST request
def send_post_request(data):
    try:
        response = requests.post(post_url, json=data)
        if response.status_code == 200:
            print(f"Notification sent successfully.")
        else:
            print(f"Failed to send notification: {response.status_code}, {response.text}")
    except Exception as e:
        print(f"Error sending notification: {e}")

# Step 6: Main logic to query, match, and send data
def main():
    # Step 1: Get alarm results
    alarm_results = query_alarm()

    # Step 2: Iterate over the results from alarm collection
    for result in alarm_results:
        alarmId = result['origenId']    #SWAP
        origenId = result['alarmId']    #SWAP
        networkElementId = result['networkElementId']

        # Step 3: Query trifecta-prod-ps and trifecta-prod-ph for each alarmId and networkElementId
        trifecta_results = query_trifecta_for_result(alarmId, networkElementId)

        # Step 4: Iterate over the aggregation results
        for trifecta_result in trifecta_results:
            if 'json_recibido' in trifecta_result:
                json_data = trifecta_result['json_recibido']
                
                # Step 5: Replace origenId with alarmId in json_recibido
                if 'origenId' in json_data:
                    json_data['alarmId'] = origenId
                    json_data['origenId']  = alarmId
                
                # Step 6: Send the modified json_recibido or print it for testing
                send_post_request(json_data)
                print(json_data)

if __name__ == "__main__":
    main()
