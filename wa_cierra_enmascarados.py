import os
import time
import requests
import pymongo
from pymongo import MongoClient
from dotenv import load_dotenv
import urllib3

# Desactivar las advertencias de seguridad de urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Cargar variables de entorno desde un archivo .env si está presente
load_dotenv()

# Configuración de variables de entorno
MONGO_USER = os.getenv('MONGO_USER')
MONGO_PASS = os.getenv('MONGO_PASS')
MONGODB_URI = os.getenv('MONGODB_URI')
MONGODB_DATABASENAME = os.getenv('MONGODB_DATABASENAME', 'OutageManager')

# Verificar que todas las variables de entorno necesarias estén definidas
if not all([MONGO_USER, MONGO_PASS, MONGODB_URI, MONGODB_DATABASENAME]):
    raise ValueError("Faltan variables de entorno: MONGO_USER, MONGO_PASS, MONGODB_URI o MONGODB_DATABASENAME.")

# URL del nuevo endpoint para enviar alarmas
POST_URL_BASE = 'https://om-alarm-event-manager-oum-outagemanager-prod.apps.ocp4-ph.cloudteco.com.ar/api/alarm/clear/'

def connect_db():
    """
    Establece una conexión con MongoDB usando las variables de entorno.
    """
    connection_string = MONGODB_URI.replace('${MONGO_USER}', MONGO_USER).replace('${MONGO_PASS}', MONGO_PASS)
    client = MongoClient(connection_string)
    return client

def aggregate_raised_alarms(db, limit=5):
    """
    Ejecuta el pipeline de agregación para obtener alarmas en estado 'RAISED' con un límite.
    """
    pipeline = [
        # Paso 1: Filtrar los documentos que tienen los alarmStates relevantes
        {
            "$match": {
                "alarmState": { "$in": ['RAISED', 'UPDATED', 'RETRY', 'CLEARED'] },
                # Agrega otros filtros aquí si es necesario para reducir el conjunto de datos
            }
        },
        # Paso 2: Proyectar solo los campos necesarios
        {
            "$project": {
                "_id": 0,
                "networkElementId": 1,
                "alarmRaisedTime": 1,
                "alarmState": 1,
                "alarmId": 1,
                "origenId": 1  # Asegúrate de incluir todos los campos necesarios
            }
        },
        # Paso 3: Agrupar por networkElementId y alarmRaisedTime, y recopilar los alarmStates y los documentos completos
        {
            "$group": {
                "_id": {
                    "networkElementId": "$networkElementId",
                    "alarmRaisedTime": "$alarmRaisedTime"
                },
                "alarmStates": { "$addToSet": "$alarmState" },
                "alarms": { "$push": "$$ROOT" }
            }
        },
        # Paso 4: Filtrar los grupos que tienen al menos un 'CLEARED' y uno de los otros estados
        {
            "$match": {
                "$and": [
                    { "alarmStates": { "$in": ['CLEARED'] } },
                    { "alarmStates": { "$in": ['RAISED', 'UPDATED', 'RETRY'] } }
                ]
            }
        },
        # Paso 5: Descomponer el array de alarmas para obtener los documentos individuales
        {
            "$unwind": "$alarms"
        },
        # Paso 6: Reemplazar la raíz del documento con cada alarma individual
        {
            "$replaceRoot": { "newRoot": "$alarms" }
        },
        # Paso 7: Filtrar solo las alarmas con alarmState 'RAISED', 'UPDATED', 'RETRY'
        {
            "$match": {
                "alarmState": { "$in": ['RAISED', 'UPDATED', 'RETRY'] },
                "alarmId": { "$exists": True, "$ne": None },
                "origenId": { "$exists": True, "$ne": None }
            }
        }#,
        # Paso 8: Limitar el número de resultados
        #{
        #    "$limit": limit
        #}
    ]

    return list(db.alarm.aggregate(pipeline, allowDiskUse=True))

def send_post_request(acaenvioalarmId):
    """
    Envía una solicitud POST al endpoint especificado con el acaenvioalarmId.
    """
    url = f"{POST_URL_BASE}{acaenvioalarmId}"
    try:
        response = requests.get(url, verify=False)  # Ignorar la verificación SSL
        if response.status_code in [200, 201]:
            print(f"Notificación: enviada exitosamente para alarmId: {acaenvioalarmId}")
        else:
            print(f"Fallo al enviar notificación para alarmId {acaenvioalarmId}: {response.status_code}, {response.text}")
    except Exception as e:
        print(f"Error al enviar notificación para alarmId {acaenvioalarmId}: {e}")

def process_alarms():
    """
    Función principal que maneja la agregación de alarmas y el envío de notificaciones.
    """
    client = None
    try:
        # Conectar a la base de datos
        client = connect_db()
        db = client[MONGODB_DATABASENAME]
        
        # Obtener alarmas en estado 'RAISED' con límite
        raised_alarms = aggregate_raised_alarms(db, limit=5)

        print(f"Total de alarmas 'RAISED' a procesar: {len(raised_alarms)}")

        for alarm in raised_alarms:
            alarmId = alarm.get('alarmId')
            origenId = alarm.get('origenId')

            if not alarmId:
                print(f"Alarma sin alarmId encontrada: {alarm}")
                continue

            # Preparar el campo acaenvioalarmId
            acaenvioalarmId = alarmId  # Asumiendo que alarmId se envía directamente

            # Enviar la solicitud POST al nuevo endpoint
            send_post_request(acaenvioalarmId)

            # Opcional: Imprimir la alarma procesada para depuración
            print(f"Alarma procesada - alarmId: {alarmId}, origenId: {origenId}")
            print("------------------------------------------------------------------------------------")            
            
            # Esperar 10 segundos antes de la siguiente llamada a la API
            time.sleep(10)            

    except Exception as e:
        print(f"Se produjo un error durante el procesamiento: {e}")
    finally:
        # Asegurarse de cerrar la conexión a la base de datos
        if client:
            client.close()
            print("Conexión a MongoDB cerrada.")

def main():
    """
    Ejecuta el proceso de manera cíclica cada 5 minutos.
    """
    while True:
        print(f"--- Iniciando nuevo ciclo a las {time.strftime('%Y-%m-%d %H:%M:%S')} ---")
        process_alarms()
        print(f"--- Ciclo completado. Durmiendo por 5 minutos. ---\n")
        # Esperar 5 minutos antes de iniciar el siguiente ciclo
        time.sleep(300)  # 300 segundos = 5 minutos

if __name__ == "__main__":
    main()
