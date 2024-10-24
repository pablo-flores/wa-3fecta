from flask_pymongo import PyMongo
import os

# Crear la instancia de PyMongo
mongo = PyMongo()

def init_mongo(app):
    username = os.getenv('MONGO_USER')
    password = os.getenv('MONGO_PASS')
    hostmongodb = os.getenv('MONGODB_URI')

    if username and password and hostmongodb:
        hostmongodb = hostmongodb.replace('${MONGO_USER}', username).replace('${MONGO_PASS}', password)
    else:
        raise ValueError("Las variables de entorno MONGODB_URI o MONGO_USER o MONGO_PASS no están definidas.")
    
    app.config["MONGO_URI"] = hostmongodb
    mongo.init_app(app)
