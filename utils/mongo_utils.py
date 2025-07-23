# utils/mongo_utils.py
from pymongo import MongoClient

# La cadena de conexión para el contenedor Docker de MongoDB
# 'mongodb' es el nombre del servicio definido en docker-compose.yml
MONGO_URI = "mongodb://mongodb:27017/"

def get_mongo_client():
    """Retorna una instancia de MongoClient conectada al contenedor Docker de MongoDB."""
    try:
        client = MongoClient(MONGO_URI)
        # Opcional: Probar la conexión
        client.admin.command('ping')
        print("Conexión a MongoDB local exitosa!")
        return client
    except Exception as e:
        print(f"Error al conectar a MongoDB local: {e}")
        raise

def get_mongo_db(db_name):
    """Retorna una instancia de la base de datos especificada."""
    client = get_mongo_client()
    return client[db_name]