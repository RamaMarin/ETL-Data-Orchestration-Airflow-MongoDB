from airflow.models.dag import DAG
from datetime import datetime
import pymongo
import json
from bson.objectid import ObjectId
from mongo_utils import get_mongo_client # Importación directa, como lo tienes ahora


def convert_objectid_to_str(obj):
    if isinstance(obj, dict):
        return {k: convert_objectid_to_str(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [convert_objectid_to_str(elem) for elem in obj]
    elif isinstance(obj, ObjectId):
        return str(obj)
    else:
        return obj

def load_processed_data_to_mongo(**kwargs):
    ti = kwargs['ti']
    
    collection_name = kwargs.get('collection_name')
    xcom_key = kwargs.get('xcom_key')
    xcom_task_id = kwargs.get('xcom_task_id')

    if not collection_name or not xcom_key or not xcom_task_id:
        raise ValueError("load_processed_data_to_mongo requiere 'collection_name', 'xcom_key' y 'xcom_task_id' en op_kwargs.")

    processed_data = ti.xcom_pull(task_ids=xcom_task_id, key="return_value")

    print(f"DEBUG: Contenido de processed_data recibido para carga en '{collection_name}': {processed_data[:500] if isinstance(processed_data, list) else processed_data}")
    print(f"DEBUG: Tipo de processed_data: {type(processed_data)}")

    if not processed_data:
        print(f"ADVERTENCIA: No hay datos procesados para cargar en '{collection_name}'. Saltando carga.")
        return

    print(f"Iniciando carga de datos procesados en MongoDB en la colección: {collection_name}...")

    client = None
    try:
        client = get_mongo_client() # Usar la función centralizada
        
        if "covid" in collection_name:
            db_name = "covid_db"
        elif "coinmarketcap" in collection_name:
            db_name = "crypto_data"
        elif "worldbank" in collection_name: 
            db_name = "population_data"    
        else:
            raise ValueError(f"Base de datos no definida para la colección: {collection_name}")

        db = client[db_name]
        collection = db[collection_name]
        
        print(f"Limpiando la colección {collection_name} antes de insertar nuevos datos...")
        # Aseguramos que la eliminación se completa antes de la inserción
        delete_result = collection.delete_many({})
        print(f"Colección limpiada exitosamente. Documentos eliminados: {delete_result.deleted_count}.")

        cleaned_data_for_mongo = convert_objectid_to_str(processed_data)

        if isinstance(cleaned_data_for_mongo, list):
            if cleaned_data_for_mongo: # Solo insertar si la lista no está vacía
                result = collection.insert_many(cleaned_data_for_mongo)
                print(f"Datos cargados exitosamente en MongoDB en {db_name}.{collection_name}.")
                print(f"Registros insertados (procesados): {len(result.inserted_ids)} en {collection_name}")
            else:
                print(f"ADVERTENCIA: La lista de datos para {collection_name} está vacía, no se insertó nada.")
        else: # Asumimos que es un solo diccionario para insert_one
            result = collection.insert_one(cleaned_data_for_mongo)
            print(f"Datos cargados exitosamente en MongoDB en {db_name}.{collection_name}.")
            print(f"Registros insertados (procesados): 1 en {collection_name} (ID: {result.inserted_id})")
        
        # Una forma extra de asegurar que las operaciones se han confirmado antes de cerrar la conexión
        # Puedes añadir un pequeño sleep si el problema persiste, pero no es ideal
        # import time
        # time.sleep(0.5) 

    except pymongo.errors.ConnectionFailure as e:
        print(f"Error de conexión a MongoDB local durante la carga en '{collection_name}': {e}") # Corregido a "local"
        raise
    except Exception as e:
        print(f"Error inesperado durante la carga en '{collection_name}': {e}")
        raise
    finally:
        if client:
            client.close() # Asegurar que la conexión se cierra