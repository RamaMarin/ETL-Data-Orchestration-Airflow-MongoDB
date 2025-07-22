from airflow.models.dag import DAG # No es estrictamente necesario aquí
from datetime import datetime
import requests
import pymongo
import json

# Configuración de MongoDB para datos crudos
MONGO_CONNECTION_STRING = "mongodb+srv://rama_marin:Peta2017@bigdataupy5b.wk9joeh.mongodb.net/?retryWrites=true&w=majority&appName=BigDataUpy5B"
DATABASE_NAME = "covid_db"
RAW_COLLECTION_NAME = "raw_covid_data"

def extract_and_load_raw_covid_data(**kwargs):
    ti = kwargs['ti']
    country_iso = 'MEX'
    
    fixed_date = "2023-03-09" 
    
    URL = f"https://covid-api.com/api/reports/total?date={fixed_date}&iso={country_iso}"

    print(f"Iniciando extracción de datos de COVID-19 para {country_iso} en la fecha {fixed_date} desde: {URL}")

    client = None
    try:
        response = requests.get(URL)
        response.raise_for_status()

        raw_api_response = response.json()

        if 'data' not in raw_api_response or not isinstance(raw_api_response.get('data'), dict):
            print(f"ERROR: La respuesta de la API no contiene la clave 'data' o 'data' no es un diccionario. Respuesta completa: {raw_api_response}")
            raise ValueError("La API de COVID-19 no devolvió los datos esperados en la clave 'data'.")

        covid_data_to_pass = raw_api_response.get('data')

        # --- Almacenar datos crudos en MongoDB (con idempotencia) ---
        client = pymongo.MongoClient(MONGO_CONNECTION_STRING)
        db = client[DATABASE_NAME]
        raw_collection = db[RAW_COLLECTION_NAME]
        
        # Añade un timestamp a los datos crudos y la fecha de ejecución para la idempotencia
        raw_api_response['ingestion_timestamp'] = datetime.now().isoformat()
        raw_api_response['execution_date'] = fixed_date # Usar la fecha fija como identificador

        # <<-- CAMBIO CLAVE AQUÍ: replace_one para idempotencia -->>
        # Busca un documento con la misma fecha de ejecución y lo reemplaza, o lo inserta si no existe.
        raw_collection.replace_one(
            {"execution_date": fixed_date}, # Filtro para encontrar el documento
            raw_api_response,              # Nuevo documento para insertar/reemplazar
            upsert=True                    # Inserta si no encuentra un match
        )
        print(f"Datos crudos de COVID-19 para {country_iso} en {fixed_date} cargados/actualizados exitosamente en {DATABASE_NAME}.{RAW_COLLECTION_NAME}.")
        
        ti.xcom_push(key="raw_covid_data_for_transform", value=covid_data_to_pass)
        print(f"Registros extraídos de COVID-19 (crudos): 1")
        
        return covid_data_to_pass

    except requests.exceptions.RequestException as e:
        print(f"Error en la extracción de la API de COVID-19 (RequestException): {e}")
        raise
    except ValueError as e:
        print(f"Error de validación en la extracción de COVID-19: {e}")
        raise
    except Exception as e:
        print(f"Error inesperado en la extracción de COVID-19: {e}")
        raise
    finally:
        if client:
            client.close()