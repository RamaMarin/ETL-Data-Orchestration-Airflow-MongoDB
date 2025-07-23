from airflow.models.dag import DAG
from datetime import datetime
import requests
import pymongo
import json
from mongo_utils import get_mongo_client 


DATABASE_NAME = "population_data" # Nueva base de datos para datos de población
RAW_COLLECTION_NAME = "raw_worldbank_population" # Colección para datos crudos de población

def extract_and_load_raw_worldbank_data(**kwargs):
    """
    Extrae datos de población de México del Banco Mundial para un año específico.
    Almacena los datos crudos en MongoDB (con idempotencia) y los pasa como XCom.
    """
    ti = kwargs['ti']
    
    country_code = 'MEX'
    indicator_code = 'SP.POP.TOTL' # Indicador para Población Total
    year = '2023' # Año específico para el que queremos el dato de población
    
    URL = f"https://api.worldbank.org/v2/country/{country_code}/indicator/{indicator_code}?format=json&date={year}"

    print(f"Iniciando extracción de datos de población para {country_code} en el año {year} desde: {URL}")
    
    client = None
    try:
        response = requests.get(URL)
        response.raise_for_status()

        raw_api_response = response.json()
        
        # La API del Banco Mundial devuelve una lista con 2 elementos: [metadata, data_records]
        # Los datos relevantes están en el segundo elemento (índice 1)
        if not isinstance(raw_api_response, list) or len(raw_api_response) < 2 or \
           not isinstance(raw_api_response[1], list) or not raw_api_response[1]:
            print(f"ERROR: La respuesta de la API del Banco Mundial no contiene el formato esperado o no hay datos. Respuesta completa: {raw_api_response}")
            raise ValueError("La API del Banco Mundial no devolvió los datos esperados.")

        # Tomamos el primer (y único) registro de datos de población para el año
        population_data_record = raw_api_response[1][0]
        
        # --- Almacenar datos crudos en MongoDB (con idempotencia) ---
        # CAMBIAR ESTA LÍNEA:
        # client = pymongo.MongoClient(MONGO_CONNECTION_STRING)
        client = get_mongo_client() # <<-- CAMBIADO: Usar la función centralizada
        db = client[DATABASE_NAME]
        raw_collection = db[RAW_COLLECTION_NAME]
        
        # <<-- IDEMPOTENCIA: Reemplazar el documento si ya existe para el mismo año -->>
        # Usamos el indicador y el año como identificador único para la idempotencia
        raw_collection.replace_one(
            {"indicator.id": indicator_code, "date": year}, # Filtro
            population_data_record,                          # Documento a reemplazar/insertar
            upsert=True                                      # Inserta si no encuentra match
        )
        print(f"Datos crudos de población para {country_code} en {year} cargados/actualizados exitosamente en {DATABASE_NAME}.{RAW_COLLECTION_NAME}.")
        
        # Pasa el registro de datos de población a la siguiente tarea (transformación) via XCom
        ti.xcom_push(key="raw_worldbank_data_for_transform", value=population_data_record)
        
        print(f"Registros extraídos de Banco Mundial (crudos): 1")

        return population_data_record # Retorna los datos para XCom

    except requests.exceptions.RequestException as e:
        print(f"Error en la extracción de la API del Banco Mundial (RequestException): {e}")
        raise
    except ValueError as e:
        print(f"Error de validación en la extracción del Banco Mundial: {e}")
        raise
    except Exception as e:
        print(f"Error inesperado en la extracción del Banco Mundial: {e}")
        raise
    finally:
        if client:
            client.close()