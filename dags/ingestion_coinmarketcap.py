from airflow.models.dag import DAG
from datetime import datetime
import requests
import pymongo
import json

# Configuración de MongoDB para datos crudos de CoinMarketCap
MONGO_CONNECTION_STRING = "mongodb+srv://rama_marin:Peta2017@bigdataupy5b.wk9joeh.mongodb.net/?retryWrites=true&w=majority&appName=BigDataUpy5B"
DATABASE_NAME = "crypto_data" # Nueva base de datos para criptomonedas
RAW_COLLECTION_NAME = "raw_coinmarketcap_listings" # Colección para datos crudos de CoinMarketCap

# --- TU API KEY DE COINMARKETCAP ---
COINMARKETCAP_API_KEY = "0590c2a5-d251-4b21-859e-11767ca74abc"

def extract_and_load_raw_coinmarketcap_data(**kwargs):
    """
    Extrae los listados más recientes de criptomonedas de CoinMarketCap.
    Almacena los datos crudos en MongoDB (con idempotencia) y los pasa como XCom.
    """
    ti = kwargs['ti']
    
    URL = "https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest"
    headers = {
        'Accepts': 'application/json',
        'X-CMC_PRO_API_KEY': COINMARKETCAP_API_KEY,
    }
    parameters = {
        'start': '1',
        'limit': '10', # Obtener las top 10 criptomonedas
        'convert': 'USD'
    }

    print(f"Iniciando extracción de datos de CoinMarketCap desde: {URL}")
    
    client = None
    try:
        response = requests.get(URL, headers=headers, params=parameters)
        response.raise_for_status()

        raw_api_response = response.json()
        
        if 'data' not in raw_api_response or not isinstance(raw_api_response.get('data'), list) or \
           'status' not in raw_api_response or raw_api_response['status'].get('error_code') != 0:
            print(f"ERROR: La respuesta de la API de CoinMarketCap no contiene la clave 'data' o 'status' indica un error. Respuesta completa: {raw_api_response}")
            raise ValueError("La API de CoinMarketCap no devolvió los datos esperados o hubo un error en la respuesta.")

        crypto_listings_to_pass = raw_api_response.get('data')
        
        # --- Almacenar datos crudos en MongoDB (con idempotencia) ---
        client = pymongo.MongoClient(MONGO_CONNECTION_STRING)
        db = client[DATABASE_NAME]
        raw_collection = db[RAW_COLLECTION_NAME]
        
        # <<-- CAMBIO CLAVE AQUÍ: ELIMINAR DOCUMENTOS ANTERIORES -->>
        print(f"Limpiando la colección {RAW_COLLECTION_NAME} antes de insertar nuevos datos crudos...")
        raw_collection.delete_many({}) # Elimina todos los documentos de la colección
        print("Colección de datos crudos limpiada exitosamente.")

        # Añade un timestamp a los datos crudos para trazabilidad
        raw_api_response['ingestion_timestamp'] = datetime.now().isoformat()
        
        raw_collection.insert_one(raw_api_response) 
        print(f"Datos crudos de CoinMarketCap cargados exitosamente en {DATABASE_NAME}.{RAW_COLLECTION_NAME}.")
        
        ti.xcom_push(key="raw_coinmarketcap_data_for_transform", value=crypto_listings_to_pass) 
        print(f"Registros extraídos de CoinMarketCap (crudos): {len(crypto_listings_to_pass)}")

        return crypto_listings_to_pass

    except requests.exceptions.RequestException as e:
        print(f"Error en la extracción de la API de CoinMarketCap (RequestException): {e}")
        raise
    except ValueError as e:
        print(f"Error de validación en la extracción de CoinMarketCap: {e}")
        raise
    except Exception as e:
        print(f"Error inesperado en la extracción de CoinMarketCap: {e}")
        raise
    finally:
        if client:
            client.close()