from airflow.models.dag import DAG
from datetime import datetime
import pandas as pd
import pymongo # Se mantiene por si acaso, aunque no se usa para carga aquí
from bson.objectid import ObjectId # Importar ObjectId para verificar y convertir

# Configuración de MongoDB (la cadena de conexión es la misma, pero no se usa para carga aquí)
MONGO_CONNECTION_STRING = "mongodb+srv://rama_marin:Peta2017@bigdataupy5b.wk9joeh.mongodb.net/?retryWrites=true&w=majority&appName=BigDataUpy5B"
DATABASE_NAME = "crypto_data"
PROCESSED_COLLECTION_NAME = "processed_coinmarketcap_data" # Se usa para el nombre de la colección en la carga final

# Función auxiliar para limpiar ObjectIds
def convert_objectid_to_str(obj):
    if isinstance(obj, dict):
        return {k: convert_objectid_to_str(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [convert_objectid_to_str(elem) for elem in obj]
    elif isinstance(obj, ObjectId):
        return str(obj)
    else:
        return obj

def transform_coinmarketcap_data(**kwargs):
    ti = kwargs['ti']
    
    raw_listings = ti.xcom_pull(task_ids="extract_and_load_raw_coinmarketcap_data", key="return_value")

    print(f"DEBUG: Contenido de raw_listings recibido en transform_coinmarketcap_data: {raw_listings[:2] if raw_listings else 'None'}")
    print(f"DEBUG: Tipo de raw_listings: {type(raw_listings)}")

    if not raw_listings or not isinstance(raw_listings, list) or len(raw_listings) == 0:
        print("ERROR: No se encontraron listados de criptomonedas para transformar o el formato es incorrecto.")
        if raw_listings is None:
            print("ADVERTENCIA: raw_listings es None, la tarea de extracción pudo haber fallado o no encontró datos.")
            return None
        else:
            raise ValueError("Datos de CoinMarketCap inválidos o faltantes para transformar.")

    print("Iniciando transformación de datos de CoinMarketCap...")
    
    processed_records = []
    for crypto in raw_listings:
        try:
            processed_record = {
                "crypto_id": crypto.get("id"),
                "name": crypto.get("name"),
                "symbol": crypto.get("symbol"),
                "rank": crypto.get("cmc_rank"),
                "circulating_supply": crypto.get("circulating_supply"),
                "total_supply": crypto.get("total_supply"),
                "max_supply": crypto.get("max_supply"),
                "last_updated_utc": pd.to_datetime(crypto.get("last_updated")).isoformat() if crypto.get("last_updated") else None,
                "quote_usd_price": crypto.get("quote", {}).get("USD", {}).get("price", 0.0),
                "quote_usd_volume_24h": crypto.get("quote", {}).get("USD", {}).get("volume_24h", 0.0),
                "quote_usd_market_cap": crypto.get("quote", {}).get("USD", {}).get("market_cap", 0.0),
                "quote_usd_percent_change_1h": crypto.get("quote", {}).get("USD", {}).get("percent_change_1h", 0.0),
                "quote_usd_percent_change_24h": crypto.get("quote", {}).get("USD", {}).get("percent_change_24h", 0.0),
                "quote_usd_percent_change_7d": crypto.get("quote", {}).get("USD", {}).get("percent_change_7d", 0.0),
            }

            if processed_record["quote_usd_percent_change_24h"] > 5:
                processed_record["volatility_category"] = "High Volatility (Up)"
            elif processed_record["quote_usd_percent_change_24h"] < -5:
                processed_record["volatility_category"] = "High Volatility (Down)"
            else:
                processed_record["volatility_category"] = "Moderate/Low Volatility"
            
            processed_record["market_cap_billions_usd"] = processed_record["quote_usd_market_cap"] / 1_000_000_000

            processed_record['transformation_timestamp'] = datetime.now().isoformat()
            
            # La limpieza de _id se hará en la función de carga genérica si es necesario
            # processed_record_clean = convert_objectid_to_str(processed_record)
            processed_records.append(processed_record)

        except Exception as e:
            print(f"ADVERTENCIA: Error al procesar un registro de criptomoneda: {e}. Saltando registro.")
            continue

    if not processed_records:
        print("ADVERTENCIA: No se generaron registros procesados de CoinMarketCap.")
        return None

    # <<-- CAMBIO CLAVE AQUÍ: ELIMINADA LA LÓGICA DE CARGA Y XCOM PUSH DIRECTO -->>
    # La carga se hará en load_mongo.py
    # Solo retornamos los datos procesados para que el PythonOperator los ponga en XCom
    print(f"Registros transformados de CoinMarketCap: {len(processed_records)}")
    return processed_records # Retorna los datos procesados para XCom