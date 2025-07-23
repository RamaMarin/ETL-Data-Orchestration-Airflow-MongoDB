from airflow.models.dag import DAG
from datetime import datetime
import pandas as pd # Se mantiene por si acaso, aunque no se usa intensivamente aquí
import pymongo # Se mantiene por si acaso, aunque no se usa para carga aquí
from bson.objectid import ObjectId # Importar ObjectId para verificar y convertir


# NOTA: La conexión a MongoDB ya no es necesaria en este archivo ya que la carga se hace en 'load_mongo.py'
DATABASE_NAME = "population_data"
PROCESSED_COLLECTION_NAME = "processed_worldbank_population" # Colección para datos procesados

# Función auxiliar para limpiar ObjectIds (si los datos pasaran por MongoDB antes)
def convert_objectid_to_str(obj):
    if isinstance(obj, dict):
        return {k: convert_objectid_to_str(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [convert_objectid_to_str(elem) for elem in obj]
    elif isinstance(obj, ObjectId):
        return str(obj)
    else:
        return obj

def transform_worldbank_data(**kwargs):
    """
    Transforma los datos crudos de población del Banco Mundial obtenidos de XCom.
    Estandariza y enriquece los datos.
    """
    ti = kwargs['ti']
    
    raw_population_record = ti.xcom_pull(task_ids="extract_and_load_raw_worldbank_data", key="return_value")

    print(f"DEBUG: Contenido de raw_population_record recibido en transform_worldbank_data: {raw_population_record}")
    print(f"DEBUG: Tipo de raw_population_record: {type(raw_population_record)}")

    if not raw_population_record or not isinstance(raw_population_record, dict) or 'value' not in raw_population_record:
        print("ERROR: No se encontraron datos de población para transformar o el formato es incorrecto.")
        if raw_population_record is None:
            print("ADVERTENCIA: raw_population_record es None, la tarea de extracción pudo haber fallado o no encontró datos.")
            return None
        else:
            raise ValueError("Datos de población inválidos o faltantes para transformar.")

    print("Iniciando transformación de datos de población del Banco Mundial...")
    
    try:
        # 1. Estandarización y Limpieza
        processed_record = {
            "country_name": raw_population_record.get("country", {}).get("value"),
            "country_iso3code": raw_population_record.get("countryiso3code"),
            "indicator_name": raw_population_record.get("indicator", {}).get("value"),
            "indicator_id": raw_population_record.get("indicator", {}).get("id"),
            "year": int(raw_population_record.get("date")), # Convertir el año a entero
            "population_total": raw_population_record.get("value"), # El valor de la población
            "last_updated_api": raw_population_record.get("obs_status") # A veces hay un campo de estado de observación
        }

        # Manejo de valores nulos (ya cubierto con .get())

        # 2. Enriquecimiento: Añadir campos derivados
        # Ejemplo: Categoría de población (si es > 100M, etc.)
        if processed_record["population_total"] > 100_000_000:
            processed_record["population_category"] = "Large Population (>100M)"
        elif processed_record["population_total"] > 50_000_000:
            processed_record["population_category"] = "Medium Population (50M-100M)"
        else:
            processed_record["population_category"] = "Smaller Population (<50M)"
        
        processed_record['transformation_timestamp'] = datetime.now().isoformat()
        
        print("Datos de población transformados exitosamente.")
        
        # Retornamos el registro procesado para que el PythonOperator lo ponga en XCom
        return processed_record

    except Exception as e:
        print(f"Error inesperado durante la transformación de datos de población: {e}")
        raise