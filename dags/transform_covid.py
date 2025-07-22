from airflow.models.dag import DAG # No es estrictamente necesario aquí
from datetime import datetime
import pandas as pd
import pymongo

def transform_covid_data(**kwargs):
    ti = kwargs['ti']
    
    # Jalar el XCom de la tarea de extracción (sin clave dinámica)
    raw_data = ti.xcom_pull(task_ids="extract_and_load_raw_covid_data", key="return_value")

    print(f"DEBUG: Contenido de raw_data recibido en transform_covid_data: {raw_data}")
    print(f"DEBUG: Tipo de raw_data: {type(raw_data)}")

    if not raw_data or not isinstance(raw_data, dict) or 'date' not in raw_data:
        print("ERROR: Datos crudos de COVID-19 inválidos o faltantes para transformar.")
        raise ValueError("No se encontraron datos crudos de COVID-19 válidos para transformar.")

    print("Iniciando transformación de datos de COVID-19...")

    try:
        transformed_record = {
            "date": pd.to_datetime(raw_data.get("date")).isoformat() if raw_data.get("date") else None,
            "last_update_utc": pd.to_datetime(raw_data.get("last_update")).isoformat() if raw_data.get("last_update") else None,
            "total_confirmed": raw_data.get("confirmed", 0),
            "confirmed_daily_change": raw_data.get("confirmed_diff", 0),
            "total_deaths": raw_data.get("deaths", 0),
            "deaths_daily_change": raw_data.get("deaths_diff", 0),
            "total_recovered": raw_data.get("recovered", 0),
            "recovered_daily_change": raw_data.get("recovered_diff", 0),
            "total_active": raw_data.get("active", 0),
            "active_daily_change": raw_data.get("active_diff", 0),
            "fatality_rate": raw_data.get("fatality_rate", 0.0),
            "country_iso": raw_data.get("iso", "MEX"),
            # Ya no se añade 'etl_execution_date' si no es dinámico
        }

        transformed_record["is_high_deaths_day"] = transformed_record["deaths_daily_change"] > 100
        transformed_record["active_to_confirmed_ratio"] = (
            transformed_record["total_active"] / transformed_record["total_confirmed"]
            if transformed_record["total_confirmed"] > 0
            else 0
        )
        transformed_record["recovery_rate"] = (
            transformed_record["total_recovered"] / transformed_record["total_confirmed"]
            if transformed_record["total_confirmed"] > 0
            else 0
        )

        transformed_record['transformation_timestamp'] = datetime.now().isoformat()

        print("Datos de COVID-19 transformados exitosamente.")

        ti.xcom_push(key="processed_covid_data_for_load", value=transformed_record)
        print(f"Registros transformados de COVID-19: 1")

        return transformed_record

    except Exception as e:
        print(f"Error inesperado durante la transformación: {e}")
        raise