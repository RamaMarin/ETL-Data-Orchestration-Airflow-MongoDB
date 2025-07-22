from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago

# Importa las funciones para el pipeline de COVID-19
from ingestion_covid import extract_and_load_raw_covid_data
from transform_covid import transform_covid_data

# Importa las funciones para el pipeline de CoinMarketCap
from ingestion_coinmarketcap import extract_and_load_raw_coinmarketcap_data
from transform_coinmarketcap import transform_coinmarketcap_data

# Importa las funciones para el pipeline del Banco Mundial
from ingestion_worldbank import extract_and_load_raw_worldbank_data # <<-- NUEVA IMPORTACIÓN
from transform_worldbank import transform_worldbank_data           # <<-- NUEVA IMPORTACIÓN

# Importa la función de carga genérica a MongoDB
from load_mongo import load_processed_data_to_mongo

with DAG(
    dag_id='main_etl_pipeline',
    start_date=days_ago(1), 
    schedule_interval=None,
    catchup=False,
    tags=['etl', 'main', 'multi_api'],
    doc_md="""
    ### Main ETL Pipeline for Multiple APIs
    This DAG orchestrates the extraction, transformation, and loading of data
    from various public APIs (COVID-19, CoinMarketCap, World Bank) into MongoDB.
    """,
) as dag:

    # --- Tareas para el Pipeline de COVID-19 ---
    extract_and_load_raw_covid_task = PythonOperator(
        task_id='extract_and_load_raw_covid_data',
        python_callable=extract_and_load_raw_covid_data,
        provide_context=True,
    )

    transform_covid_task = PythonOperator(
        task_id='transform_covid_data',
        python_callable=transform_covid_data,
        provide_context=True,
    )

    load_processed_covid_task = PythonOperator(
        task_id='load_processed_covid_data',
        python_callable=load_processed_data_to_mongo,
        op_kwargs={
            'collection_name': 'processed_covid_data',
            'xcom_key': 'processed_covid_data_for_load',
            'xcom_task_id': 'transform_covid_data'
        },
        provide_context=True,
    )

    # Definición del flujo de tareas para COVID-19
    extract_and_load_raw_covid_task >> transform_covid_task >> load_processed_covid_task


    # --- Tareas para el Pipeline de CoinMarketCap ---
    extract_and_load_raw_coinmarketcap_task = PythonOperator(
        task_id='extract_and_load_raw_coinmarketcap_data',
        python_callable=extract_and_load_raw_coinmarketcap_data,
        provide_context=True,
    )

    transform_coinmarketcap_task = PythonOperator(
        task_id='transform_coinmarketcap_data',
        python_callable=transform_coinmarketcap_data,
        provide_context=True,
    )

    load_processed_coinmarketcap_task = PythonOperator(
        task_id='load_processed_coinmarketcap_data',
        python_callable=load_processed_data_to_mongo,
        op_kwargs={
            'collection_name': 'processed_coinmarketcap_data',
            'xcom_key': 'processed_coinmarketcap_data_for_load',
            'xcom_task_id': 'transform_coinmarketcap_data'
        },
        provide_context=True,
    )

    # Definición del flujo de tareas para CoinMarketCap
    extract_and_load_raw_coinmarketcap_task >> transform_coinmarketcap_task >> load_processed_coinmarketcap_task
    
    
    # --- Tareas para el Pipeline del Banco Mundial (Población) <<-- NUEVO PIPELINE -->>
    extract_and_load_raw_worldbank_task = PythonOperator(
        task_id='extract_and_load_raw_worldbank_data',
        python_callable=extract_and_load_raw_worldbank_data,
        provide_context=True,
    )

    transform_worldbank_task = PythonOperator(
        task_id='transform_worldbank_data',
        python_callable=transform_worldbank_data,
        provide_context=True,
    )

    load_processed_worldbank_task = PythonOperator(
        task_id='load_processed_worldbank_data',
        python_callable=load_processed_data_to_mongo,
        op_kwargs={
            'collection_name': 'processed_worldbank_population', # Colección específica para World Bank
            'xcom_key': 'processed_worldbank_data_for_load',    # Clave XCom de la transformación
            'xcom_task_id': 'transform_worldbank_data'          # ID de la tarea de transformación
        },
        provide_context=True,
    )

    # Definición del flujo de tareas para el Banco Mundial
    extract_and_load_raw_worldbank_task >> transform_worldbank_task >> load_processed_worldbank_task

    # --- Definir dependencias entre los pipelines (opcional) ---
    # Por defecto, al estar en el mismo DAG, Airflow los ejecutará cuando pueda.
    # Si quieres que se ejecuten en paralelo, no añades más dependencias.
    # Si quieres que uno espere a otro (ej. COVID termine antes que Cripto y Cripto antes que Banco Mundial):
    # load_processed_covid_task >> extract_and_load_raw_coinmarketcap_task
    # load_processed_coinmarketcap_task >> extract_and_load_raw_worldbank_task
    # Esto crearía una secuencia lineal de 9 tareas, cumpliendo el requisito de +5 tareas.
    # Si no pones más dependencias, Airflow puede ejecutar los 3 pipelines en paralelo.
    # Para cumplir el requisito de 5 tareas, ya tienes 9 si los dejas independientes así.