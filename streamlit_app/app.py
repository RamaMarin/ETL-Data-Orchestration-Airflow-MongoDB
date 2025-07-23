import streamlit as st
import pymongo
import pandas as pd
from datetime import datetime
import plotly.express as px
from mongo_utils import get_mongo_client # <<-- AÑADIDO: Importar la función de conexión

# ELIMINAR ESTA LÍNEA:
# MONGO_CONNECTION_STRING = "mongodb+srv://rama_marin:Peta2017@bigdataupy5b.wk9joeh.mongodb.net/?retryWrites=true&w=majority&appName=BigDataUpy5B"

# --- Nombres de DB y Colecciones para COVID-19 ---
COVID_DATABASE_NAME = "covid_db"
COVID_PROCESSED_COLLECTION = "processed_covid_data"

# --- Nombres de DB y Colecciones para Criptomonedas ---
CRYPTO_DATABASE_NAME = "crypto_data"
CRYPTO_PROCESSED_COLLECTION = "processed_coinmarketcap_data"

# --- Nombres de DB y Colecciones para Población (Banco Mundial) ---
POPULATION_DATABASE_NAME = "population_data"
POPULATION_PROCESSED_COLLECTION = "processed_worldbank_population"


st.set_page_config(layout="wide", page_title="Dashboard de Datos ETL")

st.title("🚀 Data Dashboard - ETL Project")

st.write(
    """
    This dashboard shows the data processed by your Airflow ETL pipeline from various APIs.
    """
)

# --- Conexión y Carga de Datos Procesados de COVID-19 ---
@st.cache_data(ttl=600) # Cachea los datos por 600 segundos (10 minutos)
def get_processed_covid_data():
    client = None
    try:
        # CAMBIAR ESTA LÍNEA:
        # client = pymongo.MongoClient(MONGO_CONNECTION_STRING)
        client = get_mongo_client() # <<-- CAMBIADO: Usar la función centralizada
        db = client[COVID_DATABASE_NAME]
        collection = db[COVID_PROCESSED_COLLECTION]
        
        data = list(collection.find({}))
        
        if not data:
            st.warning(f"No se encontraron documentos en '{COVID_DATABASE_NAME}.{COVID_PROCESSED_COLLECTION}'. Asegúrate de que tus DAGs de COVID se hayan ejecutado.")
            return pd.DataFrame() 
            
        df = pd.DataFrame(data)
        df['date'] = pd.to_datetime(df['date'])
        df = df.sort_values(by='date').reset_index(drop=True)
        return df
    except pymongo.errors.ConnectionFailure as e:
        # CAMBIAR MENSAJE DE ERROR:
        # st.error(f"Error de conexión a MongoDB Atlas para datos de COVID: {e}.")
        st.error(f"Error de conexión a MongoDB local para datos de COVID: {e}.") # <<-- CAMBIADO
        return pd.DataFrame()
    except Exception as e:
        st.error(f"Error al cargar datos de COVID desde MongoDB: {e}")
        return pd.DataFrame()
    finally:
        if client:
            client.close()

# --- Conexión y Carga de Datos Procesados de Criptomonedas ---
@st.cache_data(ttl=600)
def get_processed_crypto_data():
    client = None
    try:
        # CAMBIAR ESTA LÍNEA:
        # client = pymongo.MongoClient(MONGO_CONNECTION_STRING)
        client = get_mongo_client() # <<-- CAMBIADO: Usar la función centralizada
        db = client[CRYPTO_DATABASE_NAME]
        collection = db[CRYPTO_PROCESSED_COLLECTION]
        
        data = list(collection.find({}))
        
        if not data:
            st.warning(f"No se encontraron documentos en '{CRYPTO_DATABASE_NAME}.{CRYPTO_PROCESSED_COLLECTION}'. Asegúrate de que tus DAGs de CoinMarketCap se hayan ejecutado.")
            return pd.DataFrame()
            
        df = pd.DataFrame(data)
        
        if '_id' in df.columns:
            df = df.drop(columns=['_id'])
            
        return df
    except pymongo.errors.ConnectionFailure as e:
        # CAMBIAR MENSAJE DE ERROR:
        # st.error(f"Error de conexión a MongoDB Atlas para datos de cripto: {e}.")
        st.error(f"Error de conexión a MongoDB local para datos de cripto: {e}.") # <<-- CAMBIADO
        return pd.DataFrame()
    except Exception as e:
        st.error(f"Error al cargar datos de cripto desde MongoDB: {e}")
        return pd.DataFrame()
    finally:
        if client:
            client.close()

# --- Conexión y Carga de Datos Procesados de Población (Banco Mundial) ---
@st.cache_data(ttl=600)
def get_processed_population_data():
    client = None
    try:
        # CAMBIAR ESTA LÍNEA:
        # client = pymongo.MongoClient(MONGO_CONNECTION_STRING)
        client = get_mongo_client() # <<-- CAMBIADO: Usar la función centralizada
        db = client[POPULATION_DATABASE_NAME]
        collection = db[POPULATION_PROCESSED_COLLECTION]
        
        data = list(collection.find({}))
        
        if not data:
            st.warning(f"No se encontraron documentos en '{POPULATION_DATABASE_NAME}.{POPULATION_PROCESSED_COLLECTION}'. Asegúrate de que tus DAGs del Banco Mundial se hayan ejecutado.")
            return pd.DataFrame()
            
        df = pd.DataFrame(data)
        
        if '_id' in df.columns:
            df = df.drop(columns=['_id'])
        
        # Asumimos que queremos la población más reciente si hay varias entradas
        df = df.sort_values(by='year', ascending=False).reset_index(drop=True)
        return df
    except pymongo.errors.ConnectionFailure as e:
        # CAMBIAR MENSAJE DE ERROR:
        # st.error(f"Error de conexión a MongoDB Atlas para datos de población: {e}.")
        st.error(f"Error de conexión a MongoDB local para datos de población: {e}.") # <<-- CAMBIADO
        return pd.DataFrame()
    except Exception as e:
        st.error(f"Error al cargar datos de población desde MongoDB: {e}")
        return pd.DataFrame()
    finally:
        if client:
            client.close()


# Cargar los datos de todas las fuentes
df_covid = get_processed_covid_data()
df_crypto = get_processed_crypto_data()
df_population = get_processed_population_data() # <<-- Cargar datos de población

st.write("---")

# --- Sección de Población ---
if not df_population.empty:
    st.subheader("🌎 Population Data (Mexico)")
    current_population_data = df_population.iloc[0] # Tomar el dato más reciente (ya ordenado)
    
    st.metric(
        label=f"Total population in {current_population_data['year']}",
        value=f"{current_population_data['population_total']:,}"
    )
    st.info(f"Source: World Bank. Data for the year {current_population_data['year']} for Mexico.")
else:
    st.info("No Population data available to display. Run your World Bank DAG.")

st.write("---") # Separador


# --- Sección de Dashboard COVID-19 ---
if not df_covid.empty:
    st.subheader("📊 COVID-19 Data (Mexico - 09 March 2023)")
    current_covid_data = df_covid.iloc[0] 
    
    st.markdown(f"**Report date:** {current_covid_data['date'].strftime('%Y-%m-%d')}")

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Confirmed Totals", f"{current_covid_data['total_confirmed']:,}")
    col2.metric("Total fatalities", f"{current_covid_data['total_deaths']:,}")
    col3.metric("Activos Totales", f"{current_covid_data['total_active']:,}")
    col4.metric("Fatality Rate", f"{current_covid_data['fatality_rate']:.2%}")

    st.markdown("##### Ratio of Confirmed Cases to Deaths")
    total_confirmed_value = current_covid_data['total_confirmed']
    total_deaths_value = current_covid_data['total_deaths']
    other_confirmed_value = total_confirmed_value - total_deaths_value

    pie_data = pd.DataFrame({
        'Category': ['Deceased', 'Non-Deceased'],
        'Value': [total_deaths_value, other_confirmed_value]
    })
    fig_pie_deaths_confirmed = px.pie(pie_data, values='Value', names='Category',
                                        title=f'Confirmed Cases vs. Deaths at {current_covid_data["date"].strftime("%Y-%m-%d")}',
                                        hole=0.4,
                                        color_discrete_map={'Deceased':"#EF553B", 'Confirmed Cases (Non-Deceased)':"#636EFA"})
    st.plotly_chart(fig_pie_deaths_confirmed, use_container_width=True)
else:
    st.info("No hay datos de COVID-19 disponibles para mostrar.")

st.write("---")

# --- Sección de Criptomonedas ---
if not df_crypto.empty:
    st.subheader("💰 Top 10 Cryptocurrencies (CoinMarketCap)")
    
    # Tabla de las 10 criptos
    st.markdown("##### General list")
    st.dataframe(df_crypto[['name', 'symbol', 'rank']], hide_index=True)
    
    # --- Gráfico 1: Capitalización de Mercado por Criptomoneda (Bar Chart) ---
    st.markdown("##### Market Capitalization per Cryptocurrency")
    fig_market_cap = px.bar(df_crypto.head(10), # Tomamos las top 10 si el DataFrame tiene más
                           x='name',
                           y='market_cap_billions_usd',
                           title='Top 10 Cryptocurrencies by Market Capitalization (in Billion USD)',
                           labels={'name': 'Cryptocurrency', 'market_cap_billions_usd': 'Market Capitalization (Billion USD)'},
                           color='name', # Color por nombre de la cripto
                           hover_data={'quote_usd_market_cap': ':,', 'rank': True}) # Mostrar valor exacto en hover
    st.plotly_chart(fig_market_cap, use_container_width=True)


    # --- Gráfico 2: Distribución de Capitalización de Mercado (Gráfico de Pastel con Filtro) ---
    st.markdown("##### Market Capitalization Distribution by Cryptocurrency")

    selected_cryptos = st.multiselect(
        "Select Cryptocurrencies for pie chart",
        options=df_crypto['name'].tolist(),
        default=df_crypto['name'].tolist()
    )

    if selected_cryptos:
        df_filtered_pie = df_crypto[df_crypto['name'].isin(selected_cryptos)]

        fig_market_cap_pie = px.pie(df_filtered_pie,
                                    values='quote_usd_market_cap',
                                    names='name',
                                    title='Distribución de Capitalización de Mercado (USD)',
                                    hole=0.4,
                                    labels={'quote_usd_market_cap': 'Capitalización de Mercado'})
        
        fig_market_cap_pie.update_traces(textinfo='percent+label', pull=[0.05]*len(df_filtered_pie))

        st.plotly_chart(fig_market_cap_pie, use_container_width=True)
    else:
        st.info("Select at least one cryptocurrency to see the capitalization distribution.")

    st.info(f"Última actualización de datos de criptomonedas: **{df_crypto['last_updated_utc'].max() if 'last_updated_utc' in df_crypto.columns else 'N/A'}**")
else:
    st.info("No hay datos de Criptomonedas disponibles para mostrar. Ejecuta tu DAG de CoinMarketCap.")


st.write("---")
st.subheader("💡 Project Notes:")
st.markdown(
    """
    * This dashboard visualizes the data extracted, transformed and loaded by the Airflow ETL pipeline.
    * Data is stored in MongoDB Atlas in `raw_<source>` and `processed_<source>` collections.
    * Each execution of the DAG ensures the idempotency of the data in MongoDB.
    """
)