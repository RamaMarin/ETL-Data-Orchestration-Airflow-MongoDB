# 🚀 Massive Data Management - ETL Project (UPY)

This repository contains an end-to-end batch data pipeline built as a final project for the Massive Data Management course at Universidad Politécnica de Yucatán.

The solution leverages Apache Airflow for orchestration, ingests data from multiple public APIs, processes and stores it in MongoDB Atlas, and presents key insights through a web-based Streamlit dashboard. The entire infrastructure is containerized using Docker Compose.

---

## 🎯 Objective

To build a fully functional batch ETL pipeline, orchestrated using Apache Airflow, that ingests data from at least three different public APIs, processes and stores the data in MongoDB, and presents insights through a web-based Streamlit dashboard. The entire solution is containerized using Docker Compose.

---

## ✨ Key Features

* **Orchestration:** Automated ETL workflows using Apache Airflow.
* **Multi-API Data Ingestion:** Collects data from diverse public APIs.
* **Data Processing:** Implements robust data transformation logic (standardization, cleaning, enrichment).
* **Centralized Storage:** Stores both raw and processed data in MongoDB Atlas.
* **Interactive Dashboard:** Visualizes processed data insights with Streamlit.
* **Containerization:** All services are deployed using Docker Compose for isolated and reproducible environments.

---

## 📦 Project Components & Technologies

The solution exposes the following services for local development and testing:

| Component         | Technology       | Port     | Purpose                                                                   |
| :---------------- | :--------------- | :------- | :------------------------------------------------------------------------ |
| Orchestrator      | Apache Airflow   | 8080     | User interface for Directed Acyclic Graph (DAG) scheduling and monitoring |
| Metadata Database | PostgreSQL       | internal | Exclusively utilized by Airflow for metadata storage                      |
| Data Storage      | MongoDB Atlas    | 27017    | Repository for raw and processed project data                             |
| Dashboard         | Streamlit        | 8501     | Presents processed data through an interactive user interface             |

---

## 🌐 APIs Used

This project ingests data from three different public APIs, ensuring relevance and structured JSON responses:

1.  **COVID-19 Data (México)**
    * **Domain:** Public Health
    * **Source:** `covid-api.com`
    * **Endpoint Example:** `https://covid-api.com/api/reports/total?date=2023-03-09&iso=MEX`
    * **Relevance:** Tracks key COVID-19 metrics for Mexico, providing insights into public health trends.
    * **API Key:** Not required.

2.  **Cryptocurrency Latest Listings**
    * **Domain:** Economics and Finance / Technology Trends
    * **Source:** CoinMarketCap API
    * **Endpoint Example:** `https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest?start=1&limit=10&convert=USD`
    * **Relevance:** Provides real-time market data for leading cryptocurrencies, reflecting current financial and technological landscapes.
    * **API Key:** Required (your API Key is `0590c2a5-d251-4b21-859e-11767ca74abc`).

3.  **Population Data (México)**
    * **Domain:** Demographics / Social Trends
    * **Source:** World Bank Data API
    * **Endpoint Example:** `https://api.worldbank.org/v2/country/MEX/indicator/SP.POP.TOTL?format=json&date=2023`
    * **Relevance:** Provides total population figures, essential for contextualizing other data (e.g., per capita analysis).
    * **API Key:** Not required.

---

## ⚙️ Infrastructure Clarification

The solution extends a prebuilt Docker Compose setup. PostgreSQL is exclusively used by Airflow for internal metadata. MongoDB is added specifically for ingested pipeline data, and Streamlit for the dashboard.

---

## 🚀 Getting Started 

Follow these steps to set up and run the ETL pipeline locally.

### Prerequisites

* **Docker Desktop:** Ensure Docker Desktop is installed and running on your system.

### Folder Structure

Your project should be organized as follows:

```
project-etl/
├── dags/
│   ├── ingestion_covid.py          # Extracts and loads raw COVID data
│   ├── transform_covid.py          # Transforms COVID data
│   ├── ingestion_coinmarketcap.py  # Extracts and loads raw CoinMarketCap data
│   ├── transform_coinmarketcap.py  # Transforms CoinMarketCap data
│   ├── ingestion_worldbank.py      # Extracts and loads raw World Bank data
│   ├── transform_worldbank.py      # Transforms World Bank data
│   ├── load_mongo.py               # Generic function to load processed data to MongoDB
│   └── main_pipeline.py            # Main Airflow DAG definition
├── streamlit_app/
│   ├── app.py                      # Streamlit dashboard application
│   └── Dockerfile                  # Dockerfile for the Streamlit service
├── utils/                          # Shared helper functions (e.g., mongo_utils.py, api_helpers.py, transform_helpers.py - to be implemented)
├── Dockerfile                      # Dockerfile for Airflow services
├── docker-compose.yml              # Docker Compose configuration for all services
├── requirements.txt                # Python dependencies for Airflow and DAGs
└── README.md                       # Project documentation
```

### Setup and Launch

1.  **Navigate to Project Root:**
    Open your terminal and navigate to the `PROJECT-ETL/` directory.

2.  **Initialize Airflow Database (First Time Only):**
    This step sets up the PostgreSQL database that Airflow uses for its metadata.

    ```bash
    docker compose run airflow-webserver airflow db init
    ```

3.  **Create Airflow Admin User (First Time Only):**
    Set up an admin user to access the Airflow UI.

    ```bash
    docker compose run airflow-webserver airflow users create \
        --username admin \
        --firstname 'Rama' \
        --lastname 'Marin Orozco' \
        --role Admin \
        --email rama.marin@up.edu.mx \
        --password airflow
    ```
    Confirm with `y` if prompted.

4.  **Build and Launch All Services:**
    This command builds the Docker images (if not already built) and starts all defined services (Airflow Webserver, Scheduler, PostgreSQL, MongoDB, Streamlit) in detached mode.

    ```bash
    docker compose up -d --build
    ```
    Allow a few minutes for all services to start completely.

5.  **Verify Service Status:**

    ```bash
    docker compose ps
    ```
    All services should show `Up` in their `State` column.

---

## 📊 Using the Dashboard

1.  **Access Airflow UI:**
    Open your web browser and go to `http://localhost:8080`. Log in with your admin credentials (`admin` / `airflow`).

2.  **Activate the DAG:**
    In the Airflow UI, find the `main_etl_pipeline` DAG. Toggle its switch to "On" (blue).

3.  **Trigger the ETL Pipeline:**
    Click the "Trigger DAG" (play button) next to `main_etl_pipeline`. This will start an execution of all ETL tasks for COVID-19, CoinMarketCap, and World Bank data.

4.  **Monitor DAG Execution:**
    Click on the `main_etl_pipeline` DAG name and go to the "Graph View" to monitor the status of each task (green for success, red for failure). Check task logs if any task fails.

5.  **Access Streamlit Dashboard:**
    Once the ETL pipeline has completed successfully (all tasks are green), open a new browser tab and go to `http://localhost:8501`.

6.  **Explore Data:**
    The Streamlit dashboard will display the processed data from all integrated APIs, including metrics, tables, and visualizations.

---

## Clean Up

To stop and remove all Docker containers and networks:

```bash
docker compose down
```

To also remove the volumes (which will delete all your MongoDB data and Airflow metadata):

```bash
docker compose down -v

