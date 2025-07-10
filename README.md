# 🌤️ Weather Data Extractor, Uploader and Loader
[![CI](https://github.com/Castronela/data_extractor/actions/workflows/main.yml/badge.svg?branch=main)](https://github.com/Castronela/data_extractor/actions/workflows/main.yml) 

A simple, production-ready **DataOps pipeline** that extracts weather data from the [Open-Meteo API](https://open-meteo.com/), transforms it, saves it as CSV files, uploads them to **Azure Blob Storage**, and loads them into **Snowflake**. The project is scheduled to run daily using Apache Airflow in a Dockerized environment.


## 🚀 Project Goals

- Extract hourly temperature data for Berlin (latitude 52.52, longitude 13.41)
- Store results as timestamped CSV files in the `data/` directory
- Upload those CSVs to Azure Blob Storage
- Load new files into a Snowflake table using `COPY INTO`
- Schedule automatic daily runs using Airflow
- Use logging, unit tests, and code quality tools (black, pylint)


## 🧰 Tech Stack
![Python](https://img.shields.io/badge/Python-FFD43B?style=for-the-badge&logo=python&logoColor=blue)
![Airflow](https://img.shields.io/badge/Airflow-017CEE?style=for-the-badge&logo=Apache%20Airflow&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-2CA5E0?style=for-the-badge&logo=docker&logoColor=white)
![Azure](https://img.shields.io/badge/Azure%20Blob%20Storage-0078D4?style=for-the-badge&logo=microsoftazure&logoColor=white)
![Snowflake](https://img.shields.io/badge/Snowflake-29B5E8?style=for-the-badge&logo=snowflake&logoColor=white)
- `pandas`, `requests` for data extraction
- `azure-storage-blob` for cloud uploads
- `snowflake-connector-python` for database loading
- `pytest` and `unittest.mock` for testing
- `black`, `pylint` for formatting and linting

## 🔧 Prerequisites
Before running the project, make sure you have:

- A Python 3.10+ environment with pip installed (venv recommended)
- A Snowflake account (free trial is okay)
- An Azure Blob Storage container (with SAS token access)
- Docker & Docker Compose installed (for Airflow deployment)

### 🧾 Snowflake Setup Required  
Before running the DAG or Snowflake loader script, you must run the SQL commands found in `snowflake_prerequisite.txt`.  
Paste and execute them from your Snowflake Worksheet to:  
- Create the target weather_data table
- Create the external stage `azure_weather_stage` pointing to your Azure Blob container  
(Be sure to replace `<YOUR_STORAGE_ACCOUNT>`, `<YOUR_CONTAINER_ID>`, and `<YOUR_SAS_TOKEN>` accordingly)

## 🛠️ Setup & Usage

### 1. Clone the repo

```bash
git clone https://github.com/your-username/data-extractor.git
cd data-extractor
```

### 2. Install dependencies and create `.env` file
```bash
make install
```

### 3. Input Azure & Snowflake authentication credentials
Paste the following sensitive data into the `.env` file: 
#### Azure
- `Azure storage connection string`
- `Container id`
#### Snowflake
- `SNOWFLAKE_USER`
- `SNOWFLAKE_PASSWORD`
- `SNOWFLAKE_ACCOUNT`
- `SNOWFLAKE_WAREHOUSE`
- `SNOWFLAKE_DATABASE`
- `SNOWFLAKE_SCHEMA`

### 4. Run locally
```bash
make run
```

### 5. Logs & Output
- Output CSVs: `data/weather_YYYYMMDD.csv`
- Logs:
  - `logs/log.log` and `logs/log.jsonl` (main app)
  - `logs/log_test.log` (test logs)

### 6. Run with Docker & Airflow
```bash
make docker-up
```
Then visit the Airflow UI at http://localhost:8080
(Default username: `admin`, password: `admin`)  
Your DAG will be visible under `weather_dag`.


### 7. (Optional) Run tests (local dev)
```bash
make all
```
This will:
- Install dependencies
- Format code using black
- Lint using pylint
- Run tests with pytest  
