# 🌤️ Weather Data Extractor & Uploader
[![CI](https://github.com/Castronela/data_extractor/actions/workflows/main.yml/badge.svg?branch=main)](https://github.com/Castronela/data_extractor/actions/workflows/main.yml) 

A simple, production-ready **DataOps pipeline** that extracts weather data from the [Open-Meteo API](https://open-meteo.com/), transforms it, saves it as CSV files, and uploads them to **Azure Blob Storage**. The project is scheduled to run daily using Apache Airflow in a Dockerized environment.


## 🚀 Project Goals

- Extract hourly temperature data for Berlin (latitude 52.52, longitude 13.41)
- Store results as timestamped CSV files in the `data/` directory
- Upload those CSVs to Azure Blob Storage
- Schedule automatic daily runs using Airflow
- Use logging, unit tests, and code quality tools (black, pylint)


## 🧰 Tech Stack
![Python](https://img.shields.io/badge/Python-FFD43B?style=for-the-badge&logo=python&logoColor=blue)
![Airflow](https://img.shields.io/badge/Airflow-017CEE?style=for-the-badge&logo=Apache%20Airflow&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-2CA5E0?style=for-the-badge&logo=docker&logoColor=white)
![Azure](https://img.shields.io/badge/Azure%20Blob%20Storage-0078D4?style=for-the-badge&logo=microsoftazure&logoColor=white)
- `pandas`, `requests` for data extraction
- `azure-storage-blob` for cloud uploads
- `pytest` and `unittest.mock` for testing
- `black`, `pylint` for formatting and linting


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

### 3. Input Azure authentication data
Paste the following sensitive data into the `.env` file: 
- `Azure storage connection string`
- `Container id`

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








