# 🌤️ Weather Data Extractor
[![CI](https://github.com/Castronela/data_extractor/actions/workflows/main.yml/badge.svg?branch=main)](https://github.com/Castronela/data_extractor/actions/workflows/main.yml) 

A simple, production-ready DataOps project that extracts weather data from the [Open-Meteo API](https://open-meteo.com/), transforms it, and saves it as CSV files. The project is scheduled to run daily using Apache Airflow in a Dockerized environment.


## 🚀 Project Goals

- Extract hourly temperature data for Berlin (latitude 52.52, longitude 13.41)
- Store results as timestamped CSV files in the `data/` directory
- Schedule automatic daily runs using Airflow
- Include logging, testing, and linting for reliability and maintainability


## 🧰 Tech Stack
 ![Language](https://img.shields.io/badge/Python-FFD43B?style=for-the-badge&logo=python&logoColor=blue)![Tech](https://img.shields.io/badge/Airflow-017CEE?style=for-the-badge&logo=Apache%20Airflow&logoColor=white)![Tech](https://img.shields.io/badge/Docker-2CA5E0?style=for-the-badge&logo=docker&logoColor=white)
- pandas, requests
- pytest for unit testing
- black + pylint for formatting/linting
- DAGs



## 🛠️ Setup & Usage

### 1. Clone the repo

```bash
git clone https://github.com/your-username/data-extractor.git
cd data-extractor
```

### 2. Install dependencies
```bash
make install
```

### 3. Run locally
```bash
make run
```

### 4. Logs & Output
- Output CSVs: data/weather_YYYYMMDD.csv
- Logs:
  - logs/log.log and logs/log.jsonl (main app)
  - logs/log_test.log (test logs)

### 5. Run with Docker & Airflow
```bash
make docker
```
Then visit the Airflow UI at http://localhost:8080
(Default username: admin, password: admin)  
Your DAG will be visible under weather_dag.


### 6. (Optional) Run tests (local dev)
```bash
make all
```
This will:
- Install dependencies
- Format code using black
- Lint using pylint
- Run tests with pytest  

Tests use unittest.mock to patch external API requests.




