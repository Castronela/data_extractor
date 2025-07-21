define DOTENV
AZURE_STORAGE_CONNECTION_STRING=
AZURE_BLOB_CONTAINER_ID=
SNOWFLAKE_USER=
SNOWFLAKE_PASSWORD=
SNOWFLAKE_ACCOUNT=
SNOWFLAKE_WAREHOUSE=
SNOWFLAKE_DATABASE=
SNOWFLAKE_SCHEMA=
endef

export DOTENV

all: install format lint test

install:
	@pip install -U pip \
		&& pip install -r requirements.txt
	@if [ ! -s .env ]; then \
		echo "$$DOTENV"  > .env; \
	fi
	@if [ ! -s ./docker/.env ]; then \
		echo -e "AIRFLOW_UID=$$(id -u)\nAIRFLOW_WEBHOOK_TOKEN="  > ./docker/.env; \
	fi

run:
	@python -m src.extract_weather \
		&& python -m src.transform_weather \
		&& python -m src.blob_runner \
		&& python -m src.load_to_snowflake

test:
	@python -m pytest --rootdir=tests tests/*.py

format:
	@black . --extend-exclude=".myenv/"

lint:
	@pylint --disable=R,C,E0401,E0611,E1123,W0718,W0104 --ignore=.myenv .

docker-up:
	@cd docker \
		&& docker compose up -d --build

docker-down:
	@cd docker \
		&& docker compose down

.PHONY: all install test format lint docker
