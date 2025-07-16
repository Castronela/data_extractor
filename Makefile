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
		&& pip install -r requirements.txt \
		&& if [ ! -s .env ]; then \
			echo "$$DOTENV"  > .env; \
		fi

run:
	@python src/extract_weather.py \
		&& python src/transform_weather.py \
		&& python src/blob_runner.py \
		&& python src/load_to_snowflake.py

test:
	@python -m pytest --rootdir=tests tests/*.py

format:
	@black . --extend-exclude=".myenv/"

lint:
	@pylint --disable=R,C,E0401,E0611,E1123,W0718,W0104 --ignore=.myenv .

docker-up:
	@cd docker \
		&& echo -n "AIRFLOW_UID=$$(id -u)" > .env \
		&& docker compose up -d --build

docker-down:
	@cd docker \
		&& docker compose down

.PHONY: all install test format lint docker
