all: install format lint test

install:
	@pip install -U pip \
		&& pip install -r requirements.txt

run:
	@python src/extract_weather.py

test:
	@python -m pytest --rootdir=tests tests/*.py

format:
	@black . --extend-exclude=".myenv/"

lint:
	@pylint --disable=R,C,E0401,E0611,E1123,W0718 --ignore=.myenv .

docker:
	@cd docker \
	&& echo -n "AIRFLOW_UID=$$(id -u)" > .env \
	&& docker compose up -d --build

.PHONY: all install test format lint docker
