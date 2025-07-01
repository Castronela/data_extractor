all: install format lint test

install:
	pip install -U pip && \
		pip install -r requirements.txt

test:
	python -m pytest --rootdir=tests tests/*.py

format:
	black . --extend-exclude=".myenv/"

lint:
	pylint --disable=R,C --ignore=.myenv .

