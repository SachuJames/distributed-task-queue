VENV := .venv
PY := $(VENV)/bin/python
RUFF := $(VENV)/bin/ruff
MYPY := $(VENV)/bin/mypy
PYTEST := $(VENV)/bin/pytest

.PHONY: install dev test lint format typecheck build integration-test docker-up docker-down demo load-test redis

install:
	python3 -m venv $(VENV)
	$(VENV)/bin/pip install -e .

dev: install
	$(VENV)/bin/pip install -e ".[test,lint]"

redis:
	redis-server --daemonize yes --save '' --appendonly no
	redis-cli ping

test:
	$(PYTEST) tests/unit -q

integration-test:
	$(PYTEST) tests/integration tests/concurrency -q

e2e-test:
	$(PYTEST) tests/e2e -q

lint:
	$(RUFF) check packages apps tests examples scripts

format:
	$(RUFF) format packages apps tests examples scripts

format-check:
	$(RUFF) format --check packages apps tests examples scripts

typecheck:
	$(MYPY) packages apps

build:
	$(PY) -m compileall -q packages apps

demo:
	bash scripts/demo.sh

load-test:
	$(PY) scripts/load_test.py --tasks 2000 --workers 4

docker-up:
	docker compose up --build -d

docker-down:
	docker compose down -v
