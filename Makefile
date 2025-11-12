.PHONY: install run test lint docker-up docker-down clean

install:
	pip install -r requirements.txt

run:
	python -m src.main

test:
	python -m pytest tests/ -v

lint:
	python -m py_compile src/config.py
	python -m py_compile src/models.py

docker-up:
	docker compose up -d --build

docker-down:
	docker compose down -v

clean:
	rm -rf output/ __pycache__ src/__pycache__ .pytest_cache
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
