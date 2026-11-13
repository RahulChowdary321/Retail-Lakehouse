.PHONY: install lint test gen-initial gen-incremental validate deploy docker-build docker-run
install:
	pip install -r requirements-dev.txt
lint:
	ruff check .
test:
	pytest -q
gen-initial:
	python data_generator/generate_retail_data.py --mode initial --date $$(date +%F) --out out
gen-incremental:
	python data_generator/generate_retail_data.py --mode incremental --date $$(date +%F) --out out
validate:
	databricks bundle validate -t dev
deploy:
	databricks bundle deploy -t dev
docker-build:
	docker build -t retail-genie-app:local app
docker-run:
	docker run --rm -p 8000:8000 --env-file .env retail-genie-app:local
