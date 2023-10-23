PYTHON = venv/bin/python3
PIP = venv/bin/pip

activate:
	python3 -m venv venv
	$(PIP) install -r requirements.txt

format:
	black .
	pylint *
	flake8

load_dvc:
	dvc remote modify data --local gdrive_service_account_json_file_path "dvc-local-config.json"
	cd data && dvc pull

.DEFAULT_GOAL := format