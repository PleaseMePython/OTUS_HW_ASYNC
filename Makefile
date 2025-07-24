setup:
	python -m venv .venv
	call .venv\Scripts\activate.bat
	python -m pip install -r requirements.txt
run:
	call .venv\Scripts\activate.bat
	python -m src.otus_hw_async.crawler
lint:
	python -m ruff check src
format:
	python -m ruff format src
