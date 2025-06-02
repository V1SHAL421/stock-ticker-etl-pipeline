clean:
	rm -rf __pycache__
	rm -rf .venv

install:
	pip-compile requirements.in
	pip install -r requirements.txt