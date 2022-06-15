install:
	pip install --upgrade pip &&\
		pip install -r requirements.txt

# test:
# 	python -m pytest -vv test_hello.py

format:
	black *.py

lint:
	#pylint --disable=R,C hello.py flask_app.py
	#, tsfm_prophet.py, arfm_prod.py,
	#lint Dockerfile
	#docker run --rm -i hadolint/hadolint < Dockerfile
	
all: install lint test format