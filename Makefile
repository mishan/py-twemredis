init:
	pip install -r requirements.txt

init3:
	pip3 install -r requirements.txt

test: pep8
	python -m unittest discover tests "*_test.py"

test3: pep8
	python3 -m unittest discover tests "*_test.py"

pep8:
	pep8 *.py tests/*.py

install: test
	pip install .

install3: test3
	pip3 install .
