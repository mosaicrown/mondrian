.PHONY: all clean run adults usa2018 tests 2dtest

ROOT_DIR       := $(shell dirname $(realpath $(firstword $(MAKEFILE_LIST))))

SHELL          := /bin/bash
VENV           := $(ROOT_DIR)/venv
VIRTUALENV     := python3 -m venv
ACTIVATE       := $(VENV)/bin/activate
PYTHON         := $(VENV)/bin/python
PIP            := $(PYTHON) -m pip

REQUIREMENTS   := requirements.txt
APP            := app.py
MANUAL_TESTS   := manual_tests/manual_tests.py

DEMO	       := 0

all: run

$(VENV): $(ACTIVATE)

$(ACTIVATE): requirements.txt
	test -d $(VENV) || $(VIRTUALENV) $(VENV)
	$(PIP) install wheel
	$(PYTHON) setup.py bdist_wheel
	$(PIP) install --upgrade pip
	$(PIP) install -r $(REQUIREMENTS)
	@ touch $(ACTIVATE)

run adults:
	$(PYTHON) $(APP) adults.json $(DEMO)

usa2018: $(VENV)
	$(PYTHON) $(APP) usa2018.json $(DEMO)

clean:
	@ rm -rf $(VENV)

tests:
	$(PYTHON) $(MANUAL_TESTS)

2dtest:
	$(PYTHON) $(APP) "manual_tests/2dtest.json" $(DEMO)
