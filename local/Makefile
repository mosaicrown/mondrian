.PHONY: all clean run adults usa2018 tests 2dtest

ROOT_DIR       := $(shell dirname $(realpath $(firstword $(MAKEFILE_LIST))))

SHELL          := /bin/bash
VENV           := $(ROOT_DIR)/venv
VIRTUALENV     := python3 -m venv
ACTIVATE       := $(VENV)/bin/activate
PYTHON         := $(VENV)/bin/python
PIP            := $(PYTHON) -m pip

PACKAGES       := mondrian
REQUIREMENTS   := requirements.txt
APP            := anonymize.py
MANUAL_TESTS   := test/generalization_tests.py

DEMO	       := 0

all: run

$(VENV): $(ACTIVATE)

$(ACTIVATE): $(REQUIREMENTS) $(PACKAGES)
	test -d $(VENV) || $(VIRTUALENV) $(VENV)
	$(PIP) install --upgrade pip
	$(PIP) install -r $(REQUIREMENTS)
	@ touch $(ACTIVATE)

run adults: $(VENV)
	$(PYTHON) $(APP) config/adults.json $(DEMO)

usa2018: $(VENV)
	$(PYTHON) $(APP) config/usa2018.json $(DEMO)

clean:
	@ rm -rf $(VENV)
	@ find . -path '*/__pycache__/*' -delete
	@ find . -type d -name '__pycache__' -delete

tests: $(VENV)
	$(PYTHON) $(MANUAL_TESTS)

2dtest: $(VENV)
	$(PYTHON) $(APP) config/2dtest.json $(DEMO)
