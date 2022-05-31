.PHONY: all clean run adults usa2018 tests 2dtest usa1990

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
OUTPUT_DIR	   := anonymized

DEMO	       := 0

all: run

$(VENV) venv: $(ACTIVATE)

$(ACTIVATE): $(REQUIREMENTS) $(PACKAGES)
	test -d $(VENV) || $(VIRTUALENV) $(VENV)
	$(PIP) install --upgrade pip
	$(PIP) install -r $(REQUIREMENTS)
	@ touch $(ACTIVATE)

$(OUTPUT_DIR):
	mkdir -p $(OUTPUT_DIR)

run adults: $(VENV) $(OUTPUT_DIR)
	$(PYTHON) $(APP) config/adults.json $(DEMO)

usa2018: $(VENV) $(OUTPUT_DIR)
	$(PYTHON) $(APP) config/usa2018.json $(DEMO)

usa2019: $(VENV) $(OUTPUT_DIR)
	$(PYTHON) $(APP) config/usa2019.json $(DEMO)

poker: $(VENV) $(OUTPUT_DIR)
	$(PYTHON) $(APP) config/poker.json $(DEMO)

clean:
	@ rm -rf $(VENV)
	@ find . -path '*/__pycache__/*' -delete
	@ find . -type d -name '__pycache__' -delete

tests: $(VENV)
	$(PYTHON) $(MANUAL_TESTS)

2dtest: $(VENV) $(OUTPUT_DIR)
	$(PYTHON) $(APP) config/2dtest.json $(DEMO)
