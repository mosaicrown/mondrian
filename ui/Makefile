.PHONY: all clean run

ROOT_DIR       := $(shell dirname $(realpath $(firstword $(MAKEFILE_LIST))))

SHELL          := /bin/bash
MAKE		   := make --no-print-directory

VENV           := $(ROOT_DIR)/venv
VIRTUALENV     := python3 -m venv
ACTIVATE       := $(VENV)/bin/activate
PYTHON         := $(VENV)/bin/python
PIP            := $(PYTHON) -m pip

REQUIREMENTS   := requirements.txt
APP            := app.py

all: run

$(VENV): $(ACTIVATE)

$(ACTIVATE): $(REQUIREMENTS) $(PACKAGES)
	test -d $(VENV) || $(VIRTUALENV) $(VENV)
	$(PIP) install --upgrade pip
	$(PIP) install -r $(REQUIREMENTS)
	@ touch $(ACTIVATE)

run: $(VENV)
	@ $(MAKE) -C ../local venv
	@ $(MAKE) -C ../distributed start
	@ echo -e "\n[*] Starting Web Server.\n"
	$(PYTHON) $(APP) && $(MAKE) -C ../distributed stop

clean:
	@ rm -rf $(VENV)
	@ find . -path '*/__pycache__/*' -delete
	@ find . -type d -name '__pycache__' -delete
