.PHONY: all clean download-transactions download-usa1990 download-usa2018 download-usa2019 run

ROOT_DIR       := $(shell dirname $(realpath $(firstword $(MAKEFILE_LIST))))

SHELL          := /bin/bash
MAKE		   := make --no-print-directory

VENV           := $(ROOT_DIR)/venv
VIRTUALENV     := python3 -m venv
ACTIVATE       := $(VENV)/bin/activate
PYTHON         := $(VENV)/bin/python
PIP            := $(PYTHON) -m pip

REQUIREMENTS   := requirements.txt

all: run

$(VENV): $(ACTIVATE)

$(ACTIVATE): $(REQUIREMENTS) $(PACKAGES)
	test -d $(VENV) || $(VIRTUALENV) $(VENV)
	$(PIP) install --upgrade pip
	$(PIP) install -r $(REQUIREMENTS)
	@ touch $(ACTIVATE)

run: download-usa1990 download-usa2018 download-usa2019

download-usa1990: $(VENV)
	@ ./download_usa1990.sh

download-usa2018: $(VENV)
	@ ./download_usa2018.sh

download-usa2019: $(VENV)
	@ ./download_usa2019.sh

download-transactions: $(VENV)
	@ $(PIP) install kaggle
	@ $(PIP) install kaggle --upgrade
	@ ./download_transactions.sh

clean:
	@ rm -rf $(VENV)
	@ find . -path '*/__pycache__/*' -delete
	@ find . -type d -name '__pycache__' -delete
