.PHONY: addlicense adults all artifact_experiments check_deps clean clean_test_files down local local-adults local-poker local-usa2018 local-usa1990 local-usa2019 poker run start stop ui up usa2018 usa2019 usa1990 transactions

.DEFAULT_GOAL  := all

SHELL          := /bin/bash
MAKE		   := make --no-print-directory

LICENSE_TYPE   := "apache"
LICENSE_HOLDER := "Unibg Seclab (https://seclab.unibg.it)"

REQUIRED_ARTIFACT_BINS := python3 pip3 zip gnuplot

addlicense:
	go install github.com/google/addlicense@latest
	$(shell go env GOPATH)/bin/addlicense -c $(LICENSE_HOLDER) -l $(LICENSE_TYPE) .

clean: | _clean_local _clean_distributed _clean_ui

check_deps:
	$(foreach bin,$(REQUIRED_ARTIFACT_BINS),\
		$(if $(shell which $(bin)),,$(error Please install `$(bin)`)))

# PERCOM experiments
artifact_experiments: | check_deps clean _artifact_experiments

_artifact_experiments: _extract_usa2018 clean_test_files
	cd percom_artifact_experiments; ./runtime_test.sh 20 | tee runtime.log; ./loss_test.sh 5 "5 10 20" "0.0001" | tee loss.log

clean_test_files: start
	@ $(MAKE) -C distributed clean_test_files

_extract_usa2018:
	@ cd percom_artifact_experiments; ./extract_usa2018.sh


# graphical user interface
ui:
	@ $(MAKE) -C ui

_clean_ui:
	@ $(MAKE) -C ui clean

# local
local local-adults:
	@ $(MAKE) -C local adults

local-usa1990:
	@ $(MAKE) -C local usa1990

local-usa2018:
	@ $(MAKE) -C local usa2018

local-usa2019:
	@ $(MAKE) -C local usa2019

local-poker:
	@ $(MAKE) -C local poker

_clean_local:
	@ $(MAKE) -C local clean

# distributed
all:
	@ $(MAKE) -C distributed run

start up:
	@ $(MAKE) -C distributed start

stop down:
	@ $(MAKE) -C distributed stop

run adults:
	@ $(MAKE) -C distributed adults

_run _adults:
	@ $(MAKE) -C distributed _adults

usa2018:
	@ $(MAKE) -C distributed usa2018

_usa2018:
	@ $(MAKE) -C distributed _usa2018

poker:
	@ $(MAKE) -C distributed poker

_poker:
	@ $(MAKE) -C distributed _poker

usa1990:
	@ $(MAKE) -C distributed usa1990

_usa1990:
	@ $(MAKE) -C distributed _usa1990

usa2019:
	@ $(MAKE) -C distributed usa2019

_usa2019:
	@ $(MAKE) -C distributed _usa2019

transactions:
	@ $(MAKE) -C distributed transactions

_transactions:
	@ $(MAKE) -C distributed _transactions

_clean_distributed:
	@ $(MAKE) -C distributed clean
