.PHONY: all addlicense clean start stop up down ui run _run shell _shell notebook _notebook local local-adults adults _adults local-usa2018 usa2018 _usa2018

.DEFAULT_GOAL  := all

SHELL          := /bin/bash
MAKE		   := make --no-print-directory

LICENSE_TYPE   := "apache"
LICENSE_HOLDER := "Unibg Seclab (https://seclab.unibg.it)"

addlicense:
	go get -u github.com/google/addlicense
	$(shell go env GOPATH)/bin/addlicense -c $(LICENSE_HOLDER) -l $(LICENSE_TYPE) .

clean: | _clean_local _clean_docker _clean_ui

# graphical user interface
ui:
	@ $(MAKE) -C ui

_clean_ui:
	@ $(MAKE) -C ui clean

# local
local local-adults:
	@ $(MAKE) -C local adults

local-usa2018:
	@ $(MAKE) -C local usa2018

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

shell:
	@ $(MAKE) -C distributed shell

_shell: 
	@ $(MAKE) -C distributed _shell

notebook:
	@ $(MAKE) -C distributed notebook

_notebook:
	@ $(MAKE) -C distributed _notebook

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

_clean_docker:
	@ $(MAKE) -C distributed clean
