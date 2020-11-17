.PHONY: all addlicense clean start stop up down run _run shell _shell web _web local local-adults adults _adults local-usa2018 usa2018 _usa2018

SHELL          := /bin/bash

LICENSE_TYPE   := "apache"
LICENSE_HOLDER := "Unibg Seclab (https://seclab.unibg.it)"

addlicense:
	go get -u github.com/google/addlicense
	$(shell go env GOPATH)/bin/addlicense -c $(LICENSE_HOLDER) -l $(LICENSE_TYPE) .

clean: | _clean_local _clean_docker

# local
local local-adults:
	make -C local adults

local-usa2018:
	make -C local usa2018

_clean_local:
	make -C local clean

# distributed
all:
	make -C distributed run

start up: 
	make -C distributed start

stop down:
	make -C distributed stop

shell:
	make -C distributed shell

_shell: 
	make -C distributed _shell

web:
	make -C distributed web

_web:
	make -C distributed _web

run adults:
	make -C distributed adults

_run _adults:
	make -C distributed _adults

usa2018:
	make -C distributed usa2018

_usa2018:
	make -C distributed _usa2018

_clean_docker:
	make -C distributed clean
