
DOWNLOAD_CACHE=downloads

ANACONDA_HOME=$(HOME)/anaconda
ANACONDA_URL = http://repo.continuum.io/miniconda
FN_LINUX = Miniconda-latest-Linux-x86_64.sh
FN_OSX = Miniconda-3.7.0-MacOSX-x86_64.sh
FN = $(FN_LINUX)

.PHONY: help
help:
	@echo "make [target]\n"
	@echo "targets:\n"
	@echo "help \t- prints this help message"
	@echo "build \t- builds application with buildout"
	@echo "clean \t- removes all files that are not controlled by git"

custom.cfg:
	@-cp custom.cfg.example custom.cfg

.PHONY: init
init: custom.cfg
	@echo "Initializing ..."
	mkdir -p $(DOWNLOAD_CACHE)

.PHONY: bootstrap
bootstrap: init
	@echo "Bootstrap buildout ..."
	python bootstrap.py -c custom.cfg

.PHONY: anaconda
anaconda:
	@echo "Installing Anaconda ..."
	@echo $(FN)
	wget -q -c -O "$(DOWNLOAD_CACHE)/$(FN)" $(ANACONDA_URL)/$(FN)
	@-bash "$(DOWNLOAD_CACHE)/$(FN)" -b -p $(ANACONDA_HOME)   

.PHONY: build
build: bootstrap anaconda
	@echo "Building ..."
	bin/buildout -c custom.cfg

.PHONY: clean
clean:
	@echo "Cleaning ..."
	@echo "Removing custom.cfg ... backup is custom.cfg.bak"
	@-mv -f custom.cfg custom.cfg.bak
	@-git clean -dfx --exclude custom.cfg.bak

.PHONY: selfupdate
selfupdate:
	@echo "selfupdate"


