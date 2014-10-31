# user settings

DOWNLOAD_CACHE=downloads
ANACONDA_HOME=$(HOME)/anaconda

# guess OS (Linux, Darwin, ...)
OS_NAME := $(shell uname -s 2>/dev/null || echo "unknown")
CPU_ARCH := $(shell uname -m 2>/dev/null || uname -p 2>/dev/null || echo "unknown")

# choose anaconda installer depending on your OS
ANACONDA_URL = http://repo.continuum.io/miniconda
ifeq "$(OS_NAME)" "Linux"
FN := Miniconda-latest-Linux-x86_64.sh
else ifeq "$(OS_NAME)" "Darwin"
FN := Miniconda-3.7.0-MacOSX-x86_64.sh
else
FN := unknown
endif

# buildout files
BUILDOUT_FILES := parts eggs develop-eggs bin .installed.cfg .mr.developer.cfg *.egg-info bootstrap.py $(DOWNLOAD_CACHE)

# end of configuration

.DEFAULT_GOAL := help

.PHONY: help
help:
	@echo "make [target]\n"
	@echo "targets:\n"
	@echo "\thelp\t- Prints this help message."
	@echo "\tinfo\t- Prints information about your system."
	@echo "\tbuild\t- Builds application with buildout."
	@echo "\tclean\t- Deletes all files that are created by running buildout."
	@echo "\tdistclean\t- Removes *all* files that are not controlled by git."

.PHONY: info
info:
	@echo "Informations about your System:\n"
	@echo "\tOS_NAME \t= $(OS_NAME)"
	@echo "\tCPU_ARCH \t= $(CPU_ARCH)"
	@echo "\tAnaconda \t= $(FN)"

custom.cfg:
	@test -f custom.cfg || cp custom.cfg.example custom.cfg

.PHONY: init
init: custom.cfg
	@echo "Initializing ..."
	@test -d $(DOWNLOAD_CACHE) || mkdir -p $(DOWNLOAD_CACHE)

bootstrap.py:
	@test -f boostrap.py || wget -O bootstrap.py http://downloads.buildout.org/1/bootstrap.py

.PHONY: bootstrap
bootstrap: init anaconda bootstrap.py
	@echo "Bootstrap buildout ..."
	$(HOME)/anaconda/bin/python bootstrap.py -c custom.cfg

.PHONY: anaconda
anaconda:
	@echo "Installing Anaconda ..."
	@test -d $(ANACONDA_HOME) || wget -q -c -O "$(DOWNLOAD_CACHE)/$(FN)" $(ANACONDA_URL)/$(FN)
	@test -d $(ANACONDA_HOME) || bash "$(DOWNLOAD_CACHE)/$(FN)" -b -p $(ANACONDA_HOME)   

.PHONY: conda_pkgs
conda_pkgs: anaconda
	"$(ANACONDA_HOME)/bin/conda" install --yes pyopenssl

.PHONY: build
build: bootstrap conda_pkgs
	@echo "Building application with buildout ..."
	bin/buildout -c custom.cfg

.PHONY: clean
clean:
	@echo "Cleaning buildout files ..."
	@-for i in $(BUILDOUT_FILES); do \
            test -e $$i && rm -rf $$i; \
        done

.PHONY: backup
backup:
	@echo "Backup custom config ..." 
	@-test -f custom.cfg && mv -f custom.cfg custom.cfg.bak

.PHONY: distclean
distclean: backup
	@echo "Cleaning distribution ..."
	@-git clean -dfx --exclude *.bak

.PHONY: selfupdate
selfupdate:
	@echo "selfupdate"
	wget -q --no-check-certificate -O bootstrap.sh "https://raw.githubusercontent.com/bird-house/birdhousebuilder.bootstrap/master/bootstrap.sh"

.PHONY: docker
docker:
	@echo "Building docker image ..."
	docker build -t test .


