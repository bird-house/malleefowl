
DOWNLOAD_CACHE=downloads

ANACONDA_HOME=$(HOME)/anaconda
ANACONDA_URL = http://repo.continuum.io/miniconda
FN_LINUX = Miniconda-latest-Linux-x86_64.sh
FN_OSX = Miniconda-3.7.0-MacOSX-x86_64.sh
FN = $(FN_LINUX)

.PHONY: help init bootstrap anaconda conda_pkgs build clean selfupdate docker
help:
	@echo "make [target]\n"
	@echo "targets:\n"
	@echo "help \t- prints this help message"
	@echo "build \t- builds application with buildout"
	@echo "clean \t- removes all files that are not controlled by git"

custom.cfg:
	@test -f custom.cfg || cp custom.cfg.example custom.cfg

init: custom.cfg
	@echo "Initializing ..."
	@test -d $(DOWNLOAD_CACHE) || mkdir -p $(DOWNLOAD_CACHE)

bootstrap.py:
	@test -f boostrap.py || wget -O bootstrap.py http://downloads.buildout.org/1/bootstrap.py

bootstrap: init anaconda bootstrap.py
	@echo "Bootstrap buildout ..."
	$(HOME)/anaconda/bin/python bootstrap.py -c custom.cfg

anaconda:
	@echo "Installing Anaconda ..."
	@test -d $(ANACONDA_HOME) || wget -q -c -O "$(DOWNLOAD_CACHE)/$(FN)" $(ANACONDA_URL)/$(FN)
	@test -d $(ANACONDA_HOME) || bash "$(DOWNLOAD_CACHE)/$(FN)" -b -p $(ANACONDA_HOME)   

conda_pkgs: anaconda
	"$(ANACONDA_HOME)/bin/conda" install --yes pyopenssl

build: bootstrap conda_pkgs
	@echo "Building application with buildout ..."
	bin/buildout -c custom.cfg

clean:
	@echo "Cleaning ..."
	@echo "Removing custom.cfg ... backup is custom.cfg.bak"
	@-mv -f custom.cfg custom.cfg.bak
	@-git clean -dfx --exclude custom.cfg.bak

selfupdate:
	@echo "selfupdate"
	wget -q --no-check-certificate -O bootstrap.sh "https://raw.githubusercontent.com/bird-house/birdhousebuilder.bootstrap/master/bootstrap.sh"

docker:
	@echo "Building docker image ..."
	docker build -t test .


