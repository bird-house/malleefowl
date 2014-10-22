#!/bin/bash -
#===============================================================================
# vim: softtabstop=4 shiftwidth=4 expandtab fenc=utf-8 spell spelllang=en cc=120
#===============================================================================
#
#          FILE: install.sh
#
#   DESCRIPTION: Bootstrap birdhouse installation
#
#===============================================================================
set -o nounset                              # Treat unset variables as an error

# user settings
ANACONDA_HOME=$HOME/anaconda

# don't change these settings
BUILDOUT_DIR=`dirname $0`
DOWNLOAD_CACHE=$BUILDOUT_DIR/downloads
ANACONDA_URL=http://repo.continuum.io/miniconda
FN_LINUX=Miniconda-latest-Linux-x86_64.sh
FN_OSX=Miniconda-3.5.5-MacOSX-x86_64.sh

function install_anaconda() {
    # run miniconda setup, install in ANACONDA_HOME
    if [ -d $ANACONDA_HOME ]; then
        echo "Anaconda already installed in $ANACONDA_HOME."
    else
        FN=$FN_LINUX
        if [ `uname -s` = "Darwin" ] ; then
            FN=$FN_OSX
        fi

        echo "Installing $FN ..."

        # download miniconda setup script to download cache with wget
        mkdir -p $DOWNLOAD_CACHE
        wget -q -c -O "$DOWNLOAD_CACHE/$FN" $ANACONDA_URL/$FN
        bash "$DOWNLOAD_CACHE/$FN" -b -p $ANACONDA_HOME   
    fi

    # add anaconda path to user .bashrc
    echo -n "Add \"$ANACONDA_HOME/bin\" to your PATH: "
    echo "\"export PATH=$ANACONDA_HOME/bin:\$PATH\""

    echo "Installing Anaconda ... Done"
}

# set default configurion file for buildout
function setup_cfg() {
    if [ ! -d $DOWNLOAD_CACHE ]; then
        echo "Creating buildout downloads cache $DOWNLOAD_CACHE."
        mkdir -p $DOWNLOAD_CACHE
    fi

    if [ ! -f custom.cfg ]; then
        echo "Copy default configuration to $BUILDOUT_DIR/custom.cfg"
        cp custom.cfg.example custom.cfg
    else
        echo "Using custom configuration $BUILDOUT_DIR/custom.cfg"
    fi
}

# install conda dependencies
function install_deps() {
    "$ANACONDA_HOME/bin/conda" install --yes pyopenssl
}

# run install
function install() {
    echo "Installing ..."
    echo "BUILDOUT_DIR=$BUILDOUT_DIR"
    echo "DOWNLOAD_CACHE=$DOWNLOAD_CACHE"

    pushd $BUILDOUT_DIR || exit 1
    
    setup_cfg

    if [ ! -f "$BUILDOUT_DIR/bin/buildout" ]; then
        "$ANACONDA_HOME/bin/python" bootstrap.py -c custom.cfg
        echo "Bootstrap ... Done"
    fi

    "$BUILDOUT_DIR/bin/buildout" -c custom.cfg

    popd || exit 1

    echo "Installing ... Done"
}

function clean() {
    echo "Cleaning buildout ..."
    if [ -f custom.cfg ]; then
        echo "removing custom.cfg ... backup is custom.cfg.bak"
        mv -f custom.cfg custom.cfg.bak
    fi
    rm -rf $BUILDOUT_DIR/downloads
    rm -rf $BUILDOUT_DIR/eggs
    rm -rf $BUILDOUT_DIR/develop-eggs
    rm -rf $BUILDOUT_DIR/parts
    rm -rf $BUILDOUT_DIR/bin
    rm -f $BUILDOUT_DIR/.installed.cfg
    rm -rf $BUILDOUT_DIR/*.egg-info
    rm -rf $BUILDOUT_DIR/dist
    rm -rf $BUILDOUT_DIR/build
    echo "Cleaning buildout ... Done"
}

function usage() {
    cat << EOT

  Usage : $0 [command]

  Commands:
    - selfupdate
    - bootstrap
    - install
    - clean

  Examples:
    - $0
    - $0 bootstrap
    - $0 install
    - $0 clean
EOT
    exit 1
}

# Define command
if [ "$#" -eq 0 ];then
    COMMAND="install"
else
    COMMAND=$1
fi

# Check command
if [ "$(echo "$COMMAND" | egrep '(install|clean|bootstrap|selfupdate)')" = "" ]; then
    usage
    exit 1
fi

# Check for any unparsed arguments. Should be an error.
if [ "$#" -gt 1 ]; then
    usage
    echo
    echo "Too many arguments."
    exit 1
fi

# install ...
if [ "$COMMAND" = "install" ]; then
    install
elif [ "$COMMAND" = "bootstrap" ]; then
    install_anaconda
    install_deps
elif [ "$COMMAND" = "clean" ]; then
    clean
fi

exit 0

