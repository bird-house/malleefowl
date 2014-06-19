#!/bin/bash
PWD=`pwd`
BUILDOUT_DIR=`dirname $0`
DOWNLOAD_CACHE=downloads
ANACONDA_HOME="/opt/anaconda"
ANACONDA_FILE=Miniconda-latest-Linux-x86_64.sh
ANACONDA_URL=http://repo.continuum.io/miniconda/$ANACONDA_FILE
ANACONDA_MD5=01b39f6b143102e6e0008a12533c1fc9

# actions before build
function pre_build() {
    upgrade
    setup_cfg
    setup_os
    install_anaconda
}

# upgrade stuff which can not be done by buildout
function upgrade() {
    old_phoenix="$BUILDOUT_DIR/src/Phoenix"
    if [ -d $old_phoenix ]; then
        echo "Removing old Phoenix sources in $old_phoenix."
        rm -rf $old_phoenix
    fi
}

# set configurion file for buildout
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

# install os packages needed for bootstrap
function setup_os() {
    if [ -f /etc/debian_version ] ; then
        setup_debian
    fi
    if [ -f /etc/redhat-release ] ; then
        setup_centos
    fi
}

function setup_debian() {
    sudo apt-get -y --force-yes install wget subversion
}

function setup_centos() {
    sudo rpm -i http://dl.fedoraproject.org/pub/epel/6/x86_64/epel-release-6-8.noarch.rpm
    sudo yum install -y gcc-c++ subversion

}

function install_anaconda() {
    # download miniconda setup script to download cache with wget
    wget -q -c -O $DOWNLOAD_CACHE/$ANACONDA_FILE $ANACONDA_URL

    # md5 check sum on the current file you downloaded and save results to 'test1'
    test_md5=`md5sum "$DOWNLOAD_CACHE/$ANACONDA_FILE" | awk '{print $1}'`;
    if [ "$test_md5" != $ANACONDA_MD5 ]; then 
        echo "checksum didn't match!"
        #echo "Installing Anaconda ... Failed"
        #exit 1
    fi

    # run miniconda setup, install in ANACONDA_HOME
    if [ ! -d $ANACONDA_HOME ]; then
        sudo bash "$DOWNLOAD_CACHE/$ANACONDA_FILE" -b -p $ANACONDA_HOME
         # add anaconda path to user .bashrc
        #echo -e "\n# Anaconda PATH added by climdaps installer" >> $HOME/.bashrc
        #echo "export PATH=$ANACONDA_HOME/bin:\$PATH" >> $HOME/.bashrc
    fi
    ##TODO: workaround for required packages
    #sudo $ANACONDA_HOME/bin/conda install --yes numpy
    #sudo $ANACONDA_HOME/bin/conda install --yes Fiona
    #sudo $ANACONDA_HOME/bin/conda install --yes netCDF4
    #sudo $ANACONDA_HOME/bin/conda install --yes shapely

    # add anaconda to system path for all users
    #echo "export PATH=$ANACONDA_HOME/bin:\$PATH" | sudo tee /etc/profile.d/anaconda.sh > /dev/null
    # source the anaconda settings
    #. /etc/profile.d/anaconda.sh

    sudo chown -R $USER $ANACONDA_HOME

    echo "Installing Anaconda ... Done"
}

# run install
function install() {
    echo "Installing ClimDaPS ..."
    echo "BUILDOUT_DIR=$BUILDOUT_DIR"
    echo "DOWNLOAD_CACHE=$DOWNLOAD_CACHE"

    cd $BUILDOUT_DIR
    
    pre_build
    $ANACONDA_HOME/bin/python bootstrap.py -c custom.cfg
    echo "Bootstrap ... Done"
    bin/buildout -c custom.cfg

    cd $PWD

    echo "Installing ClimDaPS ... Done"
}

function usage() {
    echo "Usage: $0"
    exit 1
}

install
