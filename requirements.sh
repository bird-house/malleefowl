#!/bin/bash
if [ -f /etc/debian_version ] ; then
    #sudo apt-get -y --force-yes install myproxy
    # java
    sudo apt-get install -y --force-yes openjdk-7-jre openjdk-7-jdk
    if [ -d /usr/lib/jvm/java-7-openjdk-amd64 ] ; then
        sudo update-alternatives --set java /usr/lib/jvm/java-7-openjdk-amd64/jre/bin/java
    elif [ -d /usr/lib/jvm/java-7-openjdk-i386 ] ; then
        sudo update-alternatives --set java /usr/lib/jvm/java-7-openjdk-i386/jre/bin/java
    fi
elif [ -f /etc/redhat-release ] ; then
    #sudo yum -y install myproxy
    # java
    sudo yum install -y java-1.7.0-openjdk java-1.7.0-openjdk-devel 
elif [ `uname -s` = "Darwin" ] ; then
    # java
    brew cask install java
    # globus myproxy
    #brew install globus-toolkit
fi
