#!/bin/bash

python -c 'import urllib; print urllib.urlopen("https://raw.githubusercontent.com/bird-house/birdhousebuilder.bootstrap/master/install.sh").read()' > install.sh
