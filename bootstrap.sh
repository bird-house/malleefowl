#!/bin/bash

echo "Starting bootstrap"
python -c 'import urllib; print urllib.urlopen("https://raw.githubusercontent.com/bird-house/birdhousebuilder.bootstrap/master/install.sh").read()' > install.sh
echo "Updated installation script"
bash install.sh bootstrap
echo "Bootstrap done"

