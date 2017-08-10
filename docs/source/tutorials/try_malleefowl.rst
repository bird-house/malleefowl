Try Malleefowl
==============

Go through this tutorial step by step.

.. contents::
    :local:
    :depth: 1


Install malleefowl with defaults
--------------------------------

.. code-block:: sh

    # get the source code
    $ git clone https://github.com/bird-house/malleefowl.git
    $ cd malleefowl

    # run the installation
    $ make clean install

    # start the service
    $ make start

    # open the capabilities document
    $ firefox http://localhost:8091/wps

Install birdy
-------------

We are using birdy in the examples, a WPS command line client.

.. code-block:: sh

    # install it via conda
    $ conda install -c birdhouse birdhouse-birdy

Check if birdy works
--------------------

.. code-block:: sh

    # point birdy to the malleefowl service url
    $ export WPS_SERVICE=http://localhost:8091/wps
    # show a list of available command (wps processes)
    $ birdy -h

Run the download process
------------------------

Make sure birdy works and is pointing to malleefowl ... see above.

.. code-block:: sh

    # show the description of the download process
    $ birdy download -h

    # download a netcdf file from a public thredds service
    $ birdy download --resource \
        https://www.esrl.noaa.gov/psd/thredds/fileServer/Datasets/ncep.reanalysis2/surface/mslp.1979.nc
