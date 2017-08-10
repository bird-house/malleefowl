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


Install Phoenix
---------------

Phoenix is a web client for WPS and comes by default with an WPS security proxy (twitcher).

.. code-block:: sh

    $ git clone https://github.com/bird-house/pyramid-phoenix.git
    $ cd pyramid-phoenix
    $ make clean install
    $ make restart

Login to Phoenix and get twitcher access token
-----------------------------------------------

.. code-block:: sh

    # login ... by default admin password is "querty"
    $ firefox https://localhost:8443/account/login

Copy the twitcher access token in Phoenix
-----------------------------------------

#. Go to your profile.
#. Choose the ``Twitcher access token`` tab.
#. Copy the access token.

Access malleefowl behind the OWS proxy with access token
--------------------------------------------------------

.. code-block:: sh

    # configure wps service
    $ export WPS_SERVICE=https://localhost:8443/ows/proxy/malleefowl

    # check if it works
    $ birdy -h

    # run the download again ... you need the access token
    $ birdy download --token 3d8c24eeebb143b3a199ba8a0e045f93 --resource \
        https://www.esrl.noaa.gov/psd/thredds/fileServer/Datasets/ncep.reanalysis2/surface/mslp.1979.nc

Get a ESGF certificate using Phoenix
------------------------------------

#. Go to your profile.
#. Choose the ``ESGF credentials`` tab.
#. Use the green button ``Update credentials``.
#. Choose your ESGF provider, enter your account details and press ``Submit``.


Download a file from ESGF
-------------------------

Make sure birdy works and points to the proxy url of malleefowl ... see above.

Choose a file from the ESGF archive you would like to download and make sure you have dowload permissions.

You can choose the ESGF `search browser <https://localhost:8443/esgfsearch>`_ in Phoenix
or an `ESGF portal <https://esgf-data.dkrz.de/>`_.

.. code-block:: sh

    # try the download ... in this example with a CORDEX file.
    # make sure your twitcher token and your ESGF cert are still valid.
    $ birdy download --token 3d8c24eeebb143b3a199ba8a0e045f93 --resource \
        http://esgf1.dkrz.de/thredds/fileServer/cordex/cordex/output/EUR-44/MPI-CSC/MPI-M-MPI-ESM-LR/historical/r1i1p1/MPI-CSC-REMO2009/v1/mon/tas/v20150609/tas_EUR-44_MPI-M-MPI-ESM-LR_historical_r1i1p1_MPI-CSC-REMO2009_v1_mon_200101-200512.nc
