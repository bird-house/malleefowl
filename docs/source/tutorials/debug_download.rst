Debug Download Process
======================

Go through this tutorial step by step.

.. contents::
    :local:
    :depth: 1


Step 0: Install malleefowl for debugging
----------------------------------------

.. code-block:: sh

    # get the source code
    $ git clone https://github.com/bird-house/malleefowl.git
    $ cd malleefowl

    # create conda env
    $ conda env create

    # activate malleefowl env
    $ source activate malleefowl

    # install malleefowl package in develop mode
    $ python setup.py develop

    # check if the demo service is available
    $ malleefowl -h

Step 1: Start the malleefowl demo service
-----------------------------------------

You might do this more often when debugging.
Make sure you are in the malleefowl conda env.

.. code-block:: sh

    # start service
    $ malleefowl

    # open the capabilities document
    $ firefox http://localhost:5000/wps?service=WPS&request=GetCapabilities

The service is started in debug mode.
See the `Werkzeug <http://werkzeug.pocoo.org/docs/0.12/debug/>`_
documenation how to work with this.

You can stop the service with ``CTRL-c``.
The service is automatically restarted on source changes.
