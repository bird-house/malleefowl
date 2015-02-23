.. _installation:
Running tests
*************
To run all unit tests one needs to fetch testdata with an esgf openid and start the malleefowl service::

    $ bin/wpsfetch -u username
    $ make start
    $ make test

Testdata is collected in ``testdata.json``::

    $ vim testdata.json

