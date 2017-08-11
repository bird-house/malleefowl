Using Docker image
==================

To run Malleefowl Web Processing Service you can also use the `Docker <https://hub.docker.com/r/birdhouse/malleefowl/>`_ image::

  $ docker run -i -d -p 9001:9001 -p 8000:8000 -p 8080:8080 --name=malleefowl birdhouse/malleefowl

Check the docker logs::

  $ docker logs malleefowl

Show running docker containers::

  $ docker ps

Open your browser and enter the url of the supervisor service:

  http://localhost:9001/

Run a GetCapabilites WPS request:

  http://localhost:8080/wps?service=WPS&version=1.0.0&request=getcapabilities



Using docker-compose
--------------------

Start malleefowl with docker-compose (docker-compose version > 1.7):

.. code-block:: sh

    $ docker-compose up

By default the WPS is available on port 8080:
http://localhost:8080/wps?service=WPS&version=1.0.0&request=GetCapabilities.

You can change the ports and hostname with environment variables:

.. code-block:: sh

  $ HOSTNAME=malleefowl HTTP_PORT=8091 SUPERVISOR_PORT=48091 docker-compose up

Now the WPS is available on port 8091:
http://malleefowl:8091/wps?service=WPS&version=1.0.0&request=GetCapabilities.
