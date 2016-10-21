.. _docker:

Using Docker image
******************

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
