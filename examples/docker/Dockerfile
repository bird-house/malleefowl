FROM ubuntu:14.04
MAINTAINER Carsten Ehbrecht <ehbrecht@dkrz.de>

RUN apt-get update && apt-get install -y git wget
RUN adduser phoenix
USER phoenix
WORKDIR /tmp
RUN git clone https://github.com/bird-house/malleefowl.git
WORKDIR malleefowl
RUN bash requirements.sh
RUN bash install.sh
RUN /home/phoenix/anaconda/bin/conda install --yes -c https://conda.binstar.org/pingucarsti pyopenssl=0.13.1
RUN bash install.sh
WORKDIR /home/phoenix/anaconda
RUN etc/init.d/supervisord start
RUN etc/init.d/nginx start
CMD bin/supervisorctl status
EXPOSE 9001
EXPOSE 8091

