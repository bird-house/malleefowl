FROM ubuntu:14.04
MAINTAINER Carsten Ehbrecht <ehbrecht@dkrz.de>

RUN apt-get update && apt-get install -y git wget
WORKDIR /tmp
RUN wget https://raw.githubusercontent.com/bird-house/malleefowl/master/requirements.sh
RUN bash requirements.sh
RUN useradd -d /home/phoenix -m phoenix
USER phoenix
RUN git clone https://github.com/bird-house/malleefowl.git
RUN cd malleefowl && bash install.sh && cd -
WORKDIR /home/phoenix/anaconda
EXPOSE 8080 8091 9001
