FROM ubuntu:14.04
MAINTAINER Carsten Ehbrecht <ehbrecht@dkrz.de>

RUN apt-get update && apt-get install -y git wget
WORKDIR /tmp
RUN wget https://raw.githubusercontent.com/bird-house/malleefowl/master/requirements.sh
RUN bash requirements.sh
RUN adduser phoenix
USER phoenix
RUN git clone https://github.com/bird-house/malleefowl.git
RUN cd malleefowl && bash install.sh && cd -
RUN git clone https://github.com/bird-house/emu.git
RUN cd emu && bash install.sh && cd -
RUN git clone https://github.com/bird-house/hummingbird.git
RUN cd hummingbird && bash install.sh && cd -
RUN git clone https://github.com/bird-house/pyramid-phoenix.git
RUN cd pyramid-phoenix && bash install.sh && cd -
WORKDIR /home/phoenix/anaconda
EXPOSE 9001
EXPOSE 8091
EXPOSE 8092
EXPOSE 8093
EXPOSE 8094
EXPOSE 8080
EXPOSE 8081
