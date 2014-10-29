FROM ubuntu:14.04
MAINTAINER Carsten Ehbrecht <ehbrecht@dkrz.de>

RUN apt-get update

# install project requirements
ADD ./requirements.sh /tmp/requirements.sh  
RUN cd /tmp && bash requirements.sh && cd -

RUN useradd -d /home/malleefowl -m malleefowl
ADD . /home/malleefowl/src
RUN chown -R malleefowl /home/malleefowl/src

USER malleefowl
WORKDIR /home/malleefowl/src

RUN bash bootstrap.sh
RUN bash install.sh clean
RUN bash install.sh build

WORKDIR /home/malleefowl/anaconda

EXPOSE 8080 8090 8091 9001

#CMD bin/supervisord -n -c etc/supervisor/supervisord.conf && bin/nginx -c etc/nginx/nginx.conf -g 'daemon off;
CMD etc/init.d/supervisord start && bin/nginx -c etc/nginx/nginx.conf -g 'daemon off;'

