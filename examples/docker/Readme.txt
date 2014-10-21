build docker image:

$ sudo docker build -t="macpingu/birdhouse-demo:v1" .

run docker container:

$ sudo docker run -i -P -t macpingu/birdhouse-demo:v1 /bin/bash
$ sudo docker run -i -p 8080:8080 -p 9001:9001 -p 8091:8091 -t macpingu/birdhouse-demo:v1 /bin/bash


