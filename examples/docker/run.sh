#!/bin/bash
sudo docker run -i -p 8080:8080 -p 8081:8081 -p 9001:9001 -p 8091:8091 -p 8092:8092 -p 8093:8093 -p 8094:8094 -t macpingu/birdhouse-demo:v1 /bin/bash
