
#!/bin/bash

if [ -z $1 ]; then
    echo "Input: start/stop"
    exit 1
fi

if [ "$1" == "start" ]; then
    docker-compose up -d
else
    if [ "$1" == "stop" ]; then
        docker-compose stop
    fi
fi