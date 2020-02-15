#!/bin/bash

PYFILE=submission.py
if [ ! -z $1 ] && [ -f $1 ]; then
    PYFILE=$1
    echo "Using $PYFILE"
else
    echo "Using $PYFILE by default."
fi

echo "===JOB Started on $PYFILE==="

#data/ is mounted to the container
cp $PYFILE data/

#Ignoring the stderr that spark-submit outputs
docker exec -it spark_master_1 /bin/bash -c "spark-submit /tmp/data/$PYFILE 2> /dev/null" 

rm data/$PYFILE
echo "===JOB Completed==="
