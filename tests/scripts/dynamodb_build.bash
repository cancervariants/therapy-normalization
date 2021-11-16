#! /bin/bash
# check if dynamodb process already on port 8001
IN_USE=$(ps aux | grep -E 'DynamoDBLocal.jar.*-port 8001' | wc -l)
if [ $IN_USE -lt 2 ]; then
    if [ ! -d tests/scripts/dynamodb_local ]; then
        mkdir tests/scripts/dynamodb_local
    fi
    cd tests/scripts/dynamodb_local
    if [ ! -e DynamoDBLocal.jar ]; then
        curl -O "https://s3-us-west-2.amazonaws.com/dynamodb-local/dynamodb_local_latest.tar.gz"
        tar -xvzf dynamodb_local_latest.tar.gz; rm dynamodb_local_latest.tar.gz
    fi
    java -jar DynamoDBLocal.jar -port 8001 &
fi
