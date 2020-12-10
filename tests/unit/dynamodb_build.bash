#! /bin/bash
mkdir ./tests/unit/dynamodb_local
cd ./tests/unit/dynamodb_local
curl -O "https://s3-us-west-2.amazonaws.com/dynamodb-local/dynamodb_local_latest.tar.gz"
tar -xvzf dynamodb_local_latest.tar.gz; rm dynamodb_local_latest.tar.gz
java -jar DynamoDBLocal.jar &
