#! /bin/bash
existing_reqs=`cat requirements.txt`
generated_reqs=$(pipenv lock --requirements)
if [ "$existing_reqs" = "$generated_reqs" ]; then
    exit 0
else
    existing_dev_reqs=`cat requirements-dev.txt`
    generated_dev_reqs=$(pipenv lock --dev --requirements)
    if [ "$existing_dev_reqs" = "$generated_dev_reqs" ]; then
        exit 0
    else
        exit 1
    fi
fi
