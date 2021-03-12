#!/bin/bash

# Source: https://documentation.uts.nlm.nih.gov/automating-downloads.html
#1. Add your API key from https://uts.nlm.nih.gov/uts/profile
#2. Run the script (for example: bash curl-uts-downloads-apikey.sh https://download.nlm.nih.gov/umls/kss/rxnorm/RxNorm_weekly_current.zip)

export apikey=$RXNORM_API_KEY
export DOWNLOAD_URL=$1

export CAS_LOGIN_URL=https://utslogin.nlm.nih.gov/cas/v1/api-key

if [ -z "$apikey" ]; then echo " Please enter you api key "
   exit
fi

if [ $# -eq 0 ]; then echo "Usage: curl-uts-downloads-apikey.sh  download_url "
                      echo "  e.g.   curl-uts-download-apikey.sh https://download.nlm.nih.gov/umls/kss/rxnorm/RxNorm_full_current.zip"
                      echo "         curl-uts-download-apikey.sh https://download.nlm.nih.gov/umls/kss/rxnorm/RxNorm_weekly_current.zip"
   exit
fi

TGT=$(curl -d "apikey="$apikey -H "Content-Type: application/x-www-form-urlencoded" -X POST https://utslogin.nlm.nih.gov/cas/v1/api-key)

TGTTICKET=$(echo $TGT | tr "=" "\n")

for TICKET in $TGTTICKET
do
    if [[ "$TICKET" == *"TGT"* ]]; then
	  SUBSTRING=$(echo $TICKET| cut -d'/' -f 7)
	  TGTVALUE=$(echo $SUBSTRING | sed 's/.$//')
	fi
done
echo $TGTVALUE
STTICKET=$(curl -d "service="$DOWNLOAD_URL -H "Content-Type: application/x-www-form-urlencoded" -X POST https://utslogin.nlm.nih.gov/cas/v1/tickets/$TGTVALUE)
echo $STTICKET

curl -c cookie.txt -b cookie.txt -L -o $RXNORM_PATH -J $DOWNLOAD_URL?ticket=$STTICKET
rm cookie.txt