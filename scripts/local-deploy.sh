#!/bin/bash
HOST_FILE=/tmp/localhost.txt
TEMPLATE_FILE=$BLOBSEER_HOME/scripts/blobseer-template.cfg
echo localhost > $HOST_FILE
$BLOBSEER_HOME/scripts/blobseer-deploy.py --vmgr=localhost --pmgr=localhost --dht=$HOST_FILE --providers=$HOST_FILE --launch=$TEMPLATE_FILE
rm $HOST_FILE
