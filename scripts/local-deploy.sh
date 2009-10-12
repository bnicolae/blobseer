#!/bin/bash
HOST_FILE=/tmp/localhost.txt
TEMPLATE_FILE=$BLOBSEER_HOME/scripts/blobseer-template.cfg
echo "127.0.0.1" > $HOST_FILE
$BLOBSEER_HOME/scripts/blobseer-deploy.py --vmgr=`cat $HOST_FILE` --pmgr=`cat $HOST_FILE` --dht=$HOST_FILE --providers=$HOST_FILE --launch=$TEMPLATE_FILE
rm $HOST_FILE
