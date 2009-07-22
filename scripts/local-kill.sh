#!/bin/bash
HOST_FILE=/tmp/localhost.txt
echo localhost > $HOST_FILE
$BLOBSEER_HOME/scripts/blobseer-deploy.py --vmgr=localhost --pmgr=localhost --dht=$HOST_FILE --providers=$HOST_FILE --kill
rm $HOST_FILE
