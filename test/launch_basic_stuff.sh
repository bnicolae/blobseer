#!/bin/bash

if [ $# -ne 1 ]
then
    echo "use: $0 <config file>"
    exit 1
fi

CONF_FILE=$1

# vmanager
echo "Launching vmanager..."
../vmanager/vmanager $CONF_FILE &> vmanager.out &
sleep 1
# pmanager
echo "Launching pmanager..."
../pmanager/pmanager $CONF_FILE &> pmanager.out &
sleep 1
# sdht
echo "Launching single sdht..."
../provider/sdht $CONF_FILE &> sdht.out &
sleep 1
# provider
echo "Launching provider..."
../provider/provider $CONF_FILE &> provider.out &
