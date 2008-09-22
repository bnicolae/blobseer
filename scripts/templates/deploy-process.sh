#!/bin/bash
#create directories and launch command: $1=path to command, $2=command, $3=config file, $4=running dir
rm -r -f $4
mkdir -p $4
$1/$2 $3 >$4/$2.stdout 2>$4/$2.stderr &
