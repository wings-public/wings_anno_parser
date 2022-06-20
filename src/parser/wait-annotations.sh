#!/bin/sh

file1=$1
file2=$2
file3=$3
file4=$4

# Checks if Error file exists for VEP or CADD. Otherwise it checks if completed file exists for VEP or CADD
# If one this conditions is fulfilled, it will exit from until and send the response
#until [ -e "$file3" ] || [ -e "$file4" ] || { [ -e "$file1" ] &&  [ -e "$file2" ] ;}; do
until [ -e "$file3" ] || [ -e "$file4" ] || ( [ -e "$file1" ] &&  [ -e "$file2" ] ); do
    #echo "Annotation files are unavailable - sleeping "
    sleep 1
done


if [ -e "$file3" ] || [ -e "$file4" ]
then
    echo "Error in one of the Annotation Engines VEP or CADD. Halting here. Does not proceed to mongoParser"
    exit 1
else
    echo "Mentioned Annotation files exists for VEP and CADD"
    exit 0
fi
