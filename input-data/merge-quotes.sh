#!/usr/bin/env bash

cd ./2015/05/04

for directory in *;
do    
    full_archive="2015-may-4.json"
    touch $full_archive
    for filename in $directory/*.json;
    do
        echo $filename
        cat $filename >> $full_archive
    done
done    
