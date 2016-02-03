#!/usr/bin/env bash

cd ./2015/05/04

for directory in *;
do    
    for filename in $directory/*.bz2;
    do
        echo $filename
        bzip2 -d $filename
    done
done    
