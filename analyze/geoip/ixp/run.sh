#!/bin/bash

[ $# -ne 1 ] && echo './run.sh $dst_dir' && exit

dst_dir=$1
path=$dst_dir"/tmp"
mkdir -p $path

echo "python bgplgdb.py >$path/bgplgdb"
python bgplgdb.py >$path/bgplgdb

echo "python wikiixp.py >$path/wikiixp"
python wikiixp.py >$path/wikiixp

echo "python pch.py $path"
python pch.py $path

echo "python peeringdb.py $path"
datetime=$( python peeringdb.py $path )
datetime=$( echo $datetime | awk -F'-' '{print $NF}' )
dst_path=$dst_dir"/"$datetime

echo "mv $path $dst_path"
mv $path $dst_path
