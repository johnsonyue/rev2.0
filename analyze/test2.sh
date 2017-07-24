#!/bin/bash

pre_caida_date(){
	input_dir=$1
	output_dir=$2

	ls $input_dir/caida* | while read line; do
		fn=$(echo $line | sed "s/^.*\///g")
		echo ">> $line ==(parse)==> $output_dir/$fn"
		./decode -t caida -m $line | python uniform.py caida | python tuple.py -b -g -o $fn -d $output_dir
	done
}

merge_caida_date(){
	dir=$1
	
	date=$(echo $dir | sed "s/\/$//g" | sed "s/^.*\///g")
	ls $dir/*.edge | grep -v "backup" | python merge.py -y edge -o $dir/$date.edge.merged -g -p tmp
	ls $dir/*.backup.edge | python merge.py -n -t -y edge -o $dir/$date.backup.edge.merged -g -p tmp
	ls $dir/*.node | grep -v "backup" | python merge.py -y node -o $dir/$date.node.merged -g -p tmp
	ls $dir/*.backup.node | python merge.py -y node -o $dir/$date.backup.node.merged -g -p tmp
	wait
}

in_dir=$1
out_dir=$2

pre_caida_date $in_dir $out_dir
merge_caida_date $out_dir
