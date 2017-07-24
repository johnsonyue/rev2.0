#!/bin/bash

pre_caida_date(){
	input_dir=$1
	output_dir=$2

	ls $input_dir/*.warts.gz | while read line; do
		fn=$(echo $line | sed "s/^.*\///g")
		fn=$(echo $fn | sed "s/\.warts\.gz//g")
		echo ">> $line ==(parse)==> $output_dir/$fn"
		./decode -t caida -m $line | python uniform.py caida | python tuple.py -m -b -g -o $fn -d $output_dir
	done
}

merge_caida_date(){
	dir=$1
	
	date=$(echo $dir | sed "s/\/$//g" | sed "s/^.*\///g")
	ls $dir/*.edge | grep -v "backup" | python merge.py -y edge -o $dir/$date.edge.merged -g
	ls $dir/*.backup.edge | python merge.py -n -t -y edge -o $dir/$date.backup.edge.merged -g
	ls $dir/*.node | grep -v "backup" | python merge.py -y node -o $dir/$date.node.merged -g
	ls $dir/*.backup.node | python merge.py -y node -o $dir/$date.backup.node.merged -g
	wait
}

merge_caida_month(){
	dir=$1
	year_month=$2
	ls $dir/$year_month*/*.edge.merged | grep -v "backup" | python merge.py -y edge -o $dir/$year_month.edge.output -g
	ls $dir/$year_month*/*.backup.edge.merged | python merge.py -n -t -y edge -o $dir/$year_month.backup.edge.output -g
	ls $dir/$year_month*/*.node.merged | grep -v "backup" | python merge.py -y node -o $dir/$year_month.node.output -g
	ls $dir/$year_month*/*.backup.node.merged | python merge.py -y node -o $dir/$year_month.backup.node.output -g
}

in_dir=$1
out_dir=$2

ls -d $in_dir/* | grep "20*" | sort -nr | while read line; do
	date=$(echo $line | sed "s/\/$//g" | sed "s/^.*\///g")
	test -d $out_dir/$date/ && echo "$out_dir/$date/ already exists" && continue
	echo "mkdir -p $out_dir/$date"
	mkdir -p $out_dir/$date
	echo "> $line ==(preprocess)==> $out_dir/$date/"
	pre_caida_date $line $out_dir/$date/
	echo "> $line ==(merge)==> $out_dir/$date/"
	merge_caida_date $out_dir/$date
done

#year_month="20170"
#merge_caida_month $out_dir $year_month
