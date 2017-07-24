#./decode -t caida /ftp/raw_data/caida/20161220-20170617/20170601/team-1.20170601.anc-us.warts.gz | python country.py -s -l mmdb -g caida
#split_caida "/ftp/raw_data/caida/20161220-20170617/20170601/" "/ftp/countries/"
#./test2.sh ftp/countries/US/raw_data/caida/20170617/ /ftp/countries/US/raw_data/caida/20170617/

split_caida(){
	dir=$1
	out_dir=$2
	date=$( echo $dir | sed "s/\/$//g" | sed "s/^.*\///g" )
	ls $dir/*.warts.gz | while read line; do
		echo "./decode -m -t caida $line | python country.py -s -l mmdb -g caida"
		./decode -m -t caida $line | python country.py -s -l mmdb -g caida
		ls | grep "^caida.*" | while read line2; do
			country=$(echo $line2 | sed "s/[^\.]*\.//g")
			echo "mkdir -p $out_dir/$country/raw_data/caida/$date"
			mkdir -p $out_dir/$country/raw_data/caida/$date
			echo "mv $line2 $out_dir/$country/raw_data/caida/$date"
			mv $line2 $out_dir/$country/raw_data/caida/$date
		done
	done
}

input_dir=$1
output_dir=$2
start_from=$3

ls -d $input_dir/* | grep "20*" | sort -nr | while read line; do
	date=$( echo $line | sed "s/\/$//g" | sed "s/^.*\///g" )
	test $date -gt $start_from && echo "ignore "$date && continue
	echo "split_caida $line"
	split_caida $line $output_dir
	ls -d $output_dir/[A-Z][A-Z] | while read line3; do
		test -d $line3/raw_data/caida/$date && ./test2.sh $line3/raw_data/caida/$date $line3/raw_data/caida/$date
	done
done
