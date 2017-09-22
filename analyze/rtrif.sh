#!/bin/bash
usage(){
	echo 'rtrif -i <$input> -o <$output>'
}

iffinder=iffinder-1.38/iffinder
while getopts "i:o:" opt; do
	case "$opt" in
		i)
			input=$OPTARG ;;
		o)
			output=$OPTARG ;;
		*)
			usage
			exit -1;;
	esac
done

test -z "$input" && usage && exit
test -z "$output" && usage && exit

echo "cat $input | tail -n +2 | cut -d ' ' -f 1 | sort | uniq >deg0"
cat $input | tail -n +2 | cut -d ' ' -f 1 | sort | uniq >deg0
echo "cat $input | grep 'Y' | cut -d ' ' -f 2 | sort | uniq >rpl"
cat $input | grep 'Y' | cut -d ' ' -f 2 | sort | uniq >rpl
comm -2 rpl deg0 >rst

kill `ps -ef | grep iffinder | awk '{print $2}'` >/dev/null 2>&1 #kill active iffinder.
echo "$iffinder -d -o $output -c 200 -r 500 rst"
$iffinder -d -o $output -c 200 -r 500 rst
