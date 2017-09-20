#!/bin/bash
outfile="temp_out_file"

>&2 echo "> scamper -i $1 -o $outfile -c trace -O warts"
scamper -f $1 -o $outfile -c trace -O warts
>&2 echo "> sc_analysis_dump $outfile | grep -v \"#\" | output"
sc_analysis_dump $outfile | grep -v "#" | while read line; do info=$(echo $line | awk 'BEGIN{FS=" "} {split($NF,a,","); print $3,$7,a[1],(NF-12)}'); ts=$(echo $line | awk 'BEGIN{FS=" "} {split($NF,a,","); print $6}'); echo $info" "$(date -d "@$ts" +%Y%m%d-%H:%M:%S); done
