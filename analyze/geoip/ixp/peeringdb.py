import urllib
import json
import datetime
import os
import sys

def update_peeringdb(dst_dir):
	seed_url = "https://www.peeringdb.com/api/"
	
	temp_dir = "peeringdb_tmp"
	prefix=dst_dir+"/"+temp_dir
	if not os.path.exists(prefix):
		os.makedirs(prefix)

	sys.stderr.write(" ... downloading peeringdb_seed ... ")
	sys.stderr.flush()
	urllib.urlretrieve(seed_url, prefix+"/peeringdb_seed")
	sys.stderr.write("done.\n")
	
	seed_json=json.loads(open(prefix+"/peeringdb_seed",'rb').read())
	file_list=[]
	url_list=[]
	for k,v in seed_json["data"][0].items():
		file_list.append("peeringdb_"+k)
		url_list.append(v)
	
	for i in range(len(file_list)):
		f=file_list[i]
		url=url_list[i]
		sys.stderr.write(" ... downloading %s ... " % (f))
		sys.stderr.flush()
		urllib.urlretrieve(url, prefix+"/"+f)
		sys.stderr.write("done.\n")
	
	db_json=json.loads(open(prefix+"/"+file_list[0]).read())
	timestamp=int(db_json["meta"]["generated"])
	date=datetime.datetime.fromtimestamp(timestamp).strftime("%Y%m%d.%H00")
	
	os.system("mv %s %s" % (dst_dir+"/"+temp_dir, dst_dir+"/peeringdb-"+date))
	print dst_dir+"/peeringdb-"+date

def usage():
	print "python peeringdb.py $dst_dir"

def main(argv):
	if (len(argv) <2):
		usage()
		exit()
	dst_dir = argv[1]
	update_peeringdb(dst_dir)

if __name__ == "__main__":
	main(sys.argv)
