import sys
import os
import urllib
import time

def download_pch(dst_dir):
	pch_ixp_url = "https://prefix.pch.net/applications/ixpdir/download.php?s=exchange"
	pch_active_subnet_url = "https://prefix.pch.net/applications/ixpdir/download.php?s=subnet_active"
	pch_subnet_url = "https://prefix.pch.net/applications/ixpdir/download.php?s=subnet"
	pch_membership_url = "https://prefix.pch.net/applications/ixpdir/download.php?s=ix_membership"
	
	url_list = [pch_ixp_url, pch_active_subnet_url, pch_subnet_url, pch_membership_url]
	file_list = ["pch_ixp", "pch_active_subnet", "pch_subnet", "pch_membership"]
	
	for i in range(len(url_list)):
		url=url_list[i]
		f=file_list[i]
		print " ... downloading %s ... " % (f),
		sys.stdout.flush()
		path=dst_dir+"/"+f
		urllib.urlretrieve(url, path)
		print "finished"

def usage():
	print "python pch.py $dst_dir"
	exit()

def main(argv):
	if (len(argv) <2):
		usage()

	dst_dir = argv[1]
	dst_dir = dst_dir+"/pch"
	if (not os.path.exists(dst_dir)):
		os.makedirs(dst_dir)
	download_pch(dst_dir)

if __name__ == "__main__":
	main(sys.argv)
