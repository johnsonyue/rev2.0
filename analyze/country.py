import sys
import getopt
import format
import re #debug

from geoip import geoip

GZIP_OUTPUT = False
OVERWRITE_FILENAME = ""
IGNORE_META = False
SUPRESS_META = False

def write(line, country, handle_dict, header_line):
	if not handle_dict.has_key(country):
		#determine filename
		if OVERWRITE_FILENAME != "":
			file_name = OVERWRITE_FILENAME
		elif header_line != "" and not IGNORE_META:
			meta_list = header_line.split(format.hd)
			source = meta_list[format.Meta.source]
			time = meta_list[format.Meta.time]
			monitor = meta_list[format.Meta.monitor]
			
			file_name = str(source) + format.fnd + str(time) + format.fnd + str(monitor)
		else:
			file_name = "nosource" + format.fnd + "notime" + format.fnd + "nomonitor"
		
		file_name += "." + str(country)
			
		if GZIP_OUTPUT:
			file_name += ".gz"
		
		#handle
		fpo = open(file_name, 'w')
		if GZIP_OUTPUT:
			h = subprocess.Popen(['gzip', '-c', '-'], stdin=subprocess.PIPE, stdout=fpo)
			handle = h.stdin
		else:
			handle = fpo
		handle_dict[country] = handle
		if not SUPRESS_META and header_line != "":
			handle.write(header_line + "\n")
	else:
		handle = handle_dict[country]
	
	handle.write(line + "\n")

def split_caida(geodb_list, country_list):
	helper = geoip.geoip_helper(geodb_list)
	handle_dict = {}
	header_line = ""
	while True:
		try:
			line = raw_input()
		except EOFError:
			exit()
	
		#comments and header line
		if (line.split(' ')[0] == "#"):
			continue
		
		if (line.split(format.hd)[0] == format.hi):
			header_line = line
			continue
		
		#trace lines
		fields = line.strip('\n').split('\t', 13)
		if (len(fields) < 14): #skip cases where there's no hop at all
			continue
		
		#construct four fields of each trace
		dstip = fields[2]
		geo = helper.query(dstip)
		
		#order in list represents priority
		for db in geodb_list:
			if geo[db] == "" or not geo[db]["country"] in country_list:
				continue
			if geo[db]["country"] in country_list:
				write(line,geo[db]["country"],handle_dict,header_line)

	#clear up
	for handle in handle_dict.itervalues():
		handle.close()

def print_debug(msg):
	if (DEBUG):
		sys.stderr.write("%s\n" % (msg))
def usage():
	print "country [OPTIONS] caida/iplane/ripeatlas/hit -"
	print "OPTIONS:"
	print "-h print this Help"
	print "-l <bgp/czdb/mmdb/ip2location> specify geoip database"
	print "-z gzip output"
	print "-o <filename> overwrite output file name,"
	print "-r ignore meta header"
	print "-s supress meta header"

def main(argv):
	global DEBUG
	global GZIP_OUTPUT
	global OVERWRITE_FILENAME
	global IGNORE_META
	global SUPRESS_META

	try:
		opts, args = getopt.getopt(argv[1:-1], "hgl:zo:rs")
	except getopt.GetoptError as err:
		print str(err)
		usage()
		exit(2)

	geodb_list = []
	country_list = [
		"CN","US","JP","KR","RU",
		"SY","IR","LY","AF","IQ",
		"PK","TW","HK"
	]

	for o,a in opts:
		if o == "-h":
			usage()
			exit(0)
		elif o == "-g":
			DEBUG = True
		elif o == "-l":
			geodb_list.append(a)
		elif o == "-z":
			GZIP_OUTPUT = True
		elif o == "-o":
			OVERWRITE_FILENAME = a
		elif o == "-r":
			IGNORE_META = True
		elif o == "-s":
			SUPRESS_META = True
	
	for geodb in geodb_list:
		if not geodb in [ "bgp", "czdb", "mmdb", "ip2location" ]:
			usage()
			exit(2)
	
	if len(geodb_list) == 0:
		geodb_list = [ "mmdb" ]
	
	source = argv[-1]
	if not source in [ "caida", "iplane", "ripeatlas", "hit" ]:
		usage()
		exit(2)

	if source == "caida":
		split_caida(geodb_list, country_list)
	elif source == "iplane":
		split_iplane(geodb_list, country_list)

if __name__ == "__main__":
	main(sys.argv)
