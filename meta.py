import sys

import format

def usage():
	print "Usage:"
	print "    meta <string_type> <dir_name_string> <file_name_string>"
	print "Types:"
	print "    caida/iplane/ripeatlas/text"
	print "Name contains 4 fields:"
	print "    source, date, time, monitor, extra (team etc.)"
	print "Example:"
	print "    meta caida team-1.20170701.pku-cn.warts.tar.gz"

def caida_meta(dir_str, file_str):
	#parse strs to get fields.
	source="caida"
	date=dir_str.split('/')[-1]
	time=file_str.split('.')[1]
	monitor=file_str.split('.')[2]
	team=file_str.split('.')[0]
	extra=team
	
	#construct argv dict.
	argv={}
	argv[format.Meta.source]=source
	argv[format.Meta.date]=date
	argv[format.Meta.time]=time
	argv[format.Meta.monitor]=monitor
	argv[format.Meta.extra]=extra
	argv[format.Meta.srcip]="*"
	
	#print meta header.
	format.print_meta(argv)

def main(argv):
	if len(argv) != 4:
		usage()
		exit()
	
	str_type=argv[1]
	dir_str=argv[2]
	file_str=argv[3]
	
	if str_type == "caida":
		caida_meta(dir_str, file_str)

if __name__ == "__main__":
	main(sys.argv)
