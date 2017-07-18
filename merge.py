import sys
import getopt
import subprocess
import re

import format

##############################################################################################
# Requirement: 
#        SORTED tuple files with \
#        SAME format, either node or edge, requires \
#        TABLE HEADER in each file. \ 
#        *META header line is not required but recommended
# Input:
#        by default, reads file name from stdin
# Output: ONE merged tuple file
#
# Options: "-h print this Help"
#          "-i <filename> input filename"
#          "-y <"node">/<"edge"> input file type"
#          "-c input file compressed"
#          "-o <filename> output filename"
#          "-z compress output file"
#          "-n group number by monitor"
#          "-t group ttl by monitor"
#
# Note: final graph does not contain src ip
##############################################################################################

DEBUG = False
#option flags
INPUT_FILETYPE = format.FileType.other
INPUT_FILE_COMPRESSED = False

OUTPUT_FILENAME = ""
GZIP_OUTPUT = False

GROUP_NUMBER_BY_MON = False
GROUP_TTL_BY_MON = False
INCLUDE_MONITOR = False

def edge_line_cmp(ln1, ln2):
	l1in = format.ip_str2int(ln1.split(format.tid)[format.EdgeLine.ingress])
	l2in = format.ip_str2int(ln2.split(format.tid)[format.EdgeLine.ingress])
	if l1in != l2in:
		return l1in - l2in

	l1out = format.ip_str2int(ln1.split(format.tid)[format.EdgeLine.outgress])
	l2out = format.ip_str2int(ln2.split(format.tid)[format.EdgeLine.outgress])
	if l1out != l2out:
		return l1out - l2out

	return 0

def edge_line_merge(line1,line2,header_line1,header_line2,th_line1):
	global GROUP_NUMBER_BY_MON
	global GROUP_TTL_BY_MON

	ingress = line1.split(format.tid)[format.EdgeLine.ingress]
	outgress = line1.split(format.tid)[format.EdgeLine.outgress]

	delay1 = line1.split(format.tid)[format.EdgeLine.delay]
	delay2 = line2.split(format.tid)[format.EdgeLine.delay]
	if delay1 == 0 and delay2 == 0:
		delay = 0
	elif delay1 ==0 and delay2 != 0:
		delay = delay2
	elif delay1 !=0 and delay2 == 0:
		delay = delay1
	else:
		delay = delay1 if delay1 < delay2 else delay2
	
	connected1 = line1.split(format.tid)[format.EdgeLine.connected]
	connected2 = line2.split(format.tid)[format.EdgeLine.connected]
	if connected1 != connected2:
		connected = format.ConnectionState.both
	else:
		connected = connected1
	
	th_list = th_line1.split(format.thd)
	if "length" in th_list:
		length1 = line1.split(format.tid)[format.EdgeLine.length]
		length2 = line2.split(format.tid)[format.EdgeLine.length]
		length = length1 + [ l for l in length2 and not in length1 ]

	if "number" in th_list:
		number1 = int(line1.split(format.tid)[format.EdgeLine.number])
		number2 = int(line2.split(format.tid)[format.EdgeLine.number])
		if not GROUP_NUMBER_BY_MON:
			number = number1 + number2
	
	if "delay_info" in th_list:
		delay_info1 = map(lambda x:int(x), line1.split(format.tid)[format.EdgeLine.delay_info].split(format.ed))
		delay_info2 = map(lambda x:int(x) ,line2.split(format.tid)[format.EdgeLine.delay_info].split(format.ed))
		max_delay = delay_info1[0] if delay_info1[0] > delay_info2[0] else delay_info2[0]
		min_delay = delay_info1[3] if delay_info1[3] < delay_info2[3] else delay_info2[3]
		mean_delay = float(number1*delay_info1[1] + number2*delay_info2[1]) / (number1 + number2)
		delay_info = [ max_delay, mean_delay, delay, min_delay ]

	if "ttl_info" in th_list:
		ttl_info_list1 = map(lambda x:int(x), line1.split(format.tid)[format.EdgeLine.ttl_info].split(format.ed).split(format.ed))
		ttl_info1 = {}
		for i in range(0,len(ttl_info_list1),2):
			key = ttl_info_list1[i]
			value = ttl_info_list1[i+1]
			ttl_info1[key] = value
		ttl_info_list2 = map(lambda x:int(x), line2.split(format.tid)[format.EdgeLine.ttl_info].split(format.ed).split(format.ed))
		for i in range(0,len(ttl_info_list2),2):
			key = ttl_info_list2[i]
			value = ttl_info_list2[i+1]
			ttl_info2[key] = value

		ttl_info = {}
		if not GROUP_TTL_BY_MON:
			for key in ttl_info1.iterkeys():
				if not ttl_info2.has_key(key):
					ttl_info[key] = ttl_info1[key]
				else:
					ttl_info[key] = ttl_info1[key] + ttl_info2[key]
			for key in ttl_info2.iterkeys():
				if not ttl_info1.has_key(key):
					ttl_info[key] = ttl_info2[key]
			
def node_line_cmp(ln1, ln2):
	l1ip = format.ip_str2int(ln1.split(format.tid)[format.NodeLine.ip])
	l2ip = format.ip_str2int(ln2.split(format.tid)[format.NodeLine.ip])
	return l1ip - l2ip

def node_line_merge(line1,line2,header_line1,header_line2,th_line1):
	global INCLUDE_MONITOR

	ip = line1.split(format.tid)[format.NodeLine.ip]
	ntype1 = line1.split(format.tid)[format.NodeLine.ntype]
	ntype2 = line2.split(format.tid)[format.NodeLine.ntype]
	
	if ntype1 != ntype2:
		ntype = format.NodeType.both
	else:
		ntype = ntype1
	
	return

def merge_two(infn1, infn2, ofn, compressed=False):
	global DEBUG
	#option flags
	global INPUT_FILETYPE
	global GZIP_OUTPUT

	global GROUP_NUMBER_BY_MON
	global GROUP_TTL_BY_MON
	
	if INPUT_FILETYPE == format.FileType.node:
		line_cmp_func = node_line_cmp
		line_merge_func = node_line_merge
	elif INPUT_FILETYPE == format.FileType.edge:
		line_cmp_func = edge_line_cmp
	
	#open handles to write
	fpo = open(ofn, 'w')
	if GZIP_OUTPUT:
		ho = subprocess.Popen(['gzip', '-c', '-'], stdin=subprocess.PIPE, stdout=fpo)
		fpo = ho.stdin

	#open handles to read
	handle1 = open(infn1, 'r')
	if compressed or re.compile(".*\.tar\.gz$").match(infn1):
		h1 = subprocess.Popen(['gzip', '-c', '-d', '-'], stdin=handle1, stdout=subprocess.PIPE)
		handle1 = h1.stdout

	handle2 = open(infn2, 'r')
	if compressed or re.compile(".*\.tar\.gz$").match(:
		h2 = subprocess.Popen(['gzip', '-c', '-d', '-'], stdin=handle2, stdout=subprocess.PIPE)
		handle2 = h2.stdout

	#headers
	th_line1 = ""
	header_line1 = ""
	first_line1=handle1.readline()
	if first_line1.split(format.thd)[0] != format.thi:
		if first_line1.split(format.thd)[0] == (format.thi + format.thi):
			print_debug( "Unsorted input file: %s" % (infn1) )
		print_debug( "Table header required in file: %s" % (infn1) )
		return
	else:
		th_line1 = first_line1
		second_line1 = handle1.readline()
		if second_line1.split(format.hd)[0] != format.hi:
			line1 = second_line1
		else:
			header_line1 = second_line1
			line1 = handle1.readline()
	
	
	th_line2 = ""
	header_line2 = ""
	first_line2=handle2.readline()
	if first_line2.split(format.thd)[0] != format.thi:
		if first_line2.split(format.thd)[0] == (format.thi + format.thi):
			print_debug( "Unsorted input file: %s" % (infn2) )
		print_debug( "Table header required in file: %s" % (infn2) )
		return
	else:
		th_line2 = first_line2
		second_line2 = handle2.readline()
		if second_line2.split(format.hd)[0] != format.hi:
			line2 = second_line2
		else:
			header_line2 = second_line2
			line2 = handle2.readline()

	if th_line1 != th_line2:
		print_debug( "File %s, %s has different format, refuse to merge" % (infn1, infn2) )
		return

	#merge
	while (line1!='' and line2!=''):
		cmp = line_cmp_func(line1,line2)
		if (cmp < 0):
			fpo.write(line1)
			line1=handle1.readline()
		elif (cmp > 0):
			fpo.write(line2)
			line2=handle2.readline()
		else:
			line=line_merge_func(line1,line2,header_line1,header_line2,th_line1)
			fpo.write(line)
				
			line1=handle1.readline()
			line2=handle2.readline()

	while (line1!=''):
		fpo.write(line1)
		line1=handle1.readline()

	while (line2!=''):
		fpo.write(line2)
		line2=handle2.readline()

	#clear up.
	handle1.close()
	handle2.close()
	fpo.close()

def print_debug(msg):
	if (DEBUG):
		sys.stderr.write("%s\n" % (msg))
def usage():
	print "merge [OPTIONS] [-]"
	print "INPUT:"
	print " by default, reads file name from stdin"
	print "REQUIREMENT:"
	print " SORTED tuple files with \\"
	print " SAME format, either node or edge, requires \\"
	print " TABLE HEADER in each file. \\" 
	print " *META header line is not required but recommended"
	print "OPTIONS:"
	print "-h print this Help"
	print "-i input filename(s)"
	print "-y input filetype"
	print "-c input file compressed"
	print "-o output filename"
	print "-z compress output file"
	print "-m include monitor"
	print "-n group number by monitor"
	print "-t group ttl by monitor"

def main(argv):
	global DEBUG
	#option flags
	global INPUT_FILETYPE
	global INPUT_FILE_COMPRESSED

	global OUTPUT_FILENAME
	global GZIP_OUTPUT

	global GROUP_NUMBER_BY_MON
	global GROUP_TTL_BY_MON
	global INCLUDE_MONITOR

	ifn_list = []
	
	try:
		opts, args = getopt.getopt(argv[1:], "hgi:y:co:znt")
	except getopt.GetoptError as err:
		print str(err)
		usage()
		exit(2)

	for o,a in opts:
		if o == "-h":
			usage()
			exit(0)
		elif o == "-g":
			DEBUG = True
		elif o == "-i":
			ifn_list.append(a)
		elif o == "-y":
			INPUT_FILETYPE = a
		elif o == "-c":
			INPUT_FILE_COMPRESSED = a
		elif o == "-o":
			OUTPUT_FILENAME = a
		elif o == "-z":
			GZIP_OUTPUT = True
		elif o == "-m":
			INCLUDE_MONITOR = True
		elif o == "-n":
			GROUP_NUMBER_BY_MON = True
		elif o == "-t":
			GROUP_TTL_BY_MON = True
	
	if (len(ifn_list) == 0):
		while True:
			try:
				line = raw_input()
			except EOFError:
				break
			ifn_list.append(line.strip('\n'))

	if (OUTPUT_FILENAME == ""):
		OUTPUT_FILENAME = "default.output"
	
	#merge()
		
if __name__ == "__main__":
	main(sys.argv)
