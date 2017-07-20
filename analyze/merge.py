import sys
import os
import getopt
import subprocess
import re

import format

##############################################################################################
# Requirement: 
#        SORTED tuple files with \
#        SAME format, either node or edge, requires \
#        TABLE HEADER in each file. \ 
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
GZIP_OUTPUT = False
COMPRESSED = False
DEL_ORG = False
OUTPUT_FILENAME = ""

GROUP_NUMBER_BY_MON = False
GROUP_TTL_BY_MON = False

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

def edge_line_merge(line1,line2,th_line1):
	global GROUP_NUMBER_BY_MON
	global GROUP_TTL_BY_MON

	line1=line1.strip("\n")
	line2=line2.strip("\n")
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
	
	line = str(ingress) + format.tid + str(outgress) + format.tid + str(delay) + format.tid + str(connected)
	
	#optional fields
	th_list = th_line1.strip("\n").split(format.thd)
	if "length" in th_list:
		length1 = line1.split(format.tid)[format.EdgeLine.length]
		length2 = line2.split(format.tid)[format.EdgeLine.length]
		length1_list = length1.split(format.ed)
		length2_list = length2.split(format.ed)
		length_list = length1_list + [ l for l in length2_list if not l in length1_list ]
		length = ""
		for l in length_list:
			length += l + format.ed
		
		line += format.tid + str(length.strip(format.ed))

	if "number" in th_list:
		number1 = line1.split(format.tid)[format.EdgeLine.number]
		number2 = line2.split(format.tid)[format.EdgeLine.number]
		if not GROUP_NUMBER_BY_MON:
			number = int(number1) + int(number2)
		
			line += format.tid + str(number)
		else:
			number_list1 = number1.split(format.ed)
			number_dict1 = {}
			for i in range(0,len(number_list1),2):
				mon = number_list1[i]
				num = int(number_list1[i+1])
				number_dict1[mon] = num

			number_list2 = number2.split(format.ed)
			number_dict2 = {}
			for i in range(0,len(number_list2),2):
				mon = number_list2[i]
				num = int(number_list2[i+1])
				number_dict2[mon] = num
			
			number_str = ""
			for key in number_dict1.iterkeys():
				if number_dict2.has_key(key):
					number_str += str(key) + format.ed + str(number_dict1[key]+number_dict2[key]) + format.ed
				else:
					number_str += str(key) + format.ed +str(number_dict1[key]) + format.ed
			for key in number_dict2.iterkeys():
				if not number_dict1.has_key(key):
					number_str += str(key) + format.ed + str(number_dict2[key]) + format.ed

			line += format.tid + number_str.strip(format.ed)
	
	if "delay_info" in th_list:
		delay_info1 = map(lambda x:float(x), line1.split(format.tid)[format.EdgeLine.delay_info].split(format.ed))
		delay_info2 = map(lambda x:float(x) ,line2.split(format.tid)[format.EdgeLine.delay_info].split(format.ed))
		max_delay = delay_info1[0] if delay_info1[0] > delay_info2[0] else delay_info2[0]
		min_delay = delay_info1[3] if delay_info1[3] < delay_info2[3] else delay_info2[3]
		if GROUP_NUMBER_BY_MON:
			number_list1 = number1.split(format.ed)
			number1 = 0
			for i in range(0,len(number_list1),2):
				num = int(number_list1[i+1])
				number1 += num

			number_list2 = number2.split(format.ed)
			number2 = 0
			for i in range(0,len(number_list2),2):
				num = int(number_list2[i+1])
				number2 += num
			
		mean_delay = (int(number1)*delay_info1[1] + int(number2)*delay_info2[1]) / (int(number1) + int(number2))
		delay_info = [ max_delay, mean_delay, delay, min_delay ]

		delay_info_str = ""
		for d in delay_info:
			delay_info_str += str(d) + format.ed
		line += format.tid + str(delay_info_str.strip(format.ed))

	if "ttl_info" in th_list:
		if not GROUP_TTL_BY_MON:
			ttl_info_list1 = map(lambda x:int(x), line1.split(format.tid)[format.EdgeLine.ttl_info].split(format.ed))
			ttl_info1 = {}
			for i in range(0,len(ttl_info_list1),2):
				key = ttl_info_list1[i]
				value = ttl_info_list1[i+1]
				ttl_info1[key] = value

			ttl_info_list2 = map(lambda x:int(x), line2.split(format.tid)[format.EdgeLine.ttl_info].split(format.ed))
			ttl_info2 = {}
			for i in range(0,len(ttl_info_list2),2):
				key = ttl_info_list2[i]
				value = ttl_info_list2[i+1]
				ttl_info2[key] = value

			ttl_info = {}
			for key in ttl_info1.iterkeys():
				if not ttl_info2.has_key(key):
					ttl_info[key] = ttl_info1[key]
				else:
					ttl_info[key] = ttl_info1[key] + ttl_info2[key]
			for key in ttl_info2.iterkeys():
				if not ttl_info1.has_key(key):
					ttl_info[key] = ttl_info2[key]
		
			ttl_info_str = ""
			for key in ttl_info.iterkeys():
				value = ttl_info[key]
				ttl_info_str += str(key) + format.ed + str(value) + format.ed

			line += format.tid + ttl_info_str.strip(format.ed)
		else:
			ttl_info_list1 = line1.split(format.tid)[format.EdgeLine.ttl_info].split(format.ed+format.ed)
			ttl_info_dict1 = {}
			for i in range(0,len(ttl_info_list1),2):
				mon = ttl_info_list1[i]
				mon_ttl_list = map( lambda x:int(x), ttl_info_list1[i+1].split(format.ed) )
				mon_ttl_dict = {}
				for i in range(0,len(mon_ttl_list),2):
					key = mon_ttl_list[i]
					value = mon_ttl_list[i+1]
					mon_ttl_dict[key] = value
				ttl_info_dict1[mon] = mon_ttl_dict

			ttl_info_list2 = line2.split(format.tid)[format.EdgeLine.ttl_info].split(format.ed+format.ed)
			ttl_info_dict2 = {}
			for i in range(0,len(ttl_info_list2),2):
				mon = ttl_info_list2[i]
				mon_ttl_list = map( lambda x:int(x), ttl_info_list2[i+1].split(format.ed) )
				mon_ttl_dict = {}
				for i in range(0,len(mon_ttl_list),2):
					key = mon_ttl_list[i]
					value = mon_ttl_list[i+1]
					mon_ttl_dict[key] = value
				ttl_info_dict2[mon] = mon_ttl_dict
			
			ttl_info_str = ""
			for key in ttl_info_dict1.iterkeys():
				if ttl_info_dict2.has_key(key):
					mon_ttl_dict1 = ttl_info_dict1[key]
					mon_ttl_dict2 = ttl_info_dict2[key]
					mon_ttl_dict = {}
					for k in mon_ttl_dict1.iterkeys():
						if mon_ttl_dict2.has_key(k):
							mon_ttl_dict[k] = mon_ttl_dict1[k] + mon_ttl_dict2[k]
						else:
							mon_ttl_dict[k] = mon_ttl_dict1[k]
					for k in mon_ttl_dict2.iterkeys():
						if not mon_ttl_dict1.has_key(k):
							mon_ttl_dict[k] = mon_ttl_dict2[k]
				else:
					mon_ttl_dict = ttl_info_dict1[key]
				
				mon_ttl_str = ""
				for k in mon_ttl_dict:
					mon_ttl_str += str(k) + format.ed + str(mon_ttl_dict[k]) + format.ed
				mon_ttl_str = mon_ttl_str.strip(format.ed)

				ttl_info_str += str(key) + format.ed + format.ed + str(mon_ttl_str) + format.ed + format.ed

			for key in ttl_info_dict2.iterkeys():
				if not ttl_info_dict1.has_key(key):
					mon_ttl_dict = ttl_info_dict2[key]
					mon_ttl_str = ""
					for k in mon_ttl_dict:
						mon_ttl_str += str(k) + format.ed + str(mon_ttl_dict[k]) + format.ed
					mon_ttl_str = mon_ttl_str.strip(format.ed)
					
					ttl_info_str += str(key) + format.ed + format.ed + str(mon_ttl_str) + format.ed + format.ed

			line += format.tid + ttl_info_str.strip(format.ed+format.ed)
	
	return str(line) + "\n"
		
def node_line_cmp(ln1, ln2):
	l1ip = format.ip_str2int(ln1.split(format.tid)[format.NodeLine.ip])
	l2ip = format.ip_str2int(ln2.split(format.tid)[format.NodeLine.ip])
	return l1ip - l2ip

def node_line_merge(line1,line2,th_line1):
	line1=line1.strip("\n")
	line2=line2.strip("\n")
	ip = line1.split(format.tid)[format.NodeLine.ip]
	ntype1 = line1.split(format.tid)[format.NodeLine.ntype]
	ntype2 = line2.split(format.tid)[format.NodeLine.ntype]
	
	if ntype1 != ntype2:
		ntype = format.NodeType.both
	else:
		ntype = ntype1
	
	return str(ip) + format.tid + str(ntype) + "\n"

def merge_two(infn1, infn2, ofn, compressed=False, gzip_output=False):
	global DEBUG
	#option flags
	global INPUT_FILETYPE

	global GROUP_NUMBER_BY_MON
	global GROUP_TTL_BY_MON
	
	if INPUT_FILETYPE == format.FileType.node:
		line_cmp_func = node_line_cmp
		line_merge_func = node_line_merge
	else:
		line_cmp_func = edge_line_cmp
		line_merge_func = edge_line_merge
	
	#open handles to write
	if gzip_output:
		fpo += ".gz"
	fpo = open(ofn, 'w')
	if gzip_output:
		ho = subprocess.Popen(['gzip', '-c', '-'], stdin=subprocess.PIPE, stdout=fpo)
		fpo = ho.stdin

	#open handles to read
	handle1 = open(infn1, 'r')
	if compressed or re.compile(".*\.gz$").match(infn1):
		h1 = subprocess.Popen(['gzip', '-c', '-d', '-'], stdin=handle1, stdout=subprocess.PIPE)
		handle1 = h1.stdout
	
	#one file hack
	if infn2 == "":
		fpo.write(handle1.read())
		return
	
	handle2 = open(infn2, 'r')
	if compressed or re.compile(".*\.gz$").match(infn2):
		h2 = subprocess.Popen(['gzip', '-c', '-d', '-'], stdin=handle2, stdout=subprocess.PIPE)
		handle2 = h2.stdout

	#headers
	th_line1 = ""
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
			line1 = handle1.readline()
	
	
	th_line2 = ""
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
			line2 = handle2.readline()

	if th_line1 != th_line2:
		print_debug( "File %s, %s has different format, refuse to merge" % (infn1, infn2) )
		return

	#th line
	fpo.write(th_line1)

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
			line=line_merge_func(line1,line2,th_line1)
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

def remove(fn_list):
	for fn in fn_list:
		os.remove(fn)

#############################################################################
# Function merge: Logic to merge files
# Input:
#    @inf_list: input file name list
#    also, function uses 4 global variables:
#       OUTPUT_FILENAME
#       COMPRESSED, indicates whether ORIGINAL input files are compressed
#       GZIP_OUTPUT, indicates whether the FINAL output file is compressed
#       DEL_ORG, indicates whether to delete the ORIGINAL files
#
# (Let c,g,d be short for local variables in current scope)
# (And Let C,G,D be short for global flags)
# Pseudo of merge: 
#    1: if len(inf_list) == 1: simply return
#    2: if len(inf_list) == 2: set c,g,d to C,G,D , merge these two
#    3: Set c,g,d = C,F,D and call merge_multi
# Pseudo of merge_multi:
#    1: if len(ifn_list) == 2: set c,g,d to F,G,T. #End of recursive calls
#    2: else:
#    3:     iterate through ifn_list, merge two by two
#    4:     update ifn_list, set c,g,d = F,F,T, recursively calls merge_multi
# 
# (see the following table for simplified logic)
# Truth Table: rows represents recursive call rounds
#     +---+---+---+
#     | c | g | d |
# +---+---+---+---+
# | 1.| C | F | D |
# +---+---+---+---+
# | 2.| F | F | T |
# +---+---+---+---+
# |...| F | F | T |
# +---+---+---+---+
# | n.| F | G | T |
# +---+---+---+---+
#
#############################################################################

def merge_multi(ifn_list, compressed, gzip_output, del_org):
	global OUTPUT_FILENAME

	if len(ifn_list) == 2:
		print_debug( "%s + %s ==> %s" % (ifn_list[0], ifn_list[1], OUTPUT_FILENAME) )
		merge_two(ifn_list[0], ifn_list[1], OUTPUT_FILENAME, compressed=False, gzip_output=GZIP_OUTPUT)
		remove(ifn_list)
	else:
		length = len(ifn_list)
		new_ifn_list = []
		for i in range(0,len(ifn_list)-1,2):
			tmp_ofn = str(length) + "." + str(i)
			print_debug( "%s + %s ==> %s" % (ifn_list[i], ifn_list[i+1], tmp_ofn) )
			merge_two(ifn_list[i], ifn_list[i+1], tmp_ofn, compressed, gzip_output)
			new_ifn_list.append(tmp_ofn)
		#odd
		i = len(ifn_list) - 1
		if (len(ifn_list) % 2) != 0:
			tmp_ofn = str(length) + "." + str(i)
			print_debug( "%s + %s ==> %s" % (ifn_list[i], "none", tmp_ofn) )
			merge_two(ifn_list[i], "", tmp_ofn, compressed, gzip_output)
			new_ifn_list.append(tmp_ofn)
			
		merge_multi(new_ifn_list, compressed=False, gzip_output=False, del_org=True)
		if del_org:
			remove(ifn_list)

def merge(ifn_list):
	global OUTPUT_FILENAME

	if len(ifn_list) <= 1:
		return #no need to merge
	if len(ifn_list) == 2: #special case: if only two input files given.
		merge_two(ifn_list[0], ifn_list[1], OUTPUT_FILENAME, COMPRESSED, GZIP_OUTPUT)
		if (DEL_ORG):
			remove(ifn_list)
	else:
		merge_multi(ifn_list, compressed=COMPRESSED, gzip_output=False, del_org=DEL_ORG)
	
def print_debug(msg):
	if (DEBUG):
		sys.stderr.write("%s\n" % (msg))
def usage():
	print "merge [OPTIONS] [-]"
	print "INPUT:"
	print "    by default, reads file name from stdin"
	print "REQUIREMENT:"
	print "    SORTED tuple files with "
	print "    SAME format, either node or edge, requires "
	print "    TABLE HEADER in each file. " 
	print "OPTIONS:"
	print "-h print this Help"
	print "-i input filename(s)"
	print "-y input filetype"
	print "-c input file compressed"
	print "-o output filename"
	print "-z compress output file"
	print "-n group number by monitor"
	print "-t group ttl by monitor"
	print "-d delete input files"

def main(argv):
	global DEBUG
	#option flags
	global INPUT_FILETYPE
	global OUTPUT_FILENAME

	global GROUP_NUMBER_BY_MON
	global GROUP_TTL_BY_MON

	ifn_list = []
	
	try:
		opts, args = getopt.getopt(argv[1:], "hgi:y:co:zntd")
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
			COMPRESSED = True
		elif o == "-o":
			OUTPUT_FILENAME = a
		elif o == "-z":
			GZIP_OUTPUT = True
		elif o == "-n":
			GROUP_NUMBER_BY_MON = True
		elif o == "-t":
			GROUP_TTL_BY_MON = True
		elif o == "-t":
			DEL_ORG = True
	
	if (len(ifn_list) == 0):
		while True:
			try:
				line = raw_input()
			except EOFError:
				break
			ifn_list.append(line.strip('\n'))

	if (OUTPUT_FILENAME == ""):
		OUTPUT_FILENAME = "default.output"
	
	merge(ifn_list)
		
if __name__ == "__main__":
	main(sys.argv)
