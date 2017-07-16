import sys
import os
import math
import subprocess
import getopt
import socket
import struct

import format

def enum(**enums):
	return type('Enum', (), enums)

##############################################################################################
# Input: meta header, trace lines.
# Output: file begin with a meta line,
#         followed by graph induced by traces \
#         using tuple format.
#
# Options: -z compress output file
#          -o <filename> overwrite default output file name
#          -d <dirname> specify output Directory
#	      by default tuple uses $pwd
#	   -u use Unsorted output
#          -b output both simpified and verbose file
#          -n include edge number
#          -l include length of indirect edge \
#             (number of non-reply hops)
#          -r ignore meta header
#          -i include full delay info
#          -a include all replied IPs in one hop (only use first reply by default)
#
# Note: final graph does not contain src ip
##############################################################################################

DEBUG = False
#option flags
GZIP_OUTPUT = False
OUTPUT_VERBOSE = False
UNSORTED_OUTPUT = False

INCLUDE_EDGE_NUMBER = False
INCLUDE_INDIRECT_LENGTH = False
FULL_DELAY = False

INCLUDE_ALL_REPLIES = False

OVERWRITE_FILENAME = ""
OVERWRITE_DIRNAME = ""
IGNORE_META = False

#model data structures
class EdgeAttr():
	MAX_DELAY = 1000000
	def __init__(self):
		self.delay = [0,0,self.MAX_DELAY,self.MAX_DELAY] #max,mean,min+,min
		self.connected = format.ConnectionState.connected
		self.length = []
		self.number = 0

class NodeAttr():
	def _init__(self):
		self.ntype = format.NodeType.router

#routines
#ip string, ip int transformation utils
def ip_str2int(ip):
	packedIP = socket.inet_aton(ip)
	return struct.unpack("!L", packedIP)[0]

def ip_int2str(i):
	return socket.inet_ntoa(struct.pack('!L',i)) 

################################################################################################################
# Main body of tuple:
# Logic in Pseudo:
#   1: combine blank hops
#   2: eliminate initial blanks
#   3: for each (in, out) in path:
#   4:     insert or update edge(in,out)
#   5: for each hop in path:
#   6:     insert or update node(hop)
################################################################################################################

def parse_trace(dstip, path, node_dict, edge_dict):
	global INCLUDE_ALL_REPLIES

	hop_list = path.split(format.hpd)

	#combine blank hops
	tmp_list = []
	blank_cnt = 0
	for hop in hop_list:
		if hop == format.blank_holder:
			blank_cnt += 1
			continue
		if blank_cnt != 0:
			tmp_list.append("q:%d" % (blank_cnt))
			blank_cnt = 0
		tmp_list.append(hop)
	
	#eliminate initial blanks
	hop_list = tmp_list if (tmp_list[0].split(":")[0] != "q") else tmp_list[1:]
	
	#parse edges
	for i in range(len(hop_list)-1):
		ingress_str = hop_list[i]
		outgress_str = hop_list[i+1]
		
		ingress_list = ingress_str.split(format.td)
		ingress = ingress_list[0] #note: if ingress has multiple replies, only consider the 1st one.
		if (outgress_str.split(":")[0] == "q"): #ignore (ingress, *), record ip
			prev_ingress = ingress
			continue
		if (ingress_str.split(":")[0] == "q"):  #transform (*, outgress) into (prev_ingress, outgress)
			in_ip_int = ip_str2int(prev_ingress.split(format.itd)[format.Tuple.ip])
			length = int(ingress_str.split(":")[1])
			ingress = prev_ingress
			is_connected = False
		else:                               #(ingress, outgress) mark as connected
			in_ip_int = ip_str2int(ingress.split(format.itd)[format.Tuple.ip])
			is_connected = True
		
		outgress_list = outgress_str.split(format.td)
		if not INCLUDE_ALL_REPLIES: #note: if outgress has multiple replies, consider all if flag is set.
			outgress_list = outgress_list[:1]
		
		for outgress in outgress_list: 
			out_ip_int = ip_str2int(outgress.split(format.itd)[format.Tuple.ip])
			edge_key = (in_ip_int, out_ip_int)
			
			in_rtt = ingress.split(format.itd)[format.Tuple.rtt]
			out_rtt = outgress.split(format.itd)[format.Tuple.rtt]
			delay = ( float(out_rtt) - float(in_rtt) ) / 2 #note it's "round" trip time.
			if (not edge_dict.has_key(edge_key)):
				attr = EdgeAttr()
				attr.delay = [delay, delay, attr.MAX_DELAY, delay] if delay <= 0 else [delay, delay, delay, delay]
				attr.connected = format.ConnectionState.connected if is_connected else format.ConnectionState.disconnected
				attr.length = [] if is_connected else [length]
				attr.number = 1
				
				edge_dict[edge_key] = attr
			else:
				attr = edge_dict[edge_key]
				#delay
				if delay > attr.delay[0]:
					attr.delay[0] = delay
				if delay < attr.delay[3]:
					attr.delay[3] = delay
				if delay > 0 and delay < attr.delay[2]:
					attr.delay[2] = delay
				attr.delay[1] = float(attr.delay[1]*attr.number + delay)/(attr.number+1)
				
				#connected
				if (attr.connected != is_connected):
					attr.connected = format.ConnectionState.both
				
				#length
				if (not is_connected) and (not length in attr.length):
					attr.length.append(length)
				
				#number
				attr.number += 1

	#parse nodes
	for hop in hop_list[:-1]:
		if (hop.split(":")[0] == "q"): #anonymous hop can be deduced from edges connection state.
			continue
		tup_list = hop.split(format.td)
		for tup in tup_list:
			ip_str = tup.split(format.itd)[format.Tuple.ip]
			ip_int = ip_str2int(ip_str)
			if (not node_dict.has_key(ip_int)):
				attr = NodeAttr()
				attr.ntype = format.NodeType.router
				node_dict[ip_int] = attr
			else:
				attr = node_dict[ip_int]
				if attr.ntype != format.NodeType.router:
					attr.ntype = format.NodeType.both


def output(node_dict, edge_dict, header_line):
	global DEBUG
	#option flags
	global GZIP_OUTPUT
	global OUTPUT_VERBOSE
	global UNSORTED_OUTPUT

	global INCLUDE_EDGE_NUMBER
	global INCLUDE_INDIRECT_LENGTH
	global FULL_DELAY

	global OVERWRITE_FILENAME
	global OVERWRITE_DIRNAME
	global IGNORE_META

	#construct dir name
	if OVERWRITE_DIRNAME != "":
		dir_name = OVERWRITE_DIRNAME + "/"
	elif header_line != "" and not IGNORE_META:
		meta_list = header_line.split(format.hd)
		date = meta_list[format.Meta.date]
	
		dir_name = "./" + str(date) + "/"
	else:
		dir_name = "./"
	
	if ( not os.path.exists(dir_name) ):
		os.makedirs(dir_name)
	
	#construct file name
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

	#start to write node
	if GZIP_OUTPUT:
		node_file_name = file_name + ".node.gz"
	else:
		node_file_name = file_name + ".node"

	fp = open( dir_name + "/" + node_file_name, 'wb' )
	if GZIP_OUTPUT:
		h = subprocess.Popen(['gzip', '-c', '-'], stdin=subprocess.PIPE, stdout=fp)
		handle = h.stdin
	else:
		handle = fp
	
	if not UNSORTED_OUTPUT:
		node_key_list = sorted( node_dict.iterkeys() )
	else:
		node_key_list = node_dict.iterkeys()

	handle.write( str(format.construct_node_th()) + "\n" )
	handle.write( str(header_line) + "\n" )
	for node_key in node_key_list:
		ip_str = ip_int2str(node_key)
		node = node_dict[node_key]
		ntype = node.ntype
		
		handle.write( str(ip_str) + format.tid + str(ntype) + "\n" )
	handle.close()
	
	#start to write edge
	if GZIP_OUTPUT:
		edge_file_name = file_name + ".edge.gz"
	else:
		edge_file_name = file_name + ".edge"

	fp = open( dir_name + "/" + edge_file_name, 'wb' )
	if GZIP_OUTPUT:
		h = subprocess.Popen(['gzip', '-c', '-'], stdin=subprocess.PIPE, stdout=fp)
		handle = h.stdin
	else:
		handle = fp
	
	if not UNSORTED_OUTPUT:
		edge_key_list = sorted( edge_dict.iterkeys(), key=lambda k:(k[0],k[1]) )
	else:
		edge_key_list = edge_dict.iterkeys()
	
	handle.write( str(format.construct_edge_th()) + "\n" )
	handle.write( str(header_line) + "\n" )
	for edge_key in edge_key_list:
		in_ip_str = ip_int2str(edge_key[0])
		out_ip_str = ip_int2str(edge_key[1])
		edge = edge_dict[edge_key]
		delay = edge.delay[2]
		if delay == edge.MAX_DELAY:
			delay = 0
		connected = edge.connected

		handle.write( str(in_ip_str) + format.tid + str(out_ip_str) + format.tid + str(delay) + format.tid + str(connected) )
		
		if INCLUDE_INDIRECT_LENGTH:
			if len(edge.length) == 0:
				handle.write( format.tid + "0" )
			else:
				length_str=""
				for l in edge.length:
					length_str += str(l) + format.ed
				length_str = length_str.rstrip(format.ed)
				handle.write( format.tid + str(length_str) )
		if INCLUDE_EDGE_NUMBER:
			handle.write( format.tid + str(edge.number) )
		if FULL_DELAY:
			min_positive = edge.delay[2] if edge.delay[2] != edge.MAX_DELAY else 0
			delay_str = str(edge.delay[0]) + format.ed + str(edge.delay[1]) + format.ed + str(min_positive) + format.ed + str(edge.delay[3])
			handle.write( format.tid + str(delay_str) )

		handle.write("\n")
	
	#output verbose file
	if OUTPUT_VERBOSE:
		INCLUDE_EDGE_NUMBER = True
		INCLUDE_INDIRECT_LENGTH = True
		FULL_DELAY = True

		OVERWRITE_FILENAME = file_name + ".backup"
		OUTPUT_VERBOSE = False
		output(node_dict, edge_dict, header_line)
		

def build_graph():
	#in memory data structure
	node_dict = {}
	edge_dict = {}
	
	header_line = ""
	while True:
		try:
			line = raw_input()
		except EOFError:
			break
		
		if (line.split(format.hd)[0] == format.hi):
			header_line = line
			continue
		
		field_list = line.split(format.fd)
		dstip = field_list[format.Trace.dstip]
		path = field_list[format.Trace.path]

		parse_trace(dstip, path, node_dict, edge_dict)

	output(node_dict, edge_dict, header_line)

def usage():
	print "tuple [OPTIONS] -"
	print "OPTIONS:"
	print "-h print this Help"
	print "-o <filename> Overwrite default output file name"
	print "   by default tuple uses meta data"
	print "   if no meta, use nos as filler"
	print "-d <dirname> specify output Directory"
	print "   by default tuple uses $pwd"
	print "-z gunZip compress output"
	print "-u use Unsorted output"
	print "-b output Both simplified and verbose file"
	print "-n include edge Number"
	print "-l include Length of indirect edge"
	print "-r ignoRe meta header"
	print "-i include full delay Info"
	print "-a include All replied addresses in one hop (only use first reply by default)"

def print_debug(msg):
	if (DEBUG):
		sys.stderr.write("%s\n" % (msg))

def main(argv):
	global DEBUG
	#option flags
	global GZIP_OUTPUT
	global OUTPUT_VERBOSE
	global UNSORTED_OUTPUT

	global INCLUDE_EDGE_NUMBER
	global INCLUDE_INDIRECT_LENGTH
	global FULL_DELAY

	global INCLUDE_ALL_REPLIES

	global OVERWRITE_FILENAME
	global OVERWRITE_DIRNAME
	global IGNORE_META

	try:
		opts, args = getopt.getopt(argv[1:], "hgo:d:zubnlria")
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
		elif o == "-o":
			OVERWRITE_FILENAME = a
		elif o == "-d":
			OVERWRITE_DIRNAME = a
		elif o == "-z":
			GZIP_OUTPUT = True
		elif o == "-u":
			UNSORTED_OUTPUT = True
		elif o == "-b":
			OUTPUT_VERBOSE = True
		elif o == "-n":
			INCLUDE_EDGE_NUMBER = True
		elif o == "-l":
			INCLUDE_INDIRECT_LENGTH = True
		elif o == "-r":
			IGNORE_META = True
		elif o == "-i":
			FULL_DELAY = True
		elif o == "-a":
			INCLUDE_ALL_REPLIES = True
		
	build_graph()

if __name__ == "__main__":
	main(sys.argv)
