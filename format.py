import sys
import socket
import struct

def enum(**enums):
	return type('Enum', (), enums)

##############
#meta format.#
##############
Meta = enum(
	source = 1,
	date = 2,
	time = 3,
	monitor = 4,
	extra = 5,
	srcip = 6
)

META_LEN = 6

#use ' ' as header delimiter.
hd = " "
#use '!!' as header indicator.
hi = "!!"

def print_meta(argv):
	if len(argv.keys()) != META_LEN:
		sys.stderr.write("Error: wrong meta args provided.\n")

	source = argv[Meta.source]
	date = argv[Meta.date]
	time = argv[Meta.time]
	monitor = argv[Meta.monitor]
	extra = argv[Meta.extra]
	srcip = argv[Meta.srcip]
	print hi + hd + str(source) + hd + str(date) + hd + str(time) + hd + str(monitor) + hd + str(extra) + hd + str(srcip)

###############
#trace format.#
###############
Tuple = enum(
	ip = 0,
	rtt = 1,
	ttl = 2 #note that in here ttl means "request_ttl"
)
#essentially a Hop is a Tuple array with each dimension stands for nth try.
#Note different implementation of traceroute has different hop dimensions, \ 
#e.g. by default CAIDA's scamper only emmits 2 Paris-UDP probes.

Trace = enum(
	dstip = 0,
	timestamp = 1,
	path = 2,
	extra = 3
)

#use "\t" as fields delimiter.
fd = "\t"
#use " " as hop delimiter.
hpd = " "
#use ";" as tuple delimiter.
td = ";"
#use "," as item delimiter.
itd = ","
#use "q" as blank holder.
blank_holder = "q"
#use ":" as extra delimiter.
ed = ":"

def update_srcip(header, srcip):
	fields = header.split(hd)
	argv = {}
	source = fields[Meta.source]
	date = fields[Meta.date]
	time = fields[Meta.time]
	monitor = fields[Meta.monitor]
	extra = fields[Meta.extra]
	
	return hi + hd + str(source) + hd + str(date) + hd + str(time) + hd + str(monitor) + hd + str(extra) + hd + str(srcip)

def construct_path(hop_array):
	path_str = ""
	for hop in hop_array:
		#first construct hop string
		hop_str = ""
		if hop != "q":
			for tup in hop:
				if (len(tup.keys()) == 0):
					continue
				ip = tup[Tuple.ip]
				rtt = tup[Tuple.rtt]
				ttl = tup[Tuple.ttl]
				tup_str = str(ip) + itd + str(rtt) + itd + str(ttl)
				hop_str += tup_str + td
		else:
			hop_str = "q"
	
		#append to the end of path string
		path_str += hop_str.rstrip(td) + hpd

	return path_str.rstrip(hpd)

def construct_trace(argv):
	dstip = argv[Trace.dstip]
	timestamp = argv[Trace.timestamp]
	path = argv[Trace.path]
	extra = argv[Trace.extra]
	
	return str(dstip) + fd + str(timestamp) + fd + str(path) + fd + str(extra)

##############
#tuple format#
##############

######################################################################################
# Output Format:
#         .edge: ingress, outgress, delay, \
#                connected, [length[:length_n], number, max_delay:min_delay:avg_delay]
#         .node: ip, ntype
#
######################################################################################
#format data structures
EdgeLine = enum(
	ingress=0,
	outgress=1,
	delay=2,
	connected=3,
	length=4,
	number=5,
	delay_info=6,
	ttl_info=7
)

NodeLine = enum(
	ip=0, #essentially, an IP is a 32 int.
	ntype=1, 
	monitor=2
)

ConnectionState = enum(
	connected='D',
	disconnected='I',
	both='B'
)

NodeType = enum(
	router='R',
	host='H', #note 'host' is not necessarily a end host, might be a router
	both='B'
)

FileName = enum(
	source = 0,
	time = 1,
	monitor = 2,
	ftype = 3
)

FileType = enum(
	edge = "edge",
	node = "node",
	other = "others"
)

#use "." as file name delimiter
fnd = "."
#use  ", " as tuple item delimiter
tid = ", "
#use ", " as th delimiter
thd = ", "
#use ", " as th indicator
thi = "#"

def construct_edge_th( UNSORTED_OUTPUT, INCLUDE_EDGE_NUMBER, INCLUDE_INDIRECT_LENGTH, FULL_DELAY, INCLUDE_TTL ):
	if (UNSORTED_OUTPUT):
		th_str = thi + thi
	else:
		th_str = thi
		
	th_str += thd + "ingress" + thd + "outgress" + thd + "delay" + thd + "connected"
	if (INCLUDE_EDGE_NUMBER):
		th_str += thd + "length"
	if (INCLUDE_INDIRECT_LENGTH):
		th_str += thd + "number"
	if (FULL_DELAY):
		th_str += thd + "delay_info"
	if (INCLUDE_TTL):
		th_str += thd + "ttl_info"
	
	return th_str

def construct_node_th( UNSORTED_OUTPUT ):
	if (UNSORTED_OUTPUT):
		th_str = thi
	else:
		th_str = thi
	
	th_str += thd + "ip" + thd + "ntype"
	
	return th_str

################
#util routines.#
################
#ip string, ip int transformation utils
def ip_str2int(ip):
	packedIP = socket.inet_aton(ip)
	return struct.unpack("!L", packedIP)[0]

def ip_int2str(i):
	return socket.inet_ntoa(struct.pack('!L',i)) 
