import sys

def enum(**enums):
	return type('Enum', (), enums)

##############
#meta format.#
##############
Meta = enum(
	source = 0,
	date = 1,
	time = 2,
	monitor = 3,
	extra = 4,
	srcip = 5
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
	fields = header.split(hd)[1:]
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
	delay_info=6
)

NodeLine = enum(
	ip=0, #essentially, an IP is a 32 int.
	ntype=1
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
	monitor = 2
)

#use "." as file name delimiter
fnd = "."
#use  " " as tuple item delimiter
tid = " "
