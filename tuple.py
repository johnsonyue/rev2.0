import sys

import format

def enum(**enums):
	return type('Enum', (), enums)

##############################################################################################
# Input: meta header, trace lines.
# Output: file begin with a meta line,
#         followed by graph induced by traces \
#         using tuple format.
#
# Output Format:
#         .edge: ingress, outgress, delay, \
#                connected, [length[:length_n], number, max_delay:min_delay:avg_delay]
#         .node: ip, ntype
#
# Options: -z compress output file
#          -b output both simpified and verbose file
#          -n include edge number
#          -l include length of indirect edge \
#             (number of non-reply hops)
#          -d include full delay info
#          -a include all replied IPs in one hop (only use first reply by default)
#
# Note: final graph does not contain src ip
##############################################################################################

#format data structures
EdgeLine = enum(
	ingress=0,
	outgress=1,
	delay=2,
	connected=3,
	length=4,
	number=5
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
	host='H' #note 'host' is not necessarily a end host, might be a router
)

#option flags
GZIP_OUTPUT = False
OUTPUT_VERBOSE = False
INCLUDE_EDGE_NUMBER = False
INCLUDE_INDIRECT_LENGTH = False
FULL_DELAY = False

#model data structures
class EdgeAttr():
	def __init__(self):
		self.delay = [0,0,0,0] #max,mean,min+,min
		self.connected = ConnectionState.connected
		self.length = 0
		self.number = 0

class NodeAttr():
	def _init__(self):
		self.ntype = NodeType.router

#routines
def is_ip_str_ligit(ip):
	decs=ip.split('.')
	if (len(decs)!=4):
		return False #4 decimals.
	for d in decs:
		if(d==""):
			return False
		if(int(d)>255 or int(d)<0): #not in [0-255]
			return False
		if(int(d)!=0 and d[0]=='0'): #has extra 0 pad
			return False
	return True

def ip_str2int(ip):
	if not is_ip_str_ligit(ip):
		return -1
	r=3
	i=0
	for o in ip.split('.'):
		o=int(o)
		i+=o*pow(256,r)
		r-=1
	
	return i

def is_ip_int_ligit(i):
	if i>math.pow(2,32)-1 or i<0:
		return False
	else:
		return True

def ip_int2str(i):
	if not is_ip_int_ligit(i):
		return ""
	l=[]
	q=i
	for j in range(4):
		l.append(q%256)
		q=q/256
	ip=""
	for j in range(3,-1,-1):
		ip+=str(l[j])+"."
	return ip.strip(".")

def parse_trace(dstip, path, node_dict, edge_dict):
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
	hop_list = tmp_list if (hop.split(":")[0] != "q") else tmp_list[1:]
	
	#parse edges
	prev_ingress = ""
	for i in range(len(hop_list)-1):
		ingress = hop_list[i]
		outgress = hop_list[i+1]
		
		if (outgress.split(":")[0] == "q"):
			prev_ingress = ingress
			continue
		if (ingress.split(":")[0] == "q"):
			in_ip_int = ip_str2int(prev_ingress.split(format.itd)[format.Tuple.ip])
			ingress = prev_ingress
			prev_ingress = ""
		else:
			in_ip_int = ip_str2int(ingress.split(format.itd)[format.Tuple.ip])

		out_ip_int = ip_str2int(outgress.split(format.itd)[format.Tuple.ip])
		edge_key = (in_ip_int, out_ip_int)
		
		if (not edge_dict.has_key(edge_key)):
			in_ttl = ingress.split(format.td)
			
			attr = EdgeAttr()
			attr.delay = [0,0,0,0]
			attr.connected = ConnectionState.connected
			attr.length = 0
			attr.number = 0
			
def build_graph():
	#in memory data structure
	node_dict = {}
	edge_dict = {}
	
	while True:
		try:
			line = raw_input()
		except EOFError:
			exit()
		
		if (line.split(format.hd)[0] == format.hi):
			continue
		
		field_list = line.split(format.fd)
		dstip = field_list[format.Trace.dstip]
		path = field_list[format.Trace.path]

		parse_trace(dstip, path, node_dict, edge_dict)

def usage():
	print "tuple [OPTIONS] -"
	print "OPTIONS:"
	print "-h print this help"
	print "-z compress output"
	print "-b output both simplified and verbose file"
	print "-n include edge number"
	print "-l include length of indirect edge"
	print "-d include full delay info"
	print "-a include all replied IPs in one hop (only use first reply by default)"

def main(argv):
	build_graph()

if __name__ == "__main__":
	main(sys.argv)
