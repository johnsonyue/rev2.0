import format

###################################################################
# Routine to transform CAIDA sc_analysis_dump text
# Input: first line starts with header indicator, plus header \
#        followed by lines each representing a TR trace
#
# Output: first line starts with header indicator \ 
#         with srcip added to header, \
#         followed by TR lines with our devised format.
#
# Note: this Routine processes only a single file.
###################################################################
def transform_caida():
	header_line=""
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
		if (header_line != ""): #update srcip to header
			srcip = fields[1]
			print format.update_srcip(header_line, src_ip)
		
		#construct four fields of each trace
		dstip = fields[2]

		timestamp = fields[5]
		
		replied = fields[6]
		dst_rtt = fields[7]
		path = fields[13]
		hop_array = construct_hop_array(path, replied, dstip, dst_rtt)
		path = format.construct_path(hop_array)

		rpl_ttl = fields[9]
		halt_reason = fields[10]
		halt_data = fields[11]
		extra = str(halt_reason) + format.ed + str(halt_data)
				
		#construct trace
		argv = {}
		argv[format.dstip] = dstip
		argv[format.timestamp] = timestamp
		argv[format.path] = path
		argv[format.extra] = extra
		
		print format.construct_trace(argv)

###################################################################
# Routine to help transform_data() build hop array. 
# Input: path field and destination info.
# Output: array of tuple vector (There might be multiple tuples \
#         in one hop.)
###################################################################
def construct_hop_array(path, replied, dstip, dst_rtt):
	hop_list = path.split('\t')
	
	hop_array = []
	MAX_PROBE_NUM = 2
	for i in range(len(hop_list)):
		hop = hop_list[i]
		tmp = [ {} for i in range(MAX_PROBE_NUM) ]
		tup_list = hop.split(';')
		for tup in tup_list:
			item_list = tup.split(',')
			ip = item_list[0]
			rtt = item_list[1]
			ttl = i+1

			#different dimensions of tuple array
			ntries = int(item_list[2])
			tmp[ntries][format.ip] = ip
			tmp[ntries][format.rtt] = rtt
			tmp[ntries][format.ttl] = ttl
		hop_array.append(tmp)
	
	tmp = [ {} for i in range(MAX_PROBE_NUM) ]
	if (replied == 'R'):
		ip = dstip
		rtt = dst_rtt
		tmp[1][format.ip] = ip #presume that targets always replies at first try.
		tmp[1][format.rtt] = rtt
		tmp[1][format.ttl] = ttl
		hop_array.append(tmp)
	
	return hop_array
